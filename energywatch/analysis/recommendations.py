from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from energywatch.db.models import StandardServiceRate, SupplierRate

# Average CT residential monthly usage (kWh) — CT PURA data
CT_AVG_MONTHLY_KWH = 725.0

# Small score bonus per % of renewable energy, to break ties in favor of green suppliers
RENEWABLE_BONUS_PER_PCT = 0.01  # ¢/kWh effective rate reduction per 1% renewable


@dataclass
class SupplierRecommendation:
    supplier_name: str
    rate_cents_kwh: float
    contract_term_months: Optional[int]
    renewable_pct: Optional[float]
    monthly_savings_cents: float
    annual_savings_dollars: float
    score: float
    rank: int
    recommendation: str
    caveats: list[str] = field(default_factory=list)


@dataclass
class RecommendationResult:
    standard_service_rate: float
    best_supplier: Optional[SupplierRecommendation]
    top_suppliers: list[SupplierRecommendation]
    generated_at: datetime
    verdict: str


def get_latest_standard_service_rate(session: Session) -> Optional[float]:
    row = (
        session.query(StandardServiceRate)
        .filter(StandardServiceRate.utility == "eversource")
        .order_by(StandardServiceRate.scraped_at.desc())
        .first()
    )
    return row.rate_cents_kwh if row else None


def get_latest_supplier_rates(session: Session) -> list[SupplierRate]:
    sub = (
        select(
            SupplierRate.supplier_name,
            func.max(SupplierRate.scraped_at).label("max_scraped_at"),
        )
        .group_by(SupplierRate.supplier_name)
        .subquery()
    )
    return (
        session.query(SupplierRate)
        .join(
            sub,
            (SupplierRate.supplier_name == sub.c.supplier_name)
            & (SupplierRate.scraped_at == sub.c.max_scraped_at),
        )
        .order_by(SupplierRate.rate_cents_kwh)
        .all()
    )


def compute_recommendations(session: Session) -> RecommendationResult:
    standard_rate = get_latest_standard_service_rate(session) or 12.64
    suppliers = get_latest_supplier_rates(session)
    now = datetime.now(timezone.utc)

    if not suppliers:
        return RecommendationResult(
            standard_service_rate=standard_rate,
            best_supplier=None,
            top_suppliers=[],
            generated_at=now,
            verdict="No supplier data available. Run 'energywatch scrape' first.",
        )

    recommendations = []
    for s in suppliers:
        renewable_bonus = (s.renewable_pct or 0) * RENEWABLE_BONUS_PER_PCT
        score = s.rate_cents_kwh - renewable_bonus

        monthly_savings_cents = (standard_rate - s.rate_cents_kwh) * CT_AVG_MONTHLY_KWH
        annual_savings_dollars = (monthly_savings_cents * 12) / 100

        caveats = []
        if s.contract_term_months and s.contract_term_months > 12:
            caveats.append(
                f"Long {s.contract_term_months}-month contract — rate may beat "
                "standard service now but could be higher after next semi-annual change"
            )
        if s.contract_term_months == 1:
            caveats.append("Month-to-month — rate can change anytime")
        if s.rate_cents_kwh > standard_rate:
            caveats.append("More expensive than standard service")
        if s.renewable_pct and s.renewable_pct >= 100:
            caveats.append("100% renewable energy")
        elif s.renewable_pct and s.renewable_pct > 35:
            caveats.append(f"{s.renewable_pct:.0f}% renewable (above CT minimum)")

        if annual_savings_dollars >= 1:
            recommendation = f"SAVE ${annual_savings_dollars:.2f}/year vs. standard service"
        elif annual_savings_dollars < -5:
            recommendation = (
                f"AVOID — costs ${abs(annual_savings_dollars):.2f}/year MORE than standard service"
            )
        else:
            recommendation = "Roughly equal to standard service"

        recommendations.append(
            SupplierRecommendation(
                supplier_name=s.supplier_name,
                rate_cents_kwh=s.rate_cents_kwh,
                contract_term_months=s.contract_term_months,
                renewable_pct=s.renewable_pct,
                monthly_savings_cents=monthly_savings_cents,
                annual_savings_dollars=annual_savings_dollars,
                score=score,
                rank=0,
                recommendation=recommendation,
                caveats=caveats,
            )
        )

    recommendations.sort(key=lambda r: r.score)
    for i, rec in enumerate(recommendations):
        rec.rank = i + 1

    best = recommendations[0] if recommendations else None
    top_3 = recommendations[:3]

    if best and best.annual_savings_dollars >= 5:
        verdict = (
            f"Switch to {best.supplier_name} at {best.rate_cents_kwh:.4f}¢/kWh "
            f"and save ${best.annual_savings_dollars:.2f}/year "
            f"(vs. Eversource at {standard_rate:.4f}¢/kWh)"
        )
    elif best and best.annual_savings_dollars > 0:
        verdict = (
            f"{best.supplier_name} offers marginal savings of "
            f"${best.annual_savings_dollars:.2f}/year"
        )
    else:
        verdict = (
            f"Stay on Eversource standard service ({standard_rate:.4f}¢/kWh) — "
            "no better deal currently available from third-party suppliers"
        )

    return RecommendationResult(
        standard_service_rate=standard_rate,
        best_supplier=best,
        top_suppliers=top_3,
        generated_at=now,
        verdict=verdict,
    )
