from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
from sqlalchemy.orm import Session

from energywatch.db.models import StandardServiceRate, SupplierRate


def get_supplier_history(
    session: Session,
    supplier_name: Optional[str] = None,
    days: int = 90,
) -> pd.DataFrame:
    """Return historical rates as a DataFrame."""
    # SQLite stores naive datetimes; compare without tz
    cutoff = datetime.utcnow() - timedelta(days=days)
    q = session.query(SupplierRate).filter(SupplierRate.scraped_at >= cutoff)
    if supplier_name:
        q = q.filter(SupplierRate.supplier_name == supplier_name)
    q = q.order_by(SupplierRate.scraped_at)

    rows = q.all()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([
        {
            "supplier_name": r.supplier_name,
            "rate_cents_kwh": r.rate_cents_kwh,
            "scraped_at": r.scraped_at,
            "contract_term_months": r.contract_term_months,
            "renewable_pct": r.renewable_pct,
        }
        for r in rows
    ])
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    return df


def compute_market_stats(session: Session, days: int = 30) -> dict[str, Any]:
    """Compute aggregate market statistics over the last N days."""
    df = get_supplier_history(session, days=days)

    std_row = (
        session.query(StandardServiceRate)
        .filter(StandardServiceRate.utility == "eversource")
        .order_by(StandardServiceRate.scraped_at.desc())
        .first()
    )
    std_rate = std_row.rate_cents_kwh if std_row else 12.64

    if df.empty:
        return {"error": "No historical data available. Run 'energywatch scrape' first."}

    # Latest rate per supplier
    latest = (
        df.sort_values("scraped_at")
        .groupby("supplier_name")
        .last()
        .reset_index()
    )

    # 7-day trend — SQLite returns tz-naive datetimes so compare without tz
    now = pd.Timestamp.utcnow().tz_localize(None)
    week_ago = now - pd.Timedelta(days=7)
    scraped = df["scraped_at"].dt.tz_localize(None) if df["scraped_at"].dt.tz is not None else df["scraped_at"]
    df_recent = df[scraped >= week_ago]
    df_older = df[scraped < week_ago]

    avg_now = df_recent["rate_cents_kwh"].mean() if not df_recent.empty else None
    avg_then = df_older["rate_cents_kwh"].mean() if not df_older.empty else None

    if avg_now is not None and avg_then is not None:
        delta = avg_now - avg_then
        trend = "rising" if delta > 0.10 else "falling" if delta < -0.10 else "stable"
    else:
        trend = "insufficient_data"

    cheapest_idx = latest["rate_cents_kwh"].idxmin()
    cheapest = latest.loc[cheapest_idx]
    pct_below = float((latest["rate_cents_kwh"] < std_rate).mean() * 100)

    # Per-supplier volatility (std dev of rate over period)
    volatility = df.groupby("supplier_name")["rate_cents_kwh"].std()
    most_volatile = volatility.idxmax() if not volatility.empty and volatility.notna().any() else None

    return {
        "market_avg_rate": round(float(latest["rate_cents_kwh"].mean()), 4),
        "market_min_rate": round(float(latest["rate_cents_kwh"].min()), 4),
        "market_max_rate": round(float(latest["rate_cents_kwh"].max()), 4),
        "rate_std_dev": round(float(latest["rate_cents_kwh"].std()), 4),
        "num_suppliers": len(latest),
        "cheapest_supplier": str(cheapest["supplier_name"]),
        "cheapest_rate": round(float(cheapest["rate_cents_kwh"]), 4),
        "pct_below_standard": round(pct_below, 1),
        "standard_service_rate": std_rate,
        "trend": trend,
        "avg_rate_7d_ago": round(float(avg_then), 4) if avg_then is not None else None,
        "avg_rate_today": round(float(avg_now), 4) if avg_now is not None else None,
        "most_volatile_supplier": most_volatile,
        "analysis_period_days": days,
    }


def get_rate_trend_series(
    session: Session, supplier_name: str, days: int = 90
) -> pd.DataFrame:
    """Return daily average rate for a specific supplier."""
    df = get_supplier_history(session, supplier_name=supplier_name, days=days)
    if df.empty:
        return df
    df["date"] = df["scraped_at"].dt.date
    return df.groupby("date")["rate_cents_kwh"].mean().reset_index()
