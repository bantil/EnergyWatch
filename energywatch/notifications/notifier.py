"""
Alert detection: compare new scrape results against the previous DB snapshot
and log PriceAlert rows for any significant changes.

IMPORTANT: Call detect_and_log_alerts() BEFORE inserting new rates into the DB
so the previous snapshot is still readable.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from energywatch.db.models import AlertType, PriceAlert, ScrapeRun, StandardServiceRate, SupplierRate

logger = logging.getLogger(__name__)

# Minimum rate delta to trigger a supplier alert (avoids noise from rounding)
RATE_CHANGE_THRESHOLD = 0.05  # ¢/kWh


def detect_and_log_alerts(
    session: Session,
    new_supplier_rates: list[dict[str, Any]],
    new_standard_rate: Optional[dict[str, Any]],
) -> list[PriceAlert]:
    """
    Compare new scrape against previous DB state. Insert PriceAlert rows.
    Returns the list of alerts created.
    """
    # Suppress NEW_SUPPLIER alerts on the very first successful scrape
    is_first_scrape = (
        session.query(ScrapeRun).filter(ScrapeRun.status == "success").count() == 0
    )

    alerts: list[PriceAlert] = []
    alerts.extend(_check_supplier_changes(session, new_supplier_rates, suppress_new=is_first_scrape))
    if new_standard_rate:
        alerts.extend(_check_standard_service_change(session, new_standard_rate))

    for alert in alerts:
        session.add(alert)

    return alerts


def _get_previous_rates(session: Session) -> dict[str, float]:
    """Return {supplier_name: rate_cents_kwh} for the most recent snapshot."""
    sub = (
        session.query(
            SupplierRate.supplier_name,
            func.max(SupplierRate.scraped_at).label("max_scraped"),
        )
        .group_by(SupplierRate.supplier_name)
        .subquery()
    )
    rows = (
        session.query(SupplierRate)
        .join(
            sub,
            (SupplierRate.supplier_name == sub.c.supplier_name)
            & (SupplierRate.scraped_at == sub.c.max_scraped),
        )
        .all()
    )
    return {r.supplier_name: r.rate_cents_kwh for r in rows}


def _check_supplier_changes(
    session: Session,
    new_rates: list[dict[str, Any]],
    suppress_new: bool = False,
) -> list[PriceAlert]:
    alerts = []
    now = datetime.now(timezone.utc)
    prev = _get_previous_rates(session)
    new_names = {r["supplier_name"] for r in new_rates}

    for rate_dict in new_rates:
        name = rate_dict["supplier_name"]
        new_rate = rate_dict["rate_cents_kwh"]

        if new_rate is None:
            continue

        if name not in prev:
            if not suppress_new:
                alerts.append(PriceAlert(
                    alert_type=AlertType.NEW_SUPPLIER.value,
                    supplier_name=name,
                    old_rate=None,
                    new_rate=new_rate,
                    delta_cents=None,
                    message=f"New supplier on EnergizeCT: {name} at {new_rate:.4f}¢/kWh",
                    created_at=now,
                ))
                logger.info(f"Alert: new supplier {name!r} at {new_rate}¢")
        else:
            old_rate = prev[name]
            delta = new_rate - old_rate
            if abs(delta) >= RATE_CHANGE_THRESHOLD:
                direction = AlertType.RATE_INCREASE if delta > 0 else AlertType.RATE_DECREASE
                alerts.append(PriceAlert(
                    alert_type=direction.value,
                    supplier_name=name,
                    old_rate=old_rate,
                    new_rate=new_rate,
                    delta_cents=round(delta, 4),
                    message=(
                        f"{name}: {'increased' if delta > 0 else 'decreased'} "
                        f"{abs(delta):.4f}¢ "
                        f"({old_rate:.4f}¢ → {new_rate:.4f}¢/kWh)"
                    ),
                    created_at=now,
                ))
                logger.info(f"Alert: {name} {old_rate}¢ → {new_rate}¢")

    # Detect removed suppliers
    for name, old_rate in prev.items():
        if name not in new_names:
            alerts.append(PriceAlert(
                alert_type=AlertType.SUPPLIER_REMOVED.value,
                supplier_name=name,
                old_rate=old_rate,
                new_rate=None,
                delta_cents=None,
                message=f"Supplier removed from EnergizeCT: {name}",
                created_at=now,
            ))
            logger.info(f"Alert: supplier removed {name!r}")

    return alerts


def _check_standard_service_change(
    session: Session,
    new_rate_dict: dict[str, Any],
) -> list[PriceAlert]:
    alerts = []
    now = datetime.now(timezone.utc)

    prev_row = (
        session.query(StandardServiceRate)
        .filter(StandardServiceRate.utility == "eversource")
        .order_by(StandardServiceRate.scraped_at.desc())
        .first()
    )
    if prev_row is None:
        return alerts

    new_rate = new_rate_dict["rate_cents_kwh"]
    old_rate = prev_row.rate_cents_kwh
    delta = new_rate - old_rate

    if abs(delta) >= 0.01:
        alerts.append(PriceAlert(
            alert_type=AlertType.STANDARD_SERVICE_CHANGE.value,
            supplier_name="eversource",
            old_rate=old_rate,
            new_rate=new_rate,
            delta_cents=round(delta, 4),
            message=(
                f"Eversource standard service rate changed: "
                f"{old_rate:.4f}¢ → {new_rate:.4f}¢/kWh "
                f"({'up' if delta > 0 else 'down'} {abs(delta):.4f}¢)"
            ),
            created_at=now,
        ))
        logger.info(f"Alert: Eversource standard service {old_rate}¢ → {new_rate}¢")

    return alerts
