from __future__ import annotations

import enum
from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Date,
    DateTime,
    Enum,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SupplierRate(Base):
    """One scraped snapshot of a third-party supplier's rate from EnergizeCT."""
    __tablename__ = "supplier_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    supplier_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    rate_cents_kwh: Mapped[float] = mapped_column(Float, nullable=False)
    contract_term_months: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    renewable_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    cancellation_fee: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), index=True
    )
    effective_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    source_url: Mapped[str] = mapped_column(String(512), nullable=False)
    raw_snippet: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_supplier_rates_name_scraped", "supplier_name", "scraped_at"),
        UniqueConstraint("supplier_name", "scraped_at", name="uq_supplier_scraped"),
    )

    def __repr__(self) -> str:
        return f"<SupplierRate {self.supplier_name!r} {self.rate_cents_kwh}¢>"


class StandardServiceRate(Base):
    """Eversource standard service (basic service) rate. Changes Jan 1 and Jul 1."""
    __tablename__ = "standard_service_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    utility: Mapped[str] = mapped_column(String(64), nullable=False, default="eversource")
    rate_cents_kwh: Mapped[float] = mapped_column(Float, nullable=False)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    source_url: Mapped[str] = mapped_column(String(512), nullable=False)

    __table_args__ = (
        UniqueConstraint("utility", "effective_from", name="uq_utility_effective_from"),
        Index("ix_standard_service_utility", "utility"),
    )

    def __repr__(self) -> str:
        return f"<StandardServiceRate {self.utility!r} {self.rate_cents_kwh}¢>"


class AlertType(str, enum.Enum):
    RATE_INCREASE = "rate_increase"
    RATE_DECREASE = "rate_decrease"
    NEW_SUPPLIER = "new_supplier"
    SUPPLIER_REMOVED = "supplier_removed"
    STANDARD_SERVICE_CHANGE = "standard_service_change"
    BETTER_DEAL_AVAILABLE = "better_deal_available"


class PriceAlert(Base):
    """Logged price change event."""
    __tablename__ = "price_alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alert_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    supplier_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    old_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    new_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    delta_cents: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), index=True
    )
    acknowledged: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    def __repr__(self) -> str:
        return f"<PriceAlert {self.alert_type} {self.supplier_name}>"


class ScrapeRun(Base):
    """Audit log of each scrape execution."""
    __tablename__ = "scrape_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    supplier_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
