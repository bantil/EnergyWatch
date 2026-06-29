from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Person(Base):
    __tablename__ = "persons"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    handle: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(String(64), nullable=False, default="other")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )

    predictions: Mapped[list[Prediction]] = relationship(
        "Prediction", back_populates="person", lazy="select"
    )

    def __repr__(self) -> str:
        return f"<Person {self.name!r}>"


class Prediction(Base):
    __tablename__ = "predictions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    person_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("persons.id"), nullable=False, index=True
    )
    text: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False, default="other")
    source_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    source_description: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    made_at: Mapped[date] = mapped_column(Date, nullable=False)
    deadline: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    category: Mapped[str] = mapped_column(String(64), nullable=False, default="other")
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending", index=True
    )
    confidence_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    market_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )

    person: Mapped[Person] = relationship("Person", back_populates="predictions")
    outcome: Mapped[Optional[Outcome]] = relationship(
        "Outcome", back_populates="prediction", uselist=False, lazy="select"
    )

    __table_args__ = (
        Index("ix_predictions_person_status", "person_id", "status"),
    )

    def __repr__(self) -> str:
        return f"<Prediction {self.id} {self.status!r}>"


class Outcome(Base):
    __tablename__ = "outcomes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prediction_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("predictions.id"), nullable=False, unique=True
    )
    verdict: Mapped[str] = mapped_column(String(32), nullable=False)
    actual_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    evidence_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )

    prediction: Mapped[Prediction] = relationship("Prediction", back_populates="outcome")

    def __repr__(self) -> str:
        return f"<Outcome {self.verdict!r} for prediction {self.prediction_id}>"
