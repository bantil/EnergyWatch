from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from sqlalchemy.orm import Session

from receipts.db.models import Outcome, Person, Prediction


@dataclass
class CategoryStats:
    category: str
    correct: int = 0
    incorrect: int = 0
    partial: int = 0

    @property
    def resolved(self) -> int:
        return self.correct + self.incorrect

    @property
    def binary_accuracy(self) -> Optional[float]:
        if self.resolved == 0:
            return None
        return self.correct / self.resolved


@dataclass
class PersonStats:
    person: Person
    predictions_made: int = 0
    correct: int = 0
    incorrect: int = 0
    partial: int = 0
    correct_on_time: int = 0
    days_deltas: list[int] = field(default_factory=list)
    by_category: dict[str, CategoryStats] = field(default_factory=dict)

    @property
    def predictions_resolved(self) -> int:
        return self.correct + self.incorrect + self.partial

    @property
    def binary_accuracy(self) -> Optional[float]:
        denom = self.correct + self.incorrect
        if denom == 0:
            return None
        return self.correct / denom

    @property
    def timely_accuracy(self) -> Optional[float]:
        denom = self.correct + self.incorrect
        if denom == 0:
            return None
        return self.correct_on_time / denom

    @property
    def avg_days_delta(self) -> Optional[float]:
        if not self.days_deltas:
            return None
        return sum(self.days_deltas) / len(self.days_deltas)

    @property
    def trust_score(self) -> float:
        ba = self.binary_accuracy
        if ba is None:
            return 0.0
        return ba * math.log2(self.predictions_resolved + 2)


def _days_delta(outcome: Outcome, prediction: Prediction) -> Optional[int]:
    if outcome.actual_date is None or prediction.deadline is None:
        return None
    return (outcome.actual_date - prediction.deadline).days


def compute_person_stats(session: Session, person: Person) -> PersonStats:
    predictions = (
        session.query(Prediction)
        .filter(Prediction.person_id == person.id)
        .all()
    )

    stats = PersonStats(person=person, predictions_made=len(predictions))

    for pred in predictions:
        cat = pred.category or "other"
        if cat not in stats.by_category:
            stats.by_category[cat] = CategoryStats(category=cat)
        cat_stats = stats.by_category[cat]

        outcome = pred.outcome
        if outcome is None:
            continue

        verdict = outcome.verdict
        if verdict == "correct":
            stats.correct += 1
            cat_stats.correct += 1
            delta = _days_delta(outcome, pred)
            if delta is not None:
                stats.days_deltas.append(delta)
                if delta <= 0:
                    stats.correct_on_time += 1
            elif pred.deadline is None:
                stats.correct_on_time += 1
        elif verdict == "incorrect":
            stats.incorrect += 1
            cat_stats.incorrect += 1
            delta = _days_delta(outcome, pred)
            if delta is not None:
                stats.days_deltas.append(delta)
        elif verdict == "partial":
            stats.partial += 1
            cat_stats.partial += 1

    return stats


def compute_leaderboard(session: Session) -> list[PersonStats]:
    persons = session.query(Person).order_by(Person.name).all()
    results = [compute_person_stats(session, p) for p in persons]
    results.sort(key=lambda s: (s.trust_score, s.binary_accuracy or 0), reverse=True)
    return results
