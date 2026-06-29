from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from receipts.analysis.scoring import compute_leaderboard, compute_person_stats, _days_delta
from receipts.db.models import Outcome, Person, Prediction
from receipts.db.session import get_session

app = FastAPI(title="Receipts", docs_url=None, redoc_url=None)

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

SOURCE_TYPES = ["tweet", "article", "speech", "podcast", "interview", "book", "other"]
CATEGORIES = ["economics", "politics", "sports", "tech", "science", "other"]
PERSON_CATEGORIES = ["economist", "politician", "sports", "tech", "media", "other"]


def _fmt_delta(days: Optional[int]) -> Optional[str]:
    if days is None:
        return None
    if days == 0:
        return "exactly on time"
    if days < 0:
        return f"{abs(days)}d early"
    return f"{days}d late"


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    session = get_session()
    try:
        leaderboard = compute_leaderboard(session)
        today = date.today()
        recent = (
            session.query(Prediction)
            .order_by(Prediction.created_at.desc())
            .limit(10)
            .all()
        )
        expiring = (
            session.query(Prediction)
            .filter(
                Prediction.status == "pending",
                Prediction.deadline.isnot(None),
                Prediction.deadline >= today,
            )
            .order_by(Prediction.deadline.asc())
            .limit(5)
            .all()
        )
        needs_review = (
            session.query(Prediction)
            .filter(Prediction.status == "expired")
            .order_by(Prediction.deadline.asc())
            .limit(20)
            .all()
        )
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "leaderboard": leaderboard,
                "recent": recent,
                "expiring": expiring,
                "needs_review": needs_review,
                "fmt_delta": _fmt_delta,
            },
        )
    finally:
        session.close()


@app.get("/person/{person_id}", response_class=HTMLResponse)
async def person_detail(request: Request, person_id: int):
    session = get_session()
    try:
        person = session.get(Person, person_id)
        if not person:
            return HTMLResponse("Person not found", status_code=404)
        stats = compute_person_stats(session, person)
        predictions = (
            session.query(Prediction)
            .filter(Prediction.person_id == person_id)
            .order_by(Prediction.made_at.desc())
            .all()
        )
        return templates.TemplateResponse(
            "person.html",
            {
                "request": request,
                "person": person,
                "stats": stats,
                "predictions": predictions,
                "fmt_delta": _fmt_delta,
                "_days_delta": _days_delta,
            },
        )
    finally:
        session.close()


@app.get("/prediction/{prediction_id}", response_class=HTMLResponse)
async def prediction_detail(request: Request, prediction_id: int):
    session = get_session()
    try:
        pred = session.get(Prediction, prediction_id)
        if not pred:
            return HTMLResponse("Prediction not found", status_code=404)
        delta = None
        if pred.outcome:
            delta = _days_delta(pred.outcome, pred)
        return templates.TemplateResponse(
            "prediction.html",
            {
                "request": request,
                "pred": pred,
                "delta": delta,
                "fmt_delta": _fmt_delta,
            },
        )
    finally:
        session.close()


@app.get("/add", response_class=HTMLResponse)
async def add_prediction_form(request: Request):
    session = get_session()
    try:
        persons = session.query(Person).order_by(Person.name).all()
        return templates.TemplateResponse(
            "add.html",
            {
                "request": request,
                "persons": persons,
                "source_types": SOURCE_TYPES,
                "categories": CATEGORIES,
                "error": None,
            },
        )
    finally:
        session.close()


@app.post("/add")
async def add_prediction_submit(
    request: Request,
    person_id: int = Form(...),
    text: str = Form(...),
    source_type: str = Form("other"),
    source_url: str = Form(""),
    source_description: str = Form(""),
    made_at: str = Form(...),
    deadline: str = Form(""),
    category: str = Form("other"),
    confidence_pct: str = Form(""),
    market_url: str = Form(""),
):
    session = get_session()
    try:
        pred = Prediction(
            person_id=person_id,
            text=text.strip(),
            source_type=source_type,
            source_url=source_url.strip() or None,
            source_description=source_description.strip() or None,
            made_at=date.fromisoformat(made_at),
            deadline=date.fromisoformat(deadline) if deadline.strip() else None,
            category=category,
            status="pending",
            confidence_pct=float(confidence_pct) if confidence_pct.strip() else None,
            market_url=market_url.strip() or None,
        )
        session.add(pred)
        session.commit()
        pred_id = pred.id
        return RedirectResponse(f"/prediction/{pred_id}", status_code=303)
    finally:
        session.close()


@app.get("/add-person", response_class=HTMLResponse)
async def add_person_form(request: Request):
    return templates.TemplateResponse(
        "add_person.html",
        {
            "request": request,
            "person_categories": PERSON_CATEGORIES,
            "error": None,
        },
    )


@app.post("/add-person")
async def add_person_submit(
    request: Request,
    name: str = Form(...),
    handle: str = Form(""),
    description: str = Form(""),
    category: str = Form("other"),
):
    session = get_session()
    try:
        person = Person(
            name=name.strip(),
            handle=handle.strip().lstrip("@") or None,
            description=description.strip() or None,
            category=category,
        )
        session.add(person)
        session.commit()
        person_id = person.id
        return RedirectResponse(f"/person/{person_id}", status_code=303)
    finally:
        session.close()


@app.get("/resolve/{prediction_id}", response_class=HTMLResponse)
async def resolve_form(request: Request, prediction_id: int):
    session = get_session()
    try:
        pred = session.get(Prediction, prediction_id)
        if not pred:
            return HTMLResponse("Prediction not found", status_code=404)
        return templates.TemplateResponse(
            "resolve.html",
            {"request": request, "pred": pred},
        )
    finally:
        session.close()


@app.post("/resolve/{prediction_id}")
async def resolve_submit(
    request: Request,
    prediction_id: int,
    verdict: str = Form(...),
    actual_date: str = Form(""),
    evidence_url: str = Form(""),
    notes: str = Form(""),
):
    session = get_session()
    try:
        pred = session.get(Prediction, prediction_id)
        if not pred:
            return HTMLResponse("Prediction not found", status_code=404)
        outcome = Outcome(
            prediction_id=prediction_id,
            verdict=verdict,
            actual_date=date.fromisoformat(actual_date) if actual_date.strip() else None,
            evidence_url=evidence_url.strip() or None,
            notes=notes.strip() or None,
        )
        session.add(outcome)
        pred.status = verdict
        session.commit()
        return RedirectResponse(f"/prediction/{prediction_id}", status_code=303)
    finally:
        session.close()


@app.get("/review", response_class=HTMLResponse)
async def review_queue(request: Request):
    session = get_session()
    try:
        needs_review = (
            session.query(Prediction)
            .filter(Prediction.status == "expired")
            .order_by(Prediction.deadline.asc())
            .all()
        )
        return templates.TemplateResponse(
            "review.html",
            {"request": request, "predictions": needs_review},
        )
    finally:
        session.close()


@app.get("/api/leaderboard")
async def api_leaderboard():
    session = get_session()
    try:
        lb = compute_leaderboard(session)
        return JSONResponse([
            {
                "rank": i + 1,
                "id": s.person.id,
                "name": s.person.name,
                "handle": s.person.handle,
                "predictions_made": s.predictions_made,
                "predictions_resolved": s.predictions_resolved,
                "correct": s.correct,
                "incorrect": s.incorrect,
                "partial": s.partial,
                "binary_accuracy": round(s.binary_accuracy, 4) if s.binary_accuracy is not None else None,
                "timely_accuracy": round(s.timely_accuracy, 4) if s.timely_accuracy is not None else None,
                "avg_days_delta": round(s.avg_days_delta, 1) if s.avg_days_delta is not None else None,
                "trust_score": round(s.trust_score, 4),
            }
            for i, s in enumerate(lb)
        ])
    finally:
        session.close()


@app.get("/api/predictions")
async def api_predictions(
    person_id: Optional[int] = None,
    status: Optional[str] = None,
    category: Optional[str] = None,
):
    session = get_session()
    try:
        q = session.query(Prediction)
        if person_id:
            q = q.filter(Prediction.person_id == person_id)
        if status:
            q = q.filter(Prediction.status == status)
        if category:
            q = q.filter(Prediction.category == category)
        preds = q.order_by(Prediction.created_at.desc()).limit(100).all()
        return JSONResponse([
            {
                "id": p.id,
                "person_id": p.person_id,
                "text": p.text,
                "source_type": p.source_type,
                "source_url": p.source_url,
                "made_at": p.made_at.isoformat(),
                "deadline": p.deadline.isoformat() if p.deadline else None,
                "category": p.category,
                "status": p.status,
                "confidence_pct": p.confidence_pct,
                "market_url": p.market_url,
            }
            for p in preds
        ])
    finally:
        session.close()
