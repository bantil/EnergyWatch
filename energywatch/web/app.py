"""
FastAPI web dashboard for EnergyWatch.

Run with: energywatch serve
Or directly: uvicorn energywatch.web.app:app --host 0.0.0.0 --port 8000

Access from phone on same WiFi: http://<your-local-IP>:8000
"""
from __future__ import annotations

import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from energywatch.analysis.recommendations import compute_recommendations, get_latest_supplier_rates
from energywatch.analysis.stats import compute_market_stats
from energywatch.db.models import PriceAlert
from energywatch.db.session import get_session

app = FastAPI(title="EnergyWatch", docs_url=None, redoc_url=None)

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    session = get_session()
    try:
        rec = compute_recommendations(session)
        stats = compute_market_stats(session, days=30)

        cutoff = datetime.utcnow() - timedelta(days=7)
        recent_alerts = (
            session.query(PriceAlert)
            .filter(PriceAlert.created_at >= cutoff)
            .order_by(PriceAlert.created_at.desc())
            .limit(20)
            .all()
        )

        suppliers = get_latest_supplier_rates(session)
        std_rate = rec.standard_service_rate

        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "verdict": rec.verdict,
                "top_suppliers": rec.top_suppliers,
                "all_suppliers": suppliers,
                "std_rate": std_rate,
                "stats": stats,
                "alerts": recent_alerts,
                "generated_at": rec.generated_at.strftime("%Y-%m-%d %H:%M UTC"),
            },
        )
    finally:
        session.close()


@app.get("/api/rates")
async def api_rates():
    session = get_session()
    try:
        suppliers = get_latest_supplier_rates(session)
        return JSONResponse([
            {
                "supplier_name": s.supplier_name,
                "rate_cents_kwh": s.rate_cents_kwh,
                "contract_term_months": s.contract_term_months,
                "renewable_pct": s.renewable_pct,
                "scraped_at": s.scraped_at.isoformat() if s.scraped_at else None,
            }
            for s in suppliers
        ])
    finally:
        session.close()


@app.get("/api/recommend")
async def api_recommend():
    session = get_session()
    try:
        rec = compute_recommendations(session)
        return JSONResponse({
            "verdict": rec.verdict,
            "standard_service_rate": rec.standard_service_rate,
            "best_supplier": {
                "supplier_name": rec.best_supplier.supplier_name,
                "rate_cents_kwh": rec.best_supplier.rate_cents_kwh,
                "annual_savings_dollars": rec.best_supplier.annual_savings_dollars,
                "contract_term_months": rec.best_supplier.contract_term_months,
                "renewable_pct": rec.best_supplier.renewable_pct,
                "caveats": rec.best_supplier.caveats,
            } if rec.best_supplier else None,
            "top_suppliers": [
                {
                    "rank": s.rank,
                    "supplier_name": s.supplier_name,
                    "rate_cents_kwh": s.rate_cents_kwh,
                    "annual_savings_dollars": s.annual_savings_dollars,
                    "recommendation": s.recommendation,
                    "caveats": s.caveats,
                }
                for s in rec.top_suppliers
            ],
        })
    finally:
        session.close()


@app.get("/api/stats")
async def api_stats():
    session = get_session()
    try:
        return JSONResponse(compute_market_stats(session, days=30))
    finally:
        session.close()


@app.get("/api/alerts")
async def api_alerts():
    session = get_session()
    try:
        cutoff = datetime.utcnow() - timedelta(days=30)
        rows = (
            session.query(PriceAlert)
            .filter(PriceAlert.created_at >= cutoff)
            .order_by(PriceAlert.created_at.desc())
            .limit(50)
            .all()
        )
        return JSONResponse([
            {
                "alert_type": a.alert_type,
                "supplier_name": a.supplier_name,
                "old_rate": a.old_rate,
                "new_rate": a.new_rate,
                "delta_cents": a.delta_cents,
                "message": a.message,
                "created_at": a.created_at.isoformat(),
            }
            for a in rows
        ])
    finally:
        session.close()


@app.post("/api/scrape")
async def api_scrape():
    """Trigger a manual scrape from the web UI."""
    import asyncio
    from energywatch.scrapers.energizect import EnergizeCTScraper, StandardServiceScraper
    from energywatch.notifications.notifier import detect_and_log_alerts
    from energywatch.db.models import ScrapeRun, SupplierRate, StandardServiceRate

    session = get_session()
    now = datetime.now(timezone.utc)
    run = ScrapeRun(started_at=now, status="running")
    session.add(run)
    session.commit()

    try:
        supplier_data = EnergizeCTScraper().scrape()
        std_results = StandardServiceScraper().scrape()
        standard_data = std_results[0] if std_results else None

        alerts = detect_and_log_alerts(session, supplier_data, standard_data)

        for d in supplier_data:
            d["scraped_at"] = now
            existing = session.query(SupplierRate).filter_by(
                supplier_name=d["supplier_name"], scraped_at=now
            ).first()
            if not existing:
                session.add(SupplierRate(**d))

        if standard_data:
            existing = session.query(StandardServiceRate).filter_by(
                utility=standard_data["utility"],
                effective_from=standard_data["effective_from"],
            ).first()
            if not existing:
                session.add(StandardServiceRate(**standard_data))
            else:
                existing.rate_cents_kwh = standard_data["rate_cents_kwh"]
                existing.scraped_at = now

        run.finished_at = datetime.now(timezone.utc)
        run.status = "success"
        run.supplier_count = len(supplier_data)
        session.commit()

        return JSONResponse({
            "status": "success",
            "supplier_count": len(supplier_data),
            "alert_count": len(alerts),
            "alerts": [a.message for a in alerts],
        })
    except Exception as e:
        run.status = "failed"
        run.error_message = str(e)
        run.finished_at = datetime.now(timezone.utc)
        session.commit()
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
    finally:
        session.close()
