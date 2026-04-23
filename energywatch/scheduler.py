"""
APScheduler 3.x blocking scheduler for periodic scraping.

Runs daily at 7am ET by default (configurable).
Performs an immediate scrape on startup so data is available right away.
"""
from __future__ import annotations

import logging
import signal
import sys
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)


def _run_scrape_job() -> None:
    from energywatch.db.models import ScrapeRun, StandardServiceRate, SupplierRate
    from energywatch.db.session import get_session
    from energywatch.notifications.notifier import detect_and_log_alerts
    from energywatch.scrapers.energizect import EnergizeCTScraper, StandardServiceScraper

    logger.info(f"Scheduled scrape starting at {datetime.now().isoformat()}")
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

        for a in alerts:
            logger.info(f"ALERT: {a.message}")
        logger.info(f"Scrape complete: {len(supplier_data)} suppliers, {len(alerts)} alerts")

    except Exception as e:
        run.status = "failed"
        run.error_message = str(e)
        run.finished_at = datetime.now(timezone.utc)
        session.commit()
        logger.error(f"Scheduled scrape failed: {e}", exc_info=True)
    finally:
        session.close()


def start_scheduler(interval_hours: int = 24, at_hour: int = 7) -> None:
    scheduler = BlockingScheduler(timezone="America/New_York")

    if interval_hours == 24:
        trigger = CronTrigger(hour=at_hour, minute=0, timezone="America/New_York")
        logger.info(f"Scheduler: daily cron at {at_hour:02d}:00 ET")
    else:
        trigger = IntervalTrigger(hours=interval_hours)
        logger.info(f"Scheduler: interval every {interval_hours}h")

    scheduler.add_job(
        _run_scrape_job,
        trigger=trigger,
        id="scrape_job",
        name="EnergizeCT Rate Scrape",
        misfire_grace_time=3600,
        coalesce=True,
    )

    # Run immediately on start
    scheduler.add_job(
        _run_scrape_job,
        trigger="date",
        id="scrape_now",
        name="Initial Scrape",
    )

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received, stopping scheduler...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info("Scheduler started.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
