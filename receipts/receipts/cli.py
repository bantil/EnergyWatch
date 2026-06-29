from __future__ import annotations

import click
import uvicorn


@click.group()
def cli():
    """Receipts — prediction tracking CLI."""


@cli.command()
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8001, show_default=True)
@click.option("--reload", is_flag=True, default=False)
def serve(host: str, port: int, reload: bool):
    """Start the web dashboard."""
    uvicorn.run(
        "receipts.web.app:app",
        host=host,
        port=port,
        reload=reload,
    )


@cli.command()
def expire():
    """Mark past-deadline pending predictions as expired."""
    from datetime import date
    from receipts.db.models import Prediction
    from receipts.db.session import get_session

    session = get_session()
    try:
        today = date.today()
        expired = (
            session.query(Prediction)
            .filter(
                Prediction.status == "pending",
                Prediction.deadline.isnot(None),
                Prediction.deadline < today,
            )
            .all()
        )
        for pred in expired:
            pred.status = "expired"
        session.commit()
        click.echo(f"Marked {len(expired)} prediction(s) as expired.")
    finally:
        session.close()
