"""
EnergyWatch CLI — Click + Rich.

Commands:
  scrape    Run a manual scrape
  rates     Show current rates table
  recommend Show best supplier recommendation
  history   Show rate history
  stats     Show market trend statistics
  alerts    Show recent price change alerts
  monitor   Start scheduling daemon
  serve     Start web dashboard (browser/phone access)
"""
from __future__ import annotations

import logging
import socket
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import click
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from energywatch.db.session import get_session, init_db

console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.version_option(version="0.1.0", prog_name="energywatch")
def cli(verbose: bool) -> None:
    """EnergyWatch — Connecticut energy rate monitor."""
    _setup_logging(verbose)
    init_db()


# ── scrape ───────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--suppliers-only", is_flag=True, help="Only scrape supplier rates")
@click.option("--standard-only", is_flag=True, help="Only scrape standard service rate")
def scrape(suppliers_only: bool, standard_only: bool) -> None:
    """Run a manual scrape of EnergizeCT and store results."""
    from energywatch.db.models import ScrapeRun, StandardServiceRate, SupplierRate
    from energywatch.notifications.notifier import detect_and_log_alerts
    from energywatch.scrapers.energizect import EnergizeCTScraper, StandardServiceScraper

    session = get_session()
    now = datetime.now(timezone.utc)
    run = ScrapeRun(started_at=now, status="running")
    session.add(run)
    session.commit()

    supplier_data = []
    standard_data = None

    try:
        if not standard_only:
            console.print("[bold]Scraping supplier rates from EnergizeCT...[/bold]")
            supplier_data = EnergizeCTScraper().scrape()
            console.print(f"[green]Found {len(supplier_data)} suppliers[/green]")

        if not suppliers_only:
            console.print("[bold]Scraping Eversource standard service rate...[/bold]")
            std_results = StandardServiceScraper().scrape()
            if std_results:
                standard_data = std_results[0]
                console.print(
                    f"[green]Standard service: {standard_data['rate_cents_kwh']:.4f}¢/kWh[/green]"
                )
            else:
                console.print("[yellow]Could not retrieve standard service rate[/yellow]")

        # Detect alerts BEFORE inserting new data
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

        if alerts:
            console.print(f"\n[yellow bold]⚠  {len(alerts)} price alert(s):[/yellow bold]")
            for a in alerts:
                console.print(f"  [yellow]•[/yellow] {a.message}")
        else:
            console.print("[dim]No price changes detected.[/dim]")

        console.print("\n[green bold]Scrape complete.[/green bold]")

    except Exception as e:
        run.status = "failed"
        run.error_message = str(e)
        run.finished_at = datetime.now(timezone.utc)
        session.commit()
        console.print(f"[red bold]Scrape failed:[/red bold] {e}")
        console.print("[dim]If browser not found, run: playwright install chromium[/dim]")
        logging.getLogger("energywatch").exception("Scrape failed")
        sys.exit(1)
    finally:
        session.close()


# ── rates ────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--sort", type=click.Choice(["rate", "renewable", "term"]), default="rate")
@click.option("--max-rate", type=float, help="Only show rates below this (¢/kWh)")
@click.option("--min-renewable", type=float, help="Only show suppliers with >= this % renewable")
def rates(sort: str, max_rate: Optional[float], min_renewable: Optional[float]) -> None:
    """Show current supplier rates in a formatted table."""
    from energywatch.analysis.recommendations import (
        get_latest_standard_service_rate,
        get_latest_supplier_rates,
    )

    session = get_session()
    try:
        std_rate = get_latest_standard_service_rate(session)
        suppliers = get_latest_supplier_rates(session)

        if not suppliers:
            console.print("[yellow]No rate data yet. Run 'energywatch scrape' first.[/yellow]")
            return

        if max_rate is not None:
            suppliers = [s for s in suppliers if s.rate_cents_kwh <= max_rate]
        if min_renewable is not None:
            suppliers = [s for s in suppliers if (s.renewable_pct or 0) >= min_renewable]

        sort_keys = {
            "rate": lambda s: s.rate_cents_kwh,
            "renewable": lambda s: -(s.renewable_pct or 0),
            "term": lambda s: s.contract_term_months or 0,
        }
        suppliers.sort(key=sort_keys[sort])

        std_label = f"{std_rate:.4f}¢" if std_rate else "unknown"
        table = Table(
            title=f"CT Supplier Rates  (Eversource standard service: {std_label}/kWh)",
            box=box.ROUNDED,
            header_style="bold cyan",
        )
        table.add_column("#", style="dim", width=4)
        table.add_column("Supplier", min_width=24)
        table.add_column("Rate (¢/kWh)", justify="right", style="bold")
        table.add_column("vs Standard", justify="right")
        table.add_column("Term", justify="center")
        table.add_column("Renewable", justify="center")

        for i, s in enumerate(suppliers, 1):
            if std_rate is not None:
                delta = s.rate_cents_kwh - std_rate
                delta_str = (
                    f"[green]-{abs(delta):.4f}[/green]"
                    if delta < 0
                    else f"[red]+{delta:.4f}[/red]"
                )
            else:
                delta_str = "—"

            rate_color = "green bold" if (std_rate and s.rate_cents_kwh < std_rate) else "white"
            term_str = f"{s.contract_term_months}mo" if s.contract_term_months else "—"
            renew_str = f"{s.renewable_pct:.0f}%" if s.renewable_pct is not None else "—"

            table.add_row(
                str(i),
                s.supplier_name,
                f"[{rate_color}]{s.rate_cents_kwh:.4f}[/{rate_color}]",
                delta_str,
                term_str,
                renew_str,
            )

        console.print(table)
        if suppliers:
            console.print(
                f"[dim]Last scraped: "
                f"{suppliers[0].scraped_at.strftime('%Y-%m-%d %H:%M UTC')}[/dim]"
            )
    finally:
        session.close()


# ── recommend ────────────────────────────────────────────────────────────────

@cli.command()
def recommend() -> None:
    """Show best supplier recommendation vs. staying on standard service."""
    from energywatch.analysis.recommendations import compute_recommendations

    session = get_session()
    try:
        result = compute_recommendations(session)

        border = "green" if "Switch" in result.verdict else "yellow"
        console.print(Panel(
            Text(result.verdict, style="bold white"),
            title="[bold cyan]EnergyWatch Recommendation[/bold cyan]",
            border_style=border,
        ))

        if result.top_suppliers:
            table = Table(
                title=(
                    f"Top Suppliers  "
                    f"(Eversource standard: {result.standard_service_rate:.4f}¢/kWh)"
                ),
                box=box.SIMPLE_HEAVY,
                header_style="bold magenta",
            )
            table.add_column("Rank", width=5)
            table.add_column("Supplier", min_width=22)
            table.add_column("Rate (¢/kWh)", justify="right")
            table.add_column("Annual Savings", justify="right")
            table.add_column("Term", justify="center")
            table.add_column("Notes")

            for r in result.top_suppliers:
                sav = r.annual_savings_dollars
                sav_str = (
                    f"[green]+${sav:.2f}[/green]"
                    if sav >= 0
                    else f"[red]-${abs(sav):.2f}[/red]"
                )
                term_str = f"{r.contract_term_months}mo" if r.contract_term_months else "—"
                notes_raw = r.caveats[0] if r.caveats else ""
                notes = notes_raw[:50] + "…" if len(notes_raw) > 50 else notes_raw
                table.add_row(
                    f"#{r.rank}",
                    r.supplier_name,
                    f"{r.rate_cents_kwh:.4f}",
                    sav_str,
                    term_str,
                    notes,
                )
            console.print(table)
    finally:
        session.close()


# ── history ──────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--supplier", "-s", help="Filter to a specific supplier name")
@click.option("--days", "-d", default=30, type=int, help="Days of history to show")
def history(supplier: Optional[str], days: int) -> None:
    """Show rate history for all suppliers or a specific one."""
    from energywatch.analysis.stats import get_supplier_history

    session = get_session()
    try:
        df = get_supplier_history(session, supplier_name=supplier, days=days)
        if df.empty:
            console.print("[yellow]No historical data found.[/yellow]")
            return

        title = f"Rate History — Last {days} days"
        if supplier:
            title += f" — {supplier}"

        table = Table(title=title, box=box.MARKDOWN, header_style="bold")
        table.add_column("Date")
        if not supplier:
            table.add_column("Supplier")
        table.add_column("Rate (¢/kWh)", justify="right")
        table.add_column("Term", justify="center")
        table.add_column("Renewable", justify="center")

        for _, row in df.iterrows():
            cells = [row["scraped_at"].strftime("%Y-%m-%d")]
            if not supplier:
                cells.append(str(row["supplier_name"]))
            cells.append(f"{row['rate_cents_kwh']:.4f}")
            term = row.get("contract_term_months")
            cells.append(f"{int(term)}mo" if term and not isinstance(term, float) else "—")
            pct = row.get("renewable_pct")
            cells.append(f"{pct:.0f}%" if pct is not None else "—")
            table.add_row(*cells)

        console.print(table)
        console.print(f"[dim]{len(df)} records[/dim]")
    finally:
        session.close()


# ── stats ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--days", "-d", default=30, type=int, help="Analysis period in days")
def stats(days: int) -> None:
    """Show market trend statistics."""
    from energywatch.analysis.stats import compute_market_stats

    session = get_session()
    try:
        s = compute_market_stats(session, days=days)
        if "error" in s:
            console.print(f"[yellow]{s['error']}[/yellow]")
            return

        trend_color = {"rising": "red", "falling": "green", "stable": "white"}.get(
            s["trend"], "dim"
        )

        table = Table(
            title=f"Market Statistics — Last {s['analysis_period_days']} Days",
            box=box.ROUNDED,
            show_header=False,
        )
        table.add_column("Metric", style="bold cyan", min_width=30)
        table.add_column("Value", justify="right")

        table.add_row("Eversource standard service", f"{s['standard_service_rate']:.4f}¢/kWh")
        table.add_row("Market average rate", f"{s['market_avg_rate']:.4f}¢/kWh")
        table.add_row(
            "Cheapest supplier",
            f"{s['cheapest_supplier']} ({s['cheapest_rate']:.4f}¢)",
        )
        table.add_row("Highest rate on market", f"{s['market_max_rate']:.4f}¢/kWh")
        table.add_row("Market spread (std dev)", f"{s['rate_std_dev']:.4f}¢")
        table.add_row("Number of suppliers tracked", str(s["num_suppliers"]))
        table.add_row(
            "% suppliers cheaper than standard service",
            f"{s['pct_below_standard']:.1f}%",
        )
        table.add_row(
            "Market trend",
            f"[{trend_color}]{s['trend'].upper()}[/{trend_color}]",
        )
        if s.get("avg_rate_7d_ago") and s.get("avg_rate_today"):
            delta = s["avg_rate_today"] - s["avg_rate_7d_ago"]
            ds = (
                f"[green]{delta:+.4f}¢[/green]"
                if delta <= 0
                else f"[red]{delta:+.4f}¢[/red]"
            )
            table.add_row("7-day market rate change", ds)
        if s.get("most_volatile_supplier"):
            table.add_row("Most volatile supplier", str(s["most_volatile_supplier"]))

        console.print(table)
    finally:
        session.close()


# ── alerts ────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--days", "-d", default=7, type=int, help="Show alerts from last N days")
@click.option("--type", "alert_type", help="Filter by alert type")
@click.option("--unread", is_flag=True, help="Show only unacknowledged alerts")
def alerts(days: int, alert_type: Optional[str], unread: bool) -> None:
    """Show recent price change alerts."""
    from energywatch.db.models import PriceAlert

    session = get_session()
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        q = (
            session.query(PriceAlert)
            .filter(PriceAlert.created_at >= cutoff)
            .order_by(PriceAlert.created_at.desc())
        )
        if alert_type:
            q = q.filter(PriceAlert.alert_type == alert_type)
        if unread:
            q = q.filter(PriceAlert.acknowledged == 0)

        rows = q.all()
        if not rows:
            console.print("[dim]No alerts in the selected period.[/dim]")
            return

        type_colors = {
            "rate_increase": "red",
            "rate_decrease": "green",
            "new_supplier": "blue",
            "supplier_removed": "yellow",
            "standard_service_change": "magenta",
        }

        table = Table(
            title=f"Price Alerts — Last {days} Days",
            box=box.SIMPLE,
            header_style="bold yellow",
        )
        table.add_column("Date", style="dim", width=12)
        table.add_column("Type", width=22)
        table.add_column("Supplier")
        table.add_column("Change", justify="right")
        table.add_column("Message")

        for a in rows:
            color = type_colors.get(a.alert_type, "white")
            if a.delta_cents is not None:
                sign = "+" if a.delta_cents > 0 else ""
                change = f"[{color}]{sign}{a.delta_cents:.4f}¢[/{color}]"
            else:
                change = "—"
            msg = a.message
            if len(msg) > 60:
                msg = msg[:57] + "…"
            table.add_row(
                a.created_at.strftime("%Y-%m-%d"),
                f"[{color}]{a.alert_type}[/{color}]",
                a.supplier_name or "—",
                change,
                msg,
            )
        console.print(table)
    finally:
        session.close()


# ── monitor ───────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--interval", "-i", default=24, type=int, help="Scrape interval in hours")
@click.option("--at-hour", default=7, type=int, help="Run daily at this hour (0-23 ET)")
def monitor(interval: int, at_hour: int) -> None:
    """Start the monitoring daemon. Scrapes on a schedule."""
    from energywatch.scheduler import start_scheduler

    console.print(
        f"[bold]EnergyWatch Monitor[/bold] — "
        f"scraping every {interval}h (daily at {at_hour:02d}:00 ET)"
    )
    console.print("[dim]Press Ctrl+C to stop.[/dim]")
    start_scheduler(interval_hours=interval, at_hour=at_hour)


# ── serve ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--host", default="0.0.0.0", help="Bind address (0.0.0.0 = all interfaces)")
@click.option("--port", default=8000, type=int, help="Port number")
@click.option("--no-browser", is_flag=True, help="Don't open browser automatically")
def serve(host: str, port: int, no_browser: bool) -> None:
    """Start the web dashboard. Access from phone via http://<local-IP>:<port>"""
    import uvicorn

    local_ip = _get_local_ip()
    console.print(f"[bold]EnergyWatch Web Dashboard[/bold]")
    console.print(f"  Local:   [link]http://localhost:{port}[/link]")
    console.print(f"  Network: [link]http://{local_ip}:{port}[/link]  ← open this on your phone")
    console.print("[dim]Press Ctrl+C to stop.[/dim]\n")

    if not no_browser:
        import threading, webbrowser
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()

    uvicorn.run(
        "energywatch.web.app:app",
        host=host,
        port=port,
        log_level="warning",
    )


def _get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"
