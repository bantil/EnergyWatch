from __future__ import annotations

import os
import sys
from typing import Optional

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_energy_summary() -> Optional[str]:
    """Pull the latest EnergyWatch rate data to use as trading context."""
    try:
        if _REPO_ROOT not in sys.path:
            sys.path.insert(0, _REPO_ROOT)

        from energywatch.db.session import get_session
        from energywatch.analysis.recommendations import compute_recommendations

        session = get_session()
        try:
            result = compute_recommendations(session)
        finally:
            session.close()

        # Bail if only defaults came back (empty DB)
        if not result.top_suppliers and result.standard_service_rate == 12.64:
            return None

        lines = [
            "=== CT Energy Market Snapshot ===",
            f"As of: {result.generated_at.strftime('%Y-%m-%d')}",
            f"Eversource standard rate: {result.standard_service_rate:.4f} ¢/kWh",
        ]

        if result.top_suppliers:
            best = result.top_suppliers[0]
            direction = "saving" if best.annual_savings_dollars > 0 else "costing extra"
            lines.append(
                f"Cheapest third-party supplier: {best.supplier_name} "
                f"@ {best.rate_cents_kwh:.4f} ¢/kWh "
                f"({direction} ${abs(best.annual_savings_dollars):.2f}/yr vs. Eversource)"
            )

        lines.append(f"Market verdict: {result.verdict}")
        lines.append("=================================")

        return "\n".join(lines)

    except Exception:
        return None
