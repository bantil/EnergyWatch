"""
Scraper for the EnergizeCT rate board (JavaScript-rendered Drupal 9 site).

Primary target: https://www.energizect.com/rate-board/compare-energy-supplier-rates
Standard service: https://www.energizect.com/rate-board-residential-standard-service-generation-rates
Fallback: https://gridshopper.com/ct/eversource
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Any, Optional

from playwright.async_api import Page, async_playwright

from energywatch.scrapers.base import BaseScraper, ScraperError

logger = logging.getLogger(__name__)

RATE_BOARD_URL = (
    "https://www.energizect.com/rate-board/compare-energy-supplier-rates"
)
STANDARD_SERVICE_URL = (
    "https://www.energizect.com/rate-board-residential-standard-service-generation-rates"
)
GRIDSHOPPER_URL = "https://gridshopper.com/ct/eversource"

_BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
]
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


# ── EnergizeCT supplier rate board ──────────────────────────────────────────

class EnergizeCTScraper(BaseScraper):
    """
    Scrapes third-party supplier rates from the EnergizeCT rate board.

    Uses a four-layer fallback strategy:
    1. Network interception of XHR/fetch JSON API responses
    2. Drupal Views table CSS selectors
    3. Card/teaser layout selectors
    4. Full-page text regex
    """

    async def _scrape_async(self) -> list[dict[str, Any]]:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=_BROWSER_ARGS)
            context = await browser.new_context(
                user_agent=_USER_AGENT,
                viewport={"width": 1280, "height": 900},
                ignore_https_errors=True,
            )

            intercepted_data: list[dict] = []
            api_event = asyncio.Event()

            async def handle_response(response):
                url = response.url
                ct = response.headers.get("content-type", "")
                if "json" not in ct or url == RATE_BOARD_URL:
                    return
                if any(p in url for p in ["jsonapi", "rate-board", "supplier", "views", "/api/"]):
                    try:
                        body = await response.json()
                        if _looks_like_rate_data(body):
                            intercepted_data.append(body)
                            api_event.set()
                            logger.info(f"Intercepted API response: {url}")
                    except Exception:
                        pass

            page = await context.new_page()
            page.on("response", handle_response)

            try:
                logger.info(f"Loading {RATE_BOARD_URL}")
                await page.goto(
                    RATE_BOARD_URL,
                    wait_until="networkidle",
                    timeout=self.timeout_ms,
                )

                try:
                    await asyncio.wait_for(asyncio.shield(api_event.wait()), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

                if intercepted_data:
                    results = _parse_intercepted_json(intercepted_data[0])
                    if results:
                        logger.info(f"JSON interception: {len(results)} suppliers")
                        return results

                results = await _parse_dom(page)
                if results:
                    return results

                html = await page.content()
                logger.error(
                    f"All strategies failed. Page length: {len(html)} chars. "
                    f"First 500 chars: {html[:500]}"
                )
                raise ScraperError(
                    "No supplier rates found. The page structure may have changed. "
                    "Check the logs for the page HTML snippet."
                )
            finally:
                await context.close()
                await browser.close()


def _looks_like_rate_data(body: Any) -> bool:
    rate_fields = {"rate", "field_rate", "rate_cents", "price", "field_price",
                   "supplier", "supplier_name", "rate_cents_kwh"}
    if isinstance(body, dict):
        if "data" in body and isinstance(body["data"], list) and body["data"]:
            first = body["data"][0]
            if isinstance(first, dict) and "attributes" in first:
                return bool(rate_fields & set(first["attributes"].keys()))
        if "suppliers" in body or "rates" in body:
            return True
    if isinstance(body, list) and body:
        if isinstance(body[0], dict):
            return bool(rate_fields & set(body[0].keys()))
    return False


def _parse_intercepted_json(body: Any) -> list[dict[str, Any]]:
    results = []
    now = datetime.now(timezone.utc)

    if isinstance(body, dict) and "data" in body:
        for item in body.get("data", []):
            attrs = item.get("attributes", {})
            name = attrs.get("title") or attrs.get("field_supplier_name") or attrs.get("name")
            rate_raw = attrs.get("field_rate") or attrs.get("rate") or attrs.get("field_price")
            if not name or rate_raw is None:
                continue
            results.append(_make_supplier_dict(
                name=str(name).strip(),
                rate_raw=str(rate_raw),
                term_raw=str(attrs.get("field_term") or attrs.get("term") or ""),
                pct_raw=str(attrs.get("field_renewable") or attrs.get("renewable") or ""),
                scraped_at=now,
            ))
    elif isinstance(body, list):
        for item in body:
            name = item.get("supplier_name") or item.get("supplier") or item.get("name")
            rate_raw = item.get("rate") or item.get("rate_cents_kwh") or item.get("price")
            if not name or rate_raw is None:
                continue
            results.append(_make_supplier_dict(
                name=str(name).strip(),
                rate_raw=str(rate_raw),
                term_raw=str(item.get("term") or item.get("contract_term") or ""),
                pct_raw=str(item.get("renewable") or item.get("renewable_pct") or ""),
                scraped_at=now,
            ))

    return [r for r in results if r.get("rate_cents_kwh") is not None]


async def _parse_dom(page: Page) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    results = []

    # Strategy 1: table-based layouts
    table_selectors = [
        "table.views-table",
        "table.rate-board-table",
        ".view-rate-board table",
        ".view-compare-energy-supplier-rates table",
        "table",
    ]
    for selector in table_selectors:
        try:
            table = await page.query_selector(selector)
            if not table:
                continue

            header_els = await table.query_selector_all("thead th, thead td")
            if not header_els:
                header_els = await table.query_selector_all("tr:first-child th")
            headers = [await h.inner_text() for h in header_els]
            headers = [h.strip().lower() for h in headers]

            col_map = _map_columns(headers)
            rows = await table.query_selector_all("tbody tr")
            if not rows:
                rows = await table.query_selector_all("tr:not(:first-child)")

            for row in rows:
                cells = await row.query_selector_all("td")
                texts = [await c.inner_text() for c in cells]
                texts = [t.strip() for t in texts]
                parsed = _extract_from_cells(texts, col_map)
                if parsed:
                    parsed["scraped_at"] = now
                    parsed["source_url"] = RATE_BOARD_URL
                    results.append(parsed)

            if results:
                logger.info(f"DOM table ({selector}): {len(results)} suppliers")
                return results
        except Exception as e:
            logger.debug(f"Table selector {selector!r} failed: {e}")

    # Strategy 2: card/list layouts
    card_selectors = [
        ".view-rate-board .views-row",
        ".view-compare-energy-supplier-rates .views-row",
        ".views-row",
        ".rate-card",
        ".supplier-card",
    ]
    for selector in card_selectors:
        try:
            cards = await page.query_selector_all(selector)
            if not cards:
                continue
            for card in cards:
                text = await card.inner_text()
                parsed = _parse_card_text(text)
                if parsed:
                    parsed["scraped_at"] = now
                    parsed["source_url"] = RATE_BOARD_URL
                    results.append(parsed)
            if results:
                logger.info(f"DOM cards ({selector}): {len(results)} suppliers")
                return results
        except Exception as e:
            logger.debug(f"Card selector {selector!r} failed: {e}")

    # Strategy 3: drupalSettings JSON embedded in page
    try:
        content = await page.content()
        for match in re.findall(r'drupalSettings\s*=\s*({.+?});\s*</script>', content, re.DOTALL):
            try:
                settings = json.loads(match)
                rate_data = _find_rates_in_drupal_settings(settings, now)
                if rate_data:
                    logger.info(f"drupalSettings: {len(rate_data)} suppliers")
                    return rate_data
            except json.JSONDecodeError:
                pass
    except Exception as e:
        logger.debug(f"drupalSettings strategy failed: {e}")

    return results


def _map_columns(headers: list[str]) -> dict[str, int]:
    col_map: dict[str, int] = {}
    patterns = [
        ("supplier_name", ["supplier", "company", "provider", "name", "title"]),
        ("rate", ["rate", "price", "¢", "cents", "kwh"]),
        ("term", ["term", "contract", "months", "length"]),
        ("renewable", ["renewable", "green", "clean", "energy source"]),
    ]
    for key, pats in patterns:
        for i, header in enumerate(headers):
            if any(p in header for p in pats):
                col_map[key] = i
                break
    return col_map


def _extract_from_cells(
    cells: list[str], col_map: dict[str, int]
) -> Optional[dict[str, Any]]:
    if not col_map:
        if len(cells) >= 2:
            col_map = {"supplier_name": 0, "rate": 1, "term": 2, "renewable": 3}

    def get(key: str) -> str:
        idx = col_map.get(key)
        return cells[idx].strip() if idx is not None and idx < len(cells) else ""

    name = get("supplier_name")
    rate_raw = get("rate")
    if not name or not rate_raw:
        return None
    rate = _parse_rate(rate_raw)
    if rate is None:
        return None
    return _make_supplier_dict(
        name=name,
        rate_raw=rate_raw,
        term_raw=get("term"),
        pct_raw=get("renewable"),
        scraped_at=None,  # filled by caller
    )


def _parse_card_text(text: str) -> Optional[dict[str, Any]]:
    rate_match = re.search(r'(\d+\.?\d*)\s*[¢¢c]\s*(?:/\s*kWh)?', text, re.IGNORECASE)
    if not rate_match:
        return None
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return None
    return _make_supplier_dict(
        name=lines[0],
        rate_raw=rate_match.group(0),
        term_raw=text,
        pct_raw=text,
        scraped_at=None,
    )


def _find_rates_in_drupal_settings(settings: dict, now: datetime) -> list[dict[str, Any]]:
    # Recursively search for lists of supplier-like objects
    def _search(obj: Any, depth: int = 0) -> list[dict[str, Any]]:
        if depth > 6:
            return []
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            results = _parse_intercepted_json(obj)
            if results:
                for r in results:
                    r["scraped_at"] = now
                return results
        if isinstance(obj, dict):
            for v in obj.values():
                found = _search(v, depth + 1)
                if found:
                    return found
        return []
    return _search(settings)


def _make_supplier_dict(
    name: str,
    rate_raw: str,
    term_raw: str,
    pct_raw: str,
    scraped_at: Optional[datetime],
) -> dict[str, Any]:
    return {
        "supplier_name": name,
        "rate_cents_kwh": _parse_rate(rate_raw),
        "contract_term_months": _parse_term(term_raw),
        "renewable_pct": _parse_pct(pct_raw),
        "cancellation_fee": None,
        "scraped_at": scraped_at,
        "source_url": RATE_BOARD_URL,
    }


# ── Standard service rate scraper ────────────────────────────────────────────

class StandardServiceScraper(BaseScraper):
    """
    Scrapes the Eversource standard service generation rate.
    Primary: EnergizeCT standard service page.
    Fallback: GridShopper.com.
    """

    async def _scrape_async(self) -> list[dict[str, Any]]:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=_BROWSER_ARGS)
            context = await browser.new_context(
                user_agent=_USER_AGENT,
                ignore_https_errors=True,
            )
            page = await context.new_page()
            try:
                result = await self._try_url(page, STANDARD_SERVICE_URL)
                if not result:
                    logger.warning("Primary standard service page failed, trying GridShopper")
                    result = await self._try_url(page, GRIDSHOPPER_URL)
                return result
            finally:
                await context.close()
                await browser.close()

    async def _try_url(self, page: Page, url: str) -> list[dict[str, Any]]:
        try:
            await page.goto(url, wait_until="networkidle", timeout=self.timeout_ms)
        except Exception as e:
            logger.error(f"Could not load {url}: {e}")
            return []
        text = await page.inner_text("body")
        return _parse_standard_service_text(text, url)


def _parse_standard_service_text(text: str, source_url: str) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)

    labeled_patterns = [
        r'(?:standard\s+service|basic\s+service|generation\s+rate)[^\d]{0,40}?(\d+\.?\d*)\s*[¢¢c]',
        r'(\d+\.?\d*)\s*[¢¢c]\s*/?\s*kwh[^.]{0,60}?(?:standard|eversource|basic)',
        r'eversource[^.]{0,60}?(\d+\.?\d*)\s*[¢¢c]',
    ]
    for pattern in labeled_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            rate = _parse_rate(match.group(1))
            if rate:
                eff_from, eff_to = _parse_effective_dates(text)
                return [_make_standard_dict(rate, eff_from, eff_to, now, source_url)]

    # Fallback: find any ¢ value in the 10-20 range
    for raw in re.findall(r'(\d{1,2}\.\d{1,4})\s*[¢¢c]', text):
        rate = _parse_rate(raw)
        if rate and 10.0 <= rate <= 20.0:
            eff_from, eff_to = _parse_effective_dates(text)
            return [_make_standard_dict(rate, eff_from, eff_to, now, source_url)]

    logger.error("Could not parse standard service rate from page text")
    return []


def _make_standard_dict(
    rate: float,
    eff_from: date,
    eff_to: date,
    now: datetime,
    source_url: str,
) -> dict[str, Any]:
    return {
        "utility": "eversource",
        "rate_cents_kwh": rate,
        "effective_from": eff_from,
        "effective_to": eff_to,
        "scraped_at": now,
        "source_url": source_url,
    }


# ── Parsing helpers ──────────────────────────────────────────────────────────

def _parse_rate(raw: str) -> Optional[float]:
    if not raw:
        return None
    cleaned = re.sub(r'[¢¢$/kKWwHh\s,]', '', raw.strip())
    try:
        value = float(cleaned)
    except ValueError:
        return None
    if value < 1.0:
        value *= 100.0
    if not (3.0 <= value <= 99.0):
        logger.warning(f"Suspicious rate value {value} parsed from {raw!r}")
    return round(value, 4)


def _parse_term(raw: str) -> Optional[int]:
    if not raw:
        return None
    raw_lower = raw.strip().lower()
    if any(k in raw_lower for k in ("month-to-month", "variable", "no contract", "mtm")):
        return 1
    match = re.search(r'(\d+)\s*-?\s*month', raw_lower)
    if match:
        return int(match.group(1))
    match = re.search(r'^(\d+)$', raw_lower.strip())
    if match:
        return int(match.group(1))
    return None


def _parse_pct(raw: str) -> Optional[float]:
    if not raw:
        return None
    match = re.search(r'(\d+\.?\d*)\s*%', raw)
    if match:
        return float(match.group(1))
    return None


def _parse_effective_dates(text: str) -> tuple[date, date]:
    from dateutil import parser as dateparser

    MONTHS = (
        r"(?:January|February|March|April|May|June|July|August|"
        r"September|October|November|December)"
    )
    pattern = re.search(
        rf'({MONTHS}\s+\d{{1,2}}(?:,?\s+\d{{4}})?)\s+(?:through|to|-|–)\s+'
        rf'({MONTHS}\s+\d{{1,2}}(?:,?\s+\d{{4}})?)',
        text,
        re.IGNORECASE,
    )
    if pattern:
        try:
            d1 = dateparser.parse(pattern.group(1), default=datetime(2025, 1, 1)).date()
            d2 = dateparser.parse(pattern.group(2), default=datetime(2025, 12, 31)).date()
            return d1, d2
        except Exception:
            pass

    # Infer from today: Eversource changes Jan 1 and Jul 1
    today = date.today()
    if today.month < 7:
        return date(today.year, 1, 1), date(today.year, 6, 30)
    else:
        return date(today.year, 7, 1), date(today.year, 12, 31)
