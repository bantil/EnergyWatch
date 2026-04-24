from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    pass


class BaseScraper(ABC):
    DEFAULT_TIMEOUT_MS = 30_000
    DEFAULT_RETRIES = 3
    RETRY_DELAY_SECONDS = 5

    def __init__(self, timeout_ms: int = DEFAULT_TIMEOUT_MS, retries: int = DEFAULT_RETRIES):
        self.timeout_ms = timeout_ms
        self.retries = retries
        self.scraped_at = datetime.now(timezone.utc)

    def scrape(self) -> list[dict[str, Any]]:
        return asyncio.run(self._scrape_with_retry())

    async def _scrape_with_retry(self) -> list[dict[str, Any]]:
        last_error = None
        for attempt in range(1, self.retries + 1):
            try:
                logger.info(f"Scrape attempt {attempt}/{self.retries}")
                return await self._scrape_async()
            except ScraperError:
                raise
            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt} failed: {e}")
                if attempt < self.retries:
                    await asyncio.sleep(self.RETRY_DELAY_SECONDS)
        raise ScraperError(f"All {self.retries} attempts failed") from last_error

    @abstractmethod
    async def _scrape_async(self) -> list[dict[str, Any]]:
        ...
