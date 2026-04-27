from __future__ import annotations

import asyncio
import logging
import random
import time

import httpx

from engine.config import TargetConfig

log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
]


class Fetcher:
    def __init__(self) -> None:
        self._last_request_at: dict[str, float] = {}
        self._client = httpx.AsyncClient(follow_redirects=True, timeout=30.0)

    async def _wait_for_rate_limit(self, target: TargetConfig) -> None:
        last = self._last_request_at.get(target.name, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < target.rate_limit:
            await asyncio.sleep(target.rate_limit - elapsed)

    def _build_headers(self, target: TargetConfig) -> dict[str, str]:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        headers.update(target.headers)
        return headers

    async def fetch(self, target: TargetConfig) -> str:
        await self._wait_for_rate_limit(target)

        last_exc: Exception | None = None
        for attempt in range(1, target.max_retries + 1):
            try:
                resp = await self._client.get(
                    target.url,
                    headers=self._build_headers(target),
                )
                self._last_request_at[target.name] = time.monotonic()

                if resp.status_code == 429:
                    wait = 2**attempt
                    log.warning(
                        "%s: rate limited (429), retrying in %ds", target.name, wait
                    )
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                log.info(
                    "%s: fetched %d bytes (attempt %d)",
                    target.name,
                    len(resp.text),
                    attempt,
                )
                return resp.text

            except httpx.HTTPStatusError:
                raise
            except httpx.HTTPError as exc:
                last_exc = exc
                wait = 2**attempt
                log.warning(
                    "%s: %s, retrying in %ds (attempt %d/%d)",
                    target.name,
                    exc,
                    wait,
                    attempt,
                    target.max_retries,
                )
                await asyncio.sleep(wait)

        raise RuntimeError(
            f"{target.name}: failed after {target.max_retries} retries"
        ) from last_exc

    async def close(self) -> None:
        await self._client.aclose()
