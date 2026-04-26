from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)


@dataclass
class RunSummary:
    target_name: str
    total_scraped: int
    new_records: int
    duplicates: int

    @property
    def message(self) -> str:
        lines = [
            f"Scrape complete: {self.target_name}",
            f"  Total scraped: {self.total_scraped}",
            f"  New records:   {self.new_records}",
            f"  Duplicates:    {self.duplicates}",
        ]
        return "\n".join(lines)


async def notify(webhook_url: str, summary: RunSummary) -> None:
    if "discord" in webhook_url:
        payload = {"content": summary.message}
    else:
        payload = {"text": summary.message}

    async with httpx.AsyncClient() as client:
        resp = await client.post(webhook_url, json=payload, timeout=10.0)
        if resp.is_success:
            log.info("notification sent for %s", summary.target_name)
        else:
            log.warning(
                "notification failed for %s: %d %s",
                summary.target_name,
                resp.status_code,
                resp.text,
            )
