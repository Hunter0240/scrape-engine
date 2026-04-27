from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from croniter import croniter

from engine.config import TargetConfig, load_all_targets
from engine.fetcher import Fetcher
from engine.main import run_target
from engine.storage import Storage

log = logging.getLogger(__name__)


async def run_scheduled(targets: list[TargetConfig] | None = None) -> None:
    if targets is None:
        targets = load_all_targets()

    scheduled = [t for t in targets if t.schedule]
    if not scheduled:
        log.warning("no targets have a schedule defined")
        return

    log.info(
        "scheduler starting with %d target(s): %s",
        len(scheduled),
        ", ".join(f"{t.name} ({t.schedule})" for t in scheduled),
    )

    iters = {t.name: croniter(t.schedule, datetime.now()) for t in scheduled}
    next_runs = {t.name: iters[t.name].get_next(datetime) for t in scheduled}

    fetcher = Fetcher()
    storage = Storage()

    try:
        while True:
            now = datetime.now()
            for target in scheduled:
                if now >= next_runs[target.name]:
                    log.info("scheduler firing: %s", target.name)
                    try:
                        summary = await run_target(target, fetcher, storage)
                        log.info("scheduler completed: %s", summary.message)
                    except Exception:
                        log.exception("scheduler error on %s", target.name)
                    next_runs[target.name] = iters[target.name].get_next(datetime)

            nearest = min(next_runs.values())
            sleep_secs = max(1, min((nearest - datetime.now()).total_seconds(), 30))
            await asyncio.sleep(sleep_secs)
    except asyncio.CancelledError:
        log.info("scheduler stopped")
    finally:
        await fetcher.close()
        storage.close()
