from __future__ import annotations

import asyncio
import json
import logging
import sys

from engine.cleaner import clean
from engine.config import TargetConfig, load_all_targets, load_target
from engine.deduper import add_hashes
from engine.fetcher import Fetcher
from engine.notifier import RunSummary, notify
from engine.parser import parse
from engine.storage import Storage

log = logging.getLogger(__name__)


async def run_target(
    target: TargetConfig, fetcher: Fetcher, storage: Storage
) -> RunSummary:
    log.info("running target: %s (%s)", target.name, target.url)

    html = await fetcher.fetch(target)
    raw = parse(html, target)
    records = clean(raw, target)
    records = add_hashes(records, target)

    before = storage.count(target.name)
    inserted = storage.insert(target, records)
    new_records = inserted if before == 0 else storage.count(target.name) - before
    summary = RunSummary(
        target_name=target.name,
        total_scraped=len(records),
        new_records=new_records,
        duplicates=len(records) - new_records,
    )

    if target.webhook_url:
        await notify(target.webhook_url, summary)

    log.info(
        "%s: %d scraped, %d new, %d duplicates",
        target.name,
        summary.total_scraped,
        summary.new_records,
        summary.duplicates,
    )
    return summary


async def run(
    targets: list[TargetConfig] | None = None, db_path: str | None = None
) -> list[RunSummary]:
    if targets is None:
        targets = load_all_targets()
    fetcher = Fetcher()
    storage = Storage(db_path)
    try:
        summaries = await asyncio.gather(
            *[run_target(target, fetcher, storage) for target in targets]
        )
    finally:
        await fetcher.close()
        storage.close()
    return list(summaries)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    args = sys.argv[1:]
    if args:
        targets = [load_target(a) for a in args]
    else:
        targets = None

    summaries = asyncio.run(run(targets))

    print()
    for s in summaries:
        print(s.message)
        print()


if __name__ == "__main__":
    main()
