from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from engine.config import load_all_targets, load_target
from engine.main import run
from engine.scheduler import run_scheduled
from engine.storage import Storage


def cmd_run(args: argparse.Namespace) -> None:
    if args.targets:
        targets = [load_target(t) for t in args.targets]
    else:
        targets = None
    summaries = asyncio.run(run(targets))
    for s in summaries:
        print(s.message)
        print()


def cmd_schedule(args: argparse.Namespace) -> None:
    asyncio.run(run_scheduled())


def cmd_targets(args: argparse.Namespace) -> None:
    targets = load_all_targets()
    for t in targets:
        schedule = t.schedule or "none"
        fields = ", ".join(t.fields.keys())
        print(f"{t.name}")
        print(f"  url:      {t.url}")
        print(f"  fields:   {fields}")
        print(f"  schedule: {schedule}")
        print()


def cmd_export(args: argparse.Namespace) -> None:
    storage = Storage()
    fmt = args.format
    for name in args.targets:
        path = f"data/{name}.{fmt}"
        if fmt == "csv":
            storage.export_csv(name, path)
        elif fmt == "json":
            storage.export_json(name, path)
        elif fmt == "parquet":
            storage.export_parquet(name, path)
        print(f"exported {name} -> {path}")
    storage.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="scrape-engine", description="Configurable web scraping engine"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="verbose logging"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run targets once")
    p_run.add_argument("targets", nargs="*", help="target YAML files (default: all)")
    p_run.set_defaults(func=cmd_run)

    p_sched = sub.add_parser("schedule", help="run targets on their cron schedules")
    p_sched.set_defaults(func=cmd_schedule)

    p_list = sub.add_parser("targets", help="list configured targets")
    p_list.set_defaults(func=cmd_targets)

    p_export = sub.add_parser("export", help="export stored data")
    p_export.add_argument("targets", nargs="+", help="target names to export")
    p_export.add_argument(
        "-f",
        "--format",
        choices=["csv", "json", "parquet"],
        default="csv",
        help="output format (default: csv)",
    )
    p_export.set_defaults(func=cmd_export)

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")

    args.func(args)


if __name__ == "__main__":
    main()
