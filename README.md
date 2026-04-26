# Scrape Engine

A configurable web scraping engine in Python. Define scrape targets in YAML -- the engine handles fetching, parsing, cleaning, deduplication, storage, and notifications.

## Features

- **YAML target configs** -- define what to scrape without writing Python
- **Async fetching** -- httpx with rate limiting, retry with backoff, user-agent rotation
- **CSS selector parsing** -- container pattern with selectolax (C-backed, fast)
- **Data cleaning** -- whitespace normalization, unicode cleanup, type coercion
- **Deduplication** -- content hashing with DuckDB primary key constraint
- **Storage** -- DuckDB with CSV, JSON, and Parquet export
- **Notifications** -- Discord/Slack webhook POST with run summaries
- **Scheduling** -- cron expressions per target, background loop
- **CLI** -- run, schedule, list targets, export data

## Quick Start

```bash
pip install -r requirements.txt

# list available targets
python -m engine targets

# run all targets once
python -m engine run

# run a specific target
python -m engine run targets/hackernews.yaml

# export stored data
python -m engine export hackernews -f csv
python -m engine export hackernews -f json
python -m engine export hackernews -f parquet

# run on cron schedules (long-running)
python -m engine schedule
```

## Docker

```bash
docker compose up --build
```

The default command runs the scheduler, which fires each target on its configured cron schedule.

## Writing a Target

Create a YAML file in `targets/`:

```yaml
name: hackernews
url: https://news.ycombinator.com
container: "tr.athing"
rate_limit: 2.0
key_fields: [title, url]
schedule: "*/30 * * * *"

fields:
  rank:
    selector: "span.rank"
    type: int
  title:
    selector: "span.titleline > a"
  url:
    selector: "span.titleline > a"
    attribute: href
    type: url
  site:
    selector: "span.sitestr"
```

### Target fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Unique target identifier (used as DB table name) |
| `url` | yes | Page URL to scrape |
| `container` | yes | CSS selector for the repeating element wrapping each record |
| `fields` | yes | Map of field name to selector config |
| `rate_limit` | no | Minimum seconds between requests (default: 1.0) |
| `max_retries` | no | Retry count for failed requests (default: 3) |
| `key_fields` | no | Fields used for dedup hashing (default: all fields) |
| `schedule` | no | Cron expression for scheduled runs |
| `webhook_url` | no | Discord/Slack webhook for notifications |
| `headers` | no | Extra HTTP headers |

### Field config

| Field | Required | Description |
|-------|----------|-------------|
| `selector` | yes | CSS selector relative to the container |
| `attribute` | no | Extract an HTML attribute instead of text content |
| `type` | no | `str`, `int`, `float`, or `url` (default: `str`) |

## How It Works

The pipeline runs in five stages:

1. **Fetch** -- async HTTP request with rate limiting and retry
2. **Parse** -- find containers with CSS selectors, extract fields from each
3. **Clean** -- normalize whitespace/unicode, coerce types
4. **Deduplicate** -- hash key fields, skip records already in the database
5. **Store** -- insert into DuckDB, optionally notify via webhook

## Tech Stack

- Python 3.11+
- httpx (async HTTP client)
- selectolax (fast HTML parser)
- DuckDB (columnar analytical database)
- Pydantic (config validation)
- croniter (cron expression parsing)

## Project Structure

```
engine/
  cli.py         -- CLI entry point (run, schedule, targets, export)
  config.py      -- YAML target loader with Pydantic validation
  fetcher.py     -- async httpx client with rate limiting and retry
  parser.py      -- CSS selector extraction using selectolax
  cleaner.py     -- whitespace, unicode, type normalization
  deduper.py     -- content hash computation
  storage.py     -- DuckDB storage and CSV/JSON/Parquet export
  notifier.py    -- Discord/Slack webhook notifications
  scheduler.py   -- cron-based background scheduling loop
targets/
  hackernews.yaml
  github-trending.yaml
tests/
```

## Tests

```bash
pip install pytest
pytest tests/ -v
```

## License

MIT
