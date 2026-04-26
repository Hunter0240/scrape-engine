from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field


class FieldDef(BaseModel):
    selector: str
    attribute: str | None = None
    type: Literal["str", "int", "float", "url"] = "str"


class TargetConfig(BaseModel):
    name: str
    url: str
    container: str = Field(
        description="CSS selector for the repeating element that wraps each record"
    )
    fields: dict[str, FieldDef]
    rate_limit: float = Field(
        default=1.0, description="Minimum seconds between requests"
    )
    max_retries: int = 3
    headers: dict[str, str] = Field(default_factory=dict)
    key_fields: list[str] | None = Field(
        default=None,
        description="Fields used for dedup hashing. Defaults to all fields.",
    )
    webhook_url: str | None = None
    schedule: str | None = Field(
        default=None, description="Cron expression for scheduled runs"
    )


def load_target(path: str | Path) -> TargetConfig:
    path = Path(path)
    with open(path) as f:
        raw = yaml.safe_load(f)
    return TargetConfig(**raw)


def load_all_targets(directory: str | Path | None = None) -> list[TargetConfig]:
    if directory is None:
        directory = Path(__file__).resolve().parent.parent / "targets"
    directory = Path(directory)
    targets = []
    for p in sorted(directory.glob("*.yaml")):
        targets.append(load_target(p))
    return targets
