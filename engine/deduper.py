from __future__ import annotations

import hashlib
import logging

from engine.config import TargetConfig

log = logging.getLogger(__name__)


def _compute_hash(record: dict, key_fields: list[str]) -> str:
    parts = []
    for field in key_fields:
        val = record.get(field)
        parts.append(str(val) if val is not None else "")
    payload = "|".join(parts).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def add_hashes(records: list[dict], target: TargetConfig) -> list[dict]:
    key_fields = target.key_fields or list(target.fields.keys())
    for record in records:
        record["_hash"] = _compute_hash(record, key_fields)
    return records
