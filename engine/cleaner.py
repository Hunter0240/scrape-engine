from __future__ import annotations

import re
import unicodedata

from engine.config import TargetConfig

_WHITESPACE_RUN = re.compile(r"\s+")


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = _WHITESPACE_RUN.sub(" ", text)
    return text.strip()


def _coerce(value: str, field_type: str) -> str | int | float:
    if field_type == "int":
        stripped = re.sub(r"[^\d\-]", "", value)
        return int(stripped) if stripped else 0
    if field_type == "float":
        stripped = re.sub(r"[^\d.\-]", "", value)
        return float(stripped) if stripped else 0.0
    if field_type == "url":
        return value.strip()
    return value


def clean(records: list[dict], target: TargetConfig) -> list[dict]:
    cleaned = []
    for record in records:
        row = {}
        for name, value in record.items():
            if value is None:
                row[name] = None
                continue
            value = _normalize(value)
            field_def = target.fields[name]
            row[name] = _coerce(value, field_def.type)
        cleaned.append(row)
    return cleaned
