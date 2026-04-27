from __future__ import annotations

import re
import unicodedata

from engine.config import TargetConfig

_WHITESPACE_RUN = re.compile(r"\s+")
_NON_DIGIT = re.compile(r"[^\d\-]")
_NON_FLOAT = re.compile(r"[^\d.\-]")


def _normalize(text: str) -> str:
    if not text.isascii():
        text = unicodedata.normalize("NFKC", text)
    text = _WHITESPACE_RUN.sub(" ", text)
    return text.strip()


def _coerce(value: str, field_type: str) -> str | int | float:
    if field_type == "int":
        stripped = _NON_DIGIT.sub("", value)
        return int(stripped) if stripped else 0
    if field_type == "float":
        stripped = _NON_FLOAT.sub("", value)
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
