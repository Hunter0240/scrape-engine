from __future__ import annotations

import logging

from selectolax.parser import HTMLParser

from engine.config import FieldDef, TargetConfig

log = logging.getLogger(__name__)


def _extract_field(container, field: FieldDef) -> str | None:
    node = container.css_first(field.selector)
    if node is None:
        return None
    if field.attribute:
        return node.attributes.get(field.attribute)
    return node.text(deep=True)


def parse(html: str, target: TargetConfig) -> list[dict[str, str | None]]:
    tree = HTMLParser(html)
    containers = tree.css(target.container)
    log.info("%s: found %d containers", target.name, len(containers))

    records: list[dict[str, str | None]] = []
    for container in containers:
        record = {}
        for name, field_def in target.fields.items():
            record[name] = _extract_field(container, field_def)
        records.append(record)

    return records
