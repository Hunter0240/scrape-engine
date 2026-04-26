from engine.config import FieldDef, TargetConfig
from engine.deduper import add_hashes


def _make_target(key_fields: list[str] | None = None) -> TargetConfig:
    fields = {
        "title": FieldDef(selector="x"),
        "url": FieldDef(selector="x", type="url"),
    }
    return TargetConfig(
        name="test",
        url="http://x",
        container="div",
        fields=fields,
        key_fields=key_fields,
    )


def test_hash_deterministic():
    target = _make_target(key_fields=["title"])
    records = [{"title": "hello", "url": "http://x"}]
    add_hashes(records, target)
    h1 = records[0]["_hash"]

    records2 = [{"title": "hello", "url": "http://different"}]
    add_hashes(records2, target)
    h2 = records2[0]["_hash"]

    assert h1 == h2


def test_hash_differs_for_different_content():
    target = _make_target(key_fields=["title"])
    r1 = [{"title": "aaa", "url": "x"}]
    r2 = [{"title": "bbb", "url": "x"}]
    add_hashes(r1, target)
    add_hashes(r2, target)
    assert r1[0]["_hash"] != r2[0]["_hash"]


def test_hash_uses_all_fields_when_no_key_fields():
    target = _make_target(key_fields=None)
    r1 = [{"title": "same", "url": "http://a"}]
    r2 = [{"title": "same", "url": "http://b"}]
    add_hashes(r1, target)
    add_hashes(r2, target)
    assert r1[0]["_hash"] != r2[0]["_hash"]


def test_hash_handles_none():
    target = _make_target(key_fields=["title"])
    records = [{"title": None, "url": "x"}]
    add_hashes(records, target)
    assert "_hash" in records[0]
    assert len(records[0]["_hash"]) == 16
