from engine.cleaner import clean
from engine.config import FieldDef, TargetConfig


def _make_target(**field_types: str) -> TargetConfig:
    fields = {
        name: FieldDef(selector="x", type=t) for name, t in field_types.items()
    }
    return TargetConfig(name="test", url="http://x", container="div", fields=fields)


def test_strip_whitespace():
    target = _make_target(name="str")
    records = [{"name": "  hello   world  "}]
    result = clean(records, target)
    assert result[0]["name"] == "hello world"


def test_unicode_normalization():
    target = _make_target(name="str")
    records = [{"name": "café\xa0latte"}]
    result = clean(records, target)
    assert "\xa0" not in result[0]["name"]
    assert result[0]["name"] == "café latte"


def test_coerce_int():
    target = _make_target(rank="int")
    records = [{"rank": "42."}]
    result = clean(records, target)
    assert result[0]["rank"] == 42
    assert isinstance(result[0]["rank"], int)


def test_coerce_int_with_commas():
    target = _make_target(count="int")
    records = [{"count": "1,234"}]
    result = clean(records, target)
    assert result[0]["count"] == 1234


def test_coerce_float():
    target = _make_target(price="float")
    records = [{"price": "$19.99"}]
    result = clean(records, target)
    assert result[0]["price"] == 19.99


def test_none_passthrough():
    target = _make_target(name="str")
    records = [{"name": None}]
    result = clean(records, target)
    assert result[0]["name"] is None


def test_coerce_int_empty():
    target = _make_target(rank="int")
    records = [{"rank": "no digits here"}]
    result = clean(records, target)
    assert result[0]["rank"] == 0
