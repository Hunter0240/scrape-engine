from pathlib import Path

from engine.config import load_target
from engine.parser import parse

FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_hackernews_fixture():
    target = load_target("targets/hackernews.yaml")
    html = (FIXTURES / "hackernews.html").read_text()
    records = parse(html, target)

    assert len(records) == 3

    assert records[0]["title"] == "First Article"
    assert records[0]["url"] == "https://example.com/article-one"
    assert records[0]["rank"] == "1."
    assert records[0]["site"] == "example.com"

    assert records[1]["title"] == "Second Article"
    assert records[1]["url"] == "https://example.com/article-two"


def test_parse_missing_field():
    target = load_target("targets/hackernews.yaml")
    html = (FIXTURES / "hackernews.html").read_text()
    records = parse(html, target)

    assert records[2]["site"] is None
    assert records[2]["title"] == "Third Article"


def test_parse_empty_html():
    target = load_target("targets/hackernews.yaml")
    records = parse("<html><body></body></html>", target)
    assert records == []
