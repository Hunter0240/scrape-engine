import pytest
from pydantic import ValidationError

from engine.config import TargetConfig, load_all_targets, load_target


def test_load_hackernews():
    target = load_target("targets/hackernews.yaml")
    assert target.name == "hackernews"
    assert "title" in target.fields
    assert target.fields["rank"].type == "int"
    assert target.key_fields == ["title", "url"]


def test_load_github_trending():
    target = load_target("targets/github-trending.yaml")
    assert target.name == "github-trending"
    assert target.fields["repo"].attribute == "href"


def test_load_all():
    targets = load_all_targets()
    names = [t.name for t in targets]
    assert "hackernews" in names
    assert "github-trending" in names


def test_missing_required_fields():
    with pytest.raises(ValidationError):
        TargetConfig(name="bad", url="http://x")


def test_defaults():
    target = TargetConfig(
        name="test",
        url="http://x",
        container="div",
        fields={"a": {"selector": "span"}},
    )
    assert target.rate_limit == 1.0
    assert target.max_retries == 3
    assert target.key_fields is None
    assert target.webhook_url is None
    assert target.schedule is None
