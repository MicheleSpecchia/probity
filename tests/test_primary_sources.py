from __future__ import annotations

from pathlib import Path

from pmx.news.primary_sources import load_primary_sources_config, match_primary_source_policy


def test_primary_sources_loader_and_subdomain_match() -> None:
    config = load_primary_sources_config(Path("config") / "primary_sources.yaml")

    assert config.version == 1
    assert len(config.domains) >= 2

    policy = match_primary_source_policy("www.reuters.com", config.domains)
    assert policy is not None
    assert policy.domain == "reuters.com"

    none_policy = match_primary_source_policy("unknown.example.com", config.domains)
    assert none_policy is None
