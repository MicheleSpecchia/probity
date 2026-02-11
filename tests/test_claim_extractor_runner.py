from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.claims.audit import compute_prompt_hash
from pmx.claims.extractor import build_prompt, validate_and_normalize
from pmx.jobs.claim_extract_stub import ClaimExtractStubConfig, run_claim_extract_stub


def _load_claim_fixture(name: str) -> dict[str, Any]:
    fixture_path = Path(__file__).with_name("fixtures") / "claims" / name
    with fixture_path.open("r", encoding="utf-8") as handle:
        loaded = json.load(handle)
    if not isinstance(loaded, dict):
        raise ValueError(f"Invalid fixture object: {name}")
    return loaded


def _sample_articles() -> list[dict[str, Any]]:
    return [
        {
            "article_id": 1002,
            "canonical_url": "https://example.com/b",
            "published_at": "2026-02-11T09:00:00Z",
            "title": "Second article",
            "body": "Body B",
        },
        {
            "article_id": 1001,
            "canonical_url": "https://example.com/a",
            "published_at": "2026-02-11T09:00:00Z",
            "title": "First article",
            "body": "Body A",
        },
        {
            "article_id": 1003,
            "canonical_url": "https://example.com/a-2",
            "published_at": "2026-02-11T10:00:00Z",
            "title": "Third article",
            "body": "Body C",
        },
    ]


def _extract_prompt_payload(prompt: str) -> dict[str, Any]:
    prefix = "input="
    _, payload = prompt.split(prefix, 1)
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("Prompt payload must be JSON object")
    return parsed


def test_build_prompt_is_deterministic_and_sorted() -> None:
    articles = _sample_articles()
    prompt_a = build_prompt("market-1", articles, schema_version="claim_extract.v1")
    prompt_b = build_prompt(
        "market-1",
        list(reversed(articles)),
        schema_version="claim_extract.v1",
    )

    assert prompt_a == prompt_b
    parsed = _extract_prompt_payload(prompt_a)
    assert parsed["schema_version"] == "claim_extract.v1"
    assert [row["article_id"] for row in parsed["articles"]] == [1001, 1002, 1003]


def test_claim_extract_stub_runner_valid_output_writes_audit_bundle(tmp_path: Path) -> None:
    articles = _sample_articles()
    fixture_path = Path(__file__).with_name("fixtures") / "claims" / "stub_llm_output_valid.json"
    decision_ts = datetime(2026, 2, 11, 12, 0, tzinfo=UTC)
    config = ClaimExtractStubConfig(
        ingest_epsilon_seconds=300,
        artifacts_root=str(tmp_path),
        claim_schema_version="claim_extract.v1",
        evidence_schema_version="evidence_checklist.v1",
    )

    result = run_claim_extract_stub(
        config=config,
        market_id="market-election-2026",
        articles=articles,
        decision_ts=decision_ts,
        stub_output_path=str(fixture_path),
        nonce="stable-nonce",
    )

    assert result["used_fallback"] == 0
    artifact_path = Path(result["artifact_path"])
    assert artifact_path.exists()

    with artifact_path.open("r", encoding="utf-8") as handle:
        artifact = json.load(handle)

    assert artifact["run_id"] == result["run_id"]
    assert artifact["job_name"] == "claim_extract_stub"
    assert artifact["decision_ts"] == decision_ts.isoformat()
    assert artifact["ingest_epsilon_seconds"] == 300
    assert artifact["schema_versions"]["claim_extract"] == "claim_extract.v1"
    assert artifact["schema_versions"]["evidence_checklist"] == "evidence_checklist.v1"
    assert artifact["input_article_ids"] == [1001, 1002, 1003]
    assert artifact["input_canonical_urls"] == [
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/a-2",
    ]
    assert artifact["validator_errors"] == []
    assert artifact["no_trade_flags"] == []
    assert artifact["prompt_hash"] == compute_prompt_hash(
        build_prompt("market-election-2026", articles, schema_version="claim_extract.v1")
    )


def test_claim_extract_stub_runner_invalid_output_triggers_fallback(tmp_path: Path) -> None:
    articles = _sample_articles()
    fixture_path = Path(__file__).with_name("fixtures") / "claims" / "stub_llm_output_invalid.json"
    invalid_payload = _load_claim_fixture("stub_llm_output_invalid.json")
    decision_ts = datetime(2026, 2, 11, 12, 0, tzinfo=UTC)
    config = ClaimExtractStubConfig(
        ingest_epsilon_seconds=300,
        artifacts_root=str(tmp_path),
        claim_schema_version="claim_extract.v1",
        evidence_schema_version="evidence_checklist.v1",
    )

    result_a = run_claim_extract_stub(
        config=config,
        market_id="market-election-2026",
        articles=articles,
        decision_ts=decision_ts,
        stub_output_path=str(fixture_path),
        nonce="stable-nonce-invalid",
    )
    with Path(result_a["artifact_path"]).open("r", encoding="utf-8") as handle:
        artifact_a = json.load(handle)

    result_b = run_claim_extract_stub(
        config=config,
        market_id="market-election-2026",
        articles=articles,
        decision_ts=decision_ts,
        stub_output_path=str(fixture_path),
        nonce="stable-nonce-invalid",
    )
    with Path(result_b["artifact_path"]).open("r", encoding="utf-8") as handle:
        artifact_b = json.load(handle)

    outcome_a = validate_and_normalize(
        invalid_payload,
        market_id="market-election-2026",
        schema_version="claim_extract.v1",
    )
    outcome_b = validate_and_normalize(
        invalid_payload,
        market_id="market-election-2026",
        schema_version="claim_extract.v1",
    )

    assert result_a["used_fallback"] == 1
    assert "llm_invalid_output" in artifact_a["no_trade_flags"]
    assert artifact_a["payload"]["claims"] == []
    assert artifact_a["payload"]["claims_raw"] == []
    assert artifact_a["validator_errors"] == artifact_a["payload"]["errors"]
    assert artifact_a == artifact_b
    assert outcome_a.validator_errors == outcome_b.validator_errors
