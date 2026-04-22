from __future__ import annotations

from datetime import UTC, datetime

from pmx.ingest.gamma_catalog import normalize_market_payload, parse_rule_text


def test_parse_rule_text_is_deterministic_for_same_input() -> None:
    rule_text = "Resolves YES if candidate wins by 2026-11-03, otherwise NO."

    first_ok, first_payload = parse_rule_text(rule_text)
    second_ok, second_payload = parse_rule_text(rule_text)

    assert first_ok is False
    assert second_ok is False
    assert first_payload == second_payload
    assert first_payload["version"] == "stub_v1"
    assert "detected_keywords" in first_payload["signals"]


def test_parse_rule_text_handles_empty_text() -> None:
    ok, payload = parse_rule_text(None)

    assert ok is False
    assert payload["signals"]["has_text"] is False
    assert payload["notes"].startswith("Rule parser stub")


def test_normalize_market_payload_uses_stable_audit_namespace() -> None:
    payload = {
        "id": "m-1",
        "title": "Sample market",
        "status": "active",
        "rule_text": "Resolves YES if condition is met.",
        "tokens": [{"outcome": "YES", "tokenId": "t-yes"}],
        "z_last": 1,
        "a_first": 2,
    }

    market_record, _ = normalize_market_payload(
        payload,
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        gamma_etag="etag-1",
    )

    assert market_record is not None
    assert list(market_record.rule_parse_json.keys()) == ["version", "signals", "notes", "audit"]
    assert list(market_record.rule_parse_json["audit"].keys()) == [
        "gamma_raw",
        "ingested_at",
        "gamma_etag",
    ]
    assert list(market_record.rule_parse_json["audit"]["gamma_raw"].keys()) == [
        "a_first",
        "id",
        "rule_text",
        "status",
        "title",
        "tokens",
        "z_last",
    ]


def test_normalize_market_payload_parses_stringified_outcomes_and_tokens() -> None:
    payload = {
        "id": "m-2",
        "title": "Sample market 2",
        "status": "active",
        "outcomes": '["YES","NO"]',
        "clobTokenIds": '["t-yes","t-no"]',
    }

    _, tokens = normalize_market_payload(
        payload,
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert [(token.outcome, token.token_id) for token in tokens] == [
        ("NO", "t-no"),
        ("YES", "t-yes"),
    ]
