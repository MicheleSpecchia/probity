from __future__ import annotations

from datetime import UTC, datetime

from pmx.ingest.gamma_catalog import infer_market_outcome


def test_infer_market_outcome_prefers_explicit_field() -> None:
    payload = {
        "id": "m-explicit",
        "winningOutcome": "Yes",
        "resolvedAt": "2026-02-10T12:00:00Z",
        "outcomes": '["Yes","No"]',
        "outcomePrices": '["0.01","0.99"]',
    }

    resolved, outcome, resolved_ts, resolver_source = infer_market_outcome(payload)

    assert resolved is True
    assert outcome == "Yes"
    assert resolved_ts == datetime(2026, 2, 10, 12, 0, tzinfo=UTC)
    assert resolver_source == "explicit:winningOutcome"


def test_infer_market_outcome_uses_outcome_prices_unique_argmax() -> None:
    payload = {
        "id": "m-price",
        "outcomes": '["Yes","No"]',
        "outcomePrices": '["0.995","0.005"]',
        "updatedAt": "2026-02-11T08:30:00Z",
    }

    resolved, outcome, resolved_ts, resolver_source = infer_market_outcome(payload)

    assert resolved is True
    assert outcome == "Yes"
    assert resolved_ts == datetime(2026, 2, 11, 8, 30, tzinfo=UTC)
    assert resolver_source == "inferred:outcomePrices_unique_max_ge_0.99"


def test_infer_market_outcome_returns_unresolved_when_signal_is_ambiguous() -> None:
    payload = {
        "id": "m-unresolved",
        "outcomes": '["Yes","No"]',
        "outcomePrices": '["0.50","0.50"]',
        "updatedAt": "2026-02-11T08:30:00Z",
    }

    resolved, outcome, resolved_ts, resolver_source = infer_market_outcome(payload)

    assert resolved is False
    assert outcome is None
    assert resolved_ts is None
    assert resolver_source == "unresolved"
