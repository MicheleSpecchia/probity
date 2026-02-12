from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pmx.decisions.policy import DecisionPolicyConfig, decide_from_forecast_artifact


def _load_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "forecast" / "forecast_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _by_market(items: tuple[dict[str, Any], ...], market_id: str) -> dict[str, Any]:
    for item in items:
        if item["market_id"] == market_id:
            return item
    raise AssertionError(f"market not found: {market_id}")


def test_decision_policy_default_mode_is_deterministic_and_stable() -> None:
    payload = _load_fixture()
    config = DecisionPolicyConfig(min_edge_bps=50.0, robust_mode="require_positive_low90")

    first = decide_from_forecast_artifact(payload, config)
    second = decide_from_forecast_artifact(payload, config)

    assert first == second
    assert [item["market_id"] for item in first] == [
        "mkt-b",
        "mkt-f",
        "mkt-a",
        "mkt-c",
        "mkt-d",
        "mkt-e",
    ]

    assert _by_market(first, "mkt-b")["action"] == "BUY_NO"
    assert _by_market(first, "mkt-f")["action"] == "BUY_NO"
    assert _by_market(first, "mkt-a")["action"] == "BUY_YES"
    assert _by_market(first, "mkt-c")["no_trade_reasons"] == ["flag:illiquid"]
    assert _by_market(first, "mkt-d")["no_trade_reasons"] == ["robust_check_failed"]
    assert _by_market(first, "mkt-e")["no_trade_reasons"] == ["edge_below_threshold"]


def test_decision_policy_negative_high90_mode_blocks_buy_no_with_positive_high90() -> None:
    payload = _load_fixture()
    config = DecisionPolicyConfig(min_edge_bps=50.0, robust_mode="require_negative_high90")

    decisions = decide_from_forecast_artifact(payload, config)

    assert [item["market_id"] for item in decisions] == [
        "mkt-b",
        "mkt-a",
        "mkt-d",
        "mkt-c",
        "mkt-e",
        "mkt-f",
    ]
    assert _by_market(decisions, "mkt-b")["action"] == "BUY_NO"
    assert _by_market(decisions, "mkt-f")["action"] == "NO_TRADE"
    assert _by_market(decisions, "mkt-f")["no_trade_reasons"] == ["robust_check_failed"]
    assert _by_market(decisions, "mkt-d")["action"] == "BUY_YES"
