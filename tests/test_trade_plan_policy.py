from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pmx.trade_plan.policy import TradePlanPolicyConfig, build_trade_plan


def _load_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "decisions" / "decision_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _reason_by_token(skipped: tuple[dict[str, Any], ...], token_id: str) -> str:
    for item in skipped:
        if item["token_id"] == token_id:
            return str(item["reason_code"])
    raise AssertionError(f"missing skipped token_id={token_id}")


def test_trade_plan_policy_is_deterministic_with_stable_ordering() -> None:
    payload = _load_fixture()
    config = TradePlanPolicyConfig(
        max_orders=200,
        max_total_notional_usd=5000.0,
        max_notional_per_market_usd=500.0,
        max_notional_per_category_usd=2000.0,
        sizing_mode="fixed_notional",
        fixed_notional_usd=100.0,
    )

    first = build_trade_plan(payload, config)
    second = build_trade_plan(payload, config)

    assert first == second
    assert [item["market_id"] for item in first.orders] == ["mkt-b", "mkt-a", "mkt-e", "mkt-f"]
    assert [item["side"] for item in first.orders] == ["BUY_NO", "BUY_YES", "BUY_YES", "BUY_NO"]
    assert [item["reason_code"] for item in first.skipped] == [
        "blocked_by_quality_flag:illiquid",
        "decision_no_trade:edge_below_threshold",
    ]
    assert [item["notional_usd"] for item in first.orders] == [100.0, 100.0, 100.0, 100.0]


def test_trade_plan_policy_enforces_caps_without_partial_fills() -> None:
    payload = _load_fixture()
    payload["items"][4]["market_id"] = "mkt-a"

    config = TradePlanPolicyConfig(
        max_orders=2,
        max_total_notional_usd=250.0,
        max_notional_per_market_usd=150.0,
        max_notional_per_category_usd=2000.0,
        sizing_mode="fixed_notional",
        fixed_notional_usd=100.0,
    )
    result = build_trade_plan(payload, config)

    assert [item["market_id"] for item in result.orders] == ["mkt-b", "mkt-a"]
    assert _reason_by_token(result.skipped, "tok-c") == "blocked_by_quality_flag:illiquid"
    assert _reason_by_token(result.skipped, "tok-d").startswith("decision_no_trade")
    assert _reason_by_token(result.skipped, "tok-e") == "cap_exceeded:max_orders"
    assert _reason_by_token(result.skipped, "tok-f") == "cap_exceeded:max_orders"


def test_trade_plan_policy_scaled_sizing_and_zero_edge_skip() -> None:
    payload = _load_fixture()
    payload["items"][4]["edge_bps"] = 0.0
    payload["items"][4]["edge"] = 0.0

    config = TradePlanPolicyConfig(
        max_orders=200,
        max_total_notional_usd=5000.0,
        max_notional_per_market_usd=5000.0,
        max_notional_per_category_usd=5000.0,
        sizing_mode="scaled_by_edge",
        base_notional_usd=100.0,
        target_edge_bps=1000.0,
        min_scale=0.5,
        max_scale=2.0,
    )
    result = build_trade_plan(payload, config)

    assert [item["market_id"] for item in result.orders] == ["mkt-b", "mkt-a", "mkt-f"]
    assert [item["notional_usd"] for item in result.orders] == [110.0, 80.0, 50.0]
    assert _reason_by_token(result.skipped, "tok-e") == "zero_edge"
