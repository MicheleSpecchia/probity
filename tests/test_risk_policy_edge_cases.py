from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from pmx.risk.policy import RiskHooks, RiskPolicyConfig, evaluate_risk_policy


def _trade_plan_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "trade_plans" / "trade_plan_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _hooks_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "risk" / "risk_hooks_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _item_by_token(result_items: tuple[dict[str, Any], ...], token_id: str) -> dict[str, Any]:
    for item in result_items:
        if item["token_id"] == token_id:
            return item
    raise AssertionError(f"Missing risk item for token_id={token_id}")


def test_risk_policy_blocks_quality_flags_and_cooldown_tokens() -> None:
    trade_plan = _trade_plan_fixture()
    mutated = copy.deepcopy(trade_plan)
    mutated["orders"][1]["quality_flags"] = ["illiquid"]  # tok-a
    hooks = RiskHooks.from_mapping(_hooks_fixture())
    config = RiskPolicyConfig()

    result = evaluate_risk_policy(mutated, config, hooks=hooks)

    tok_a = _item_by_token(result.items, "tok-a")
    tok_e = _item_by_token(result.items, "tok-e")
    assert tok_a["verdict"] == "BLOCK"
    assert "blocked_by_quality_flag:illiquid" in tok_a["reason_codes"]
    assert tok_e["verdict"] == "BLOCK"
    assert "cooldown_active:token:critical_loss" in tok_e["reason_codes"]


def test_risk_policy_downsizes_when_caps_leave_partial_room() -> None:
    trade_plan = _trade_plan_fixture()
    config = RiskPolicyConfig(
        max_total_notional_usd=150.0,
        max_notional_per_market_usd=500.0,
        max_notional_per_category_usd=150.0,
        top1_share_cap=1.0,
        top3_share_cap=1.0,
        allow_downsize=True,
        min_notional_usd=5.0,
    )
    result = evaluate_risk_policy(trade_plan, config)

    tok_b = _item_by_token(result.items, "tok-b")
    tok_a = _item_by_token(result.items, "tok-a")
    tok_e = _item_by_token(result.items, "tok-e")
    tok_f = _item_by_token(result.items, "tok-f")
    assert tok_b["verdict"] == "ALLOW"
    assert tok_b["approved_notional_usd"] == 100.0
    assert tok_a["verdict"] == "DOWNSIZE"
    assert tok_a["approved_notional_usd"] == 50.0
    assert tok_e["verdict"] == "BLOCK"
    assert tok_f["verdict"] == "BLOCK"
    assert result.counts["n_allow"] == 1
    assert result.counts["n_downsize"] == 1
    assert result.counts["n_block"] == 2
