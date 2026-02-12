from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from pmx.execution.policy import ExecutionPolicyConfig, apply_execution_policy


def _load_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "trade_plans" / "trade_plan_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _status_by_token(result_orders: tuple[dict[str, Any], ...], token_id: str) -> str:
    for item in result_orders:
        if item["token_id"] == token_id:
            return str(item["status"])
    raise AssertionError(f"token_id not found: {token_id}")


def test_execution_policy_deterministic_reject_simulation_and_client_order_ids() -> None:
    payload = _load_fixture()
    config = ExecutionPolicyConfig(
        mode="simulate_submit",
        max_orders=200,
        simulate_reject_modulo=2,
        simulate_reject_remainder=0,
    )

    first = apply_execution_policy(trade_plan_artifact=payload, config=config)
    second = apply_execution_policy(trade_plan_artifact=payload, config=config)

    assert first == second
    assert [item["token_id"] for item in first.orders] == ["tok-b", "tok-a", "tok-e", "tok-f"]
    assert len({item["client_order_id"] for item in first.orders}) == len(first.orders)
    assert any(item["status"] == "SIMULATED_REJECTED" for item in first.orders)
    assert _status_by_token(first.orders, "tok-a") in {"SIMULATED_SUBMITTED", "SIMULATED_REJECTED"}


def test_execution_policy_enforces_max_orders_and_invalid_action_skip() -> None:
    payload = _load_fixture()
    payload_mutable = copy.deepcopy(payload)
    payload_mutable["orders"][0]["action"] = "NO_TRADE"

    config = ExecutionPolicyConfig(
        mode="dry_run",
        max_orders=2,
        simulate_reject_modulo=0,
        simulate_reject_remainder=0,
    )
    result = apply_execution_policy(trade_plan_artifact=payload_mutable, config=config)

    assert [item["token_id"] for item in result.orders] == ["tok-a", "tok-e"]
    skipped_reasons = {(item["token_id"], item["reason_code"]) for item in result.skipped}
    assert ("tok-b", "invalid_order_action") in skipped_reasons
    assert ("tok-f", "cap_exceeded:max_orders") in skipped_reasons
