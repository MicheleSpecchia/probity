from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.execution.artifact import EXECUTION_POLICY_VERSION, build_execution_artifact
from pmx.execution.policy import ExecutionPolicyConfig, apply_execution_policy


def _load_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "trade_plans" / "trade_plan_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_artifact() -> dict[str, Any]:
    trade_plan = _load_fixture()
    policy_config = ExecutionPolicyConfig(
        mode="simulate_submit",
        max_orders=200,
        simulate_reject_modulo=2,
        simulate_reject_remainder=0,
    )
    result = apply_execution_policy(trade_plan_artifact=trade_plan, config=policy_config)
    run_context = build_run_context(
        "execute_trade_plan_stub",
        {
            "input_trade_plan_artifact_hash": trade_plan["trade_plan_payload_hash"],
            "policy_version": EXECUTION_POLICY_VERSION,
            "params": policy_config.as_hash_dict(),
        },
        started_at=datetime(2026, 2, 3, tzinfo=UTC),
        nonce="execution-hashing-test",
    )
    return build_execution_artifact(
        run_context=run_context,
        trade_plan_artifact=trade_plan,
        params=policy_config.as_hash_dict(),
        idempotency_key=result.idempotency_key,
        orders=result.orders,
        skipped=result.skipped,
        policy_version=EXECUTION_POLICY_VERSION,
    )


def test_execution_hashes_are_deterministic_for_same_input() -> None:
    first = _build_artifact()
    second = _build_artifact()

    assert first["execution_policy_hash"] == second["execution_policy_hash"]
    assert first["orders_hash"] == second["orders_hash"]
    assert first["execution_payload_hash"] == second["execution_payload_hash"]
    assert [item["client_order_id"] for item in first["orders"]] == [
        item["client_order_id"] for item in second["orders"]
    ]
