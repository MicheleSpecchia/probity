from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.execution.artifact import EXECUTION_POLICY_VERSION, build_execution_artifact
from pmx.execution.policy import ExecutionPolicyConfig, apply_execution_policy
from pmx.execution.validate_artifact import validate_execution_artifact


def _load_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "trade_plans" / "trade_plan_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_artifact() -> dict[str, Any]:
    trade_plan = _load_fixture()
    policy_config = ExecutionPolicyConfig(
        mode="dry_run",
        max_orders=200,
        simulate_reject_modulo=0,
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
        nonce="execution-artifact-schema-test",
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


def test_execution_artifact_schema_valid_payload() -> None:
    artifact = _build_artifact()
    errors = validate_execution_artifact(artifact)
    assert errors == []


def test_execution_artifact_schema_missing_required_field() -> None:
    artifact = _build_artifact()
    artifact.pop("orders")

    errors = validate_execution_artifact(artifact)
    assert len(errors) >= 1
    assert errors[0]["code"] == "schema:required"
    assert errors[0]["path"] == "$"
    assert "orders" in str(errors[0]["reason"])
