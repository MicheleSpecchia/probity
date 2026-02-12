from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.trade_plan.artifact import TRADE_PLAN_POLICY_VERSION, build_trade_plan_artifact
from pmx.trade_plan.policy import TradePlanPolicyConfig, build_trade_plan
from pmx.trade_plan.validate_artifact import validate_trade_plan_artifact


def _load_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "decisions" / "decision_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_artifact() -> dict[str, Any]:
    decision_artifact = _load_fixture()
    policy_config = TradePlanPolicyConfig(
        max_orders=200,
        max_total_notional_usd=5000.0,
        max_notional_per_market_usd=500.0,
        max_notional_per_category_usd=2000.0,
        sizing_mode="fixed_notional",
        fixed_notional_usd=100.0,
    )
    result = build_trade_plan(decision_artifact, policy_config)
    run_context = build_run_context(
        "trade_plan_from_decision",
        {
            "input_decision_artifact_hash": decision_artifact["decision_payload_hash"],
            "params": policy_config.as_hash_dict(),
            "policy_version": TRADE_PLAN_POLICY_VERSION,
        },
        started_at=datetime(2026, 2, 2, tzinfo=UTC),
        nonce="trade-plan-artifact-schema-test",
    )
    return build_trade_plan_artifact(
        run_context=run_context,
        decision_artifact=decision_artifact,
        params=policy_config.as_hash_dict(),
        orders=result.orders,
        skipped=result.skipped,
        policy_version=TRADE_PLAN_POLICY_VERSION,
    )


def test_trade_plan_artifact_schema_valid_payload() -> None:
    artifact = _build_artifact()
    errors = validate_trade_plan_artifact(artifact)
    assert errors == []


def test_trade_plan_artifact_schema_missing_required_field() -> None:
    artifact = _build_artifact()
    artifact.pop("orders")

    errors = validate_trade_plan_artifact(artifact)
    assert len(errors) >= 1
    assert errors[0]["code"] == "schema:required"
    assert errors[0]["path"] == "$"
    assert "orders" in str(errors[0]["reason"])
