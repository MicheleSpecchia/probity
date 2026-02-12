from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.risk.artifact import RISK_POLICY_VERSION, build_risk_artifact
from pmx.risk.policy import RiskPolicyConfig, evaluate_risk_policy
from pmx.risk.validate_artifact import validate_risk_artifact


def _trade_plan_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "trade_plans" / "trade_plan_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_artifact() -> dict[str, Any]:
    trade_plan = _trade_plan_fixture()
    policy_config = RiskPolicyConfig()
    result = evaluate_risk_policy(trade_plan, policy_config)
    run_context = build_run_context(
        "risk_from_trade_plan",
        {
            "input_trade_plan_artifact_hash": trade_plan["trade_plan_payload_hash"],
            "params": policy_config.as_hash_dict(),
            "policy_version": RISK_POLICY_VERSION,
        },
        started_at=datetime(2026, 2, 12, tzinfo=UTC),
        nonce="risk-artifact-schema",
    )
    return build_risk_artifact(
        run_context=run_context,
        trade_plan_artifact=trade_plan,
        params=policy_config.as_hash_dict(),
        items=result.items,
        counts=result.counts,
        notional_summary=result.notional_summary,
        quality_flags=result.quality_flags,
        quality_warnings=result.quality_warnings,
        policy_version=RISK_POLICY_VERSION,
    )


def test_risk_artifact_schema_valid_payload() -> None:
    artifact = _build_artifact()
    assert validate_risk_artifact(artifact) == []


def test_risk_artifact_schema_missing_required_field() -> None:
    artifact = _build_artifact()
    artifact.pop("items")

    errors = validate_risk_artifact(artifact)
    assert len(errors) >= 1
    assert errors[0]["code"] == "schema:required"
    assert errors[0]["path"] == "$"
    assert "items" in str(errors[0]["reason"])
