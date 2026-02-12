from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.decisions.artifact import DECISION_POLICY_VERSION, build_decision_artifact
from pmx.decisions.policy import DecisionPolicyConfig, decide_from_forecast_artifact
from pmx.decisions.validate_artifact import validate_decision_artifact


def _load_fixture() -> dict[str, Any]:
    path = Path(__file__).with_name("fixtures") / "forecast" / "forecast_artifact_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_artifact() -> dict[str, Any]:
    forecast = _load_fixture()
    policy = DecisionPolicyConfig(min_edge_bps=50.0, robust_mode="require_positive_low90")
    items = decide_from_forecast_artifact(forecast, policy)
    run_context = build_run_context(
        "decide_from_forecast",
        {
            "forecast_payload_hash": forecast["forecast_payload_hash"],
            "params": policy.as_hash_dict(),
            "policy_version": DECISION_POLICY_VERSION,
        },
        started_at=datetime(2026, 2, 1, tzinfo=UTC),
        nonce="decision-artifact-schema-test",
    )
    return build_decision_artifact(
        run_context=run_context,
        forecast_artifact=forecast,
        params=policy.as_hash_dict(),
        items=items,
        policy_version=DECISION_POLICY_VERSION,
    )


def test_decision_artifact_schema_valid_payload() -> None:
    artifact = _build_artifact()
    errors = validate_decision_artifact(artifact)
    assert errors == []


def test_decision_artifact_hashes_are_deterministic() -> None:
    first = _build_artifact()
    second = _build_artifact()

    assert first["policy_hash"] == second["policy_hash"]
    assert first["decision_items_hash"] == second["decision_items_hash"]
    assert first["decision_payload_hash"] == second["decision_payload_hash"]


def test_decision_artifact_schema_missing_required_field() -> None:
    artifact = _build_artifact()
    artifact.pop("items")

    errors = validate_decision_artifact(artifact)
    assert len(errors) >= 1
    assert errors[0]["code"] == "schema:required"
    assert errors[0]["path"] == "$"
    assert "items" in str(errors[0]["reason"])
