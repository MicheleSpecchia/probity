from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.monitoring.artifact import MONITORING_POLICY_VERSION, build_monitoring_report_artifact
from pmx.monitoring.policy import MonitoringPolicyConfig, evaluate_monitoring_health


def _load_critical_risk_fixture() -> dict[str, Any]:
    path = (
        Path(__file__).with_name("fixtures") / "monitoring" / "risk_artifact_critical_sample.json"
    )
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _pipeline_fixture() -> dict[str, Any]:
    return {
        "run_id": "pipeline-run",
        "pipeline_payload_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "quality_flags": ["poor_calibration", "stale"],
        "quality_warnings": [
            {"code": "stale", "message": "Book stale.", "source": "pipeline"},
            {"code": "poor_calibration", "message": "ECE high.", "source": "pipeline"},
        ],
    }


def _build_artifact(
    *,
    pipeline_artifact: dict[str, Any],
    risk_artifact: dict[str, Any],
) -> dict[str, Any]:
    config = MonitoringPolicyConfig()
    result = evaluate_monitoring_health(
        pipeline_artifact=pipeline_artifact,
        risk_artifact=risk_artifact,
        config=config,
    )
    run_context = build_run_context(
        "monitor_from_pipeline",
        {
            "input_hashes": [
                pipeline_artifact["pipeline_payload_hash"],
                risk_artifact["risk_payload_hash"],
            ],
            "policy_version": MONITORING_POLICY_VERSION,
        },
        started_at=datetime(2026, 2, 12, tzinfo=UTC),
        nonce="monitoring-determinism",
    )
    return build_monitoring_report_artifact(
        run_context=run_context,
        pipeline_artifact=pipeline_artifact,
        risk_artifact=risk_artifact,
        params=config.as_hash_dict(),
        health_status=result.health_status,
        health_summary=result.health_summary,
        quality_flags=result.quality_flags,
        quality_warnings=result.quality_warnings,
        policy_version=MONITORING_POLICY_VERSION,
    )


def test_monitoring_hashes_are_deterministic_and_fail_on_critical_block() -> None:
    risk_artifact = _load_critical_risk_fixture()
    pipeline_artifact = _pipeline_fixture()
    first = _build_artifact(pipeline_artifact=pipeline_artifact, risk_artifact=risk_artifact)

    pipeline_permuted = dict(pipeline_artifact)
    pipeline_permuted["quality_flags"] = ["stale", "poor_calibration"]
    pipeline_permuted["quality_warnings"] = list(reversed(pipeline_artifact["quality_warnings"]))
    second = _build_artifact(pipeline_artifact=pipeline_permuted, risk_artifact=risk_artifact)

    assert first["health_status"] == "FAIL"
    assert first["monitoring_policy_hash"] == second["monitoring_policy_hash"]
    assert first["monitoring_inputs_hash"] == second["monitoring_inputs_hash"]
    assert first["monitoring_payload_hash"] == second["monitoring_payload_hash"]
    assert first["quality_flags"] == second["quality_flags"]
    assert first["quality_warnings"] == second["quality_warnings"]
