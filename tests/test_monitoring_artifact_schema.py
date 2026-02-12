from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.monitoring.artifact import MONITORING_POLICY_VERSION, build_monitoring_report_artifact
from pmx.monitoring.validate_artifact import validate_monitoring_report_artifact


def _pipeline_stub() -> dict[str, Any]:
    return {
        "run_id": "pipeline-run-sample",
        "pipeline_payload_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    }


def _build_artifact() -> dict[str, Any]:
    run_context = build_run_context(
        "monitor_from_pipeline",
        {
            "input_hashes": [
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ],
            "policy_version": MONITORING_POLICY_VERSION,
        },
        started_at=datetime(2026, 2, 12, tzinfo=UTC),
        nonce="monitoring-artifact-schema",
    )
    return build_monitoring_report_artifact(
        run_context=run_context,
        pipeline_artifact=_pipeline_stub(),
        params={"artifacts_root": "artifacts"},
        health_status="WARN",
        health_summary={
            "n_flags": 1,
            "n_warnings": 1,
            "n_fail_codes": 0,
            "fail_codes": [],
            "sources": {"pipeline": {"flags": 1, "warnings": 1}},
        },
        quality_flags=("poor_calibration",),
        quality_warnings=(
            {
                "code": "poor_calibration",
                "message": "ECE threshold breached.",
                "source": "pipeline",
            },
        ),
        policy_version=MONITORING_POLICY_VERSION,
    )


def test_monitoring_artifact_schema_valid_payload() -> None:
    artifact = _build_artifact()
    assert validate_monitoring_report_artifact(artifact) == []


def test_monitoring_artifact_schema_missing_required_field() -> None:
    artifact = _build_artifact()
    artifact.pop("health_status")

    errors = validate_monitoring_report_artifact(artifact)
    assert len(errors) >= 1
    assert errors[0]["code"] == "schema:required"
    assert errors[0]["path"] == "$"
    assert "health_status" in str(errors[0]["reason"])
