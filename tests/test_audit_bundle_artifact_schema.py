from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.audit_bundle.artifact import AUDIT_BUNDLE_POLICY_VERSION, build_audit_bundle_artifact
from pmx.audit_bundle.validate_artifact import validate_audit_bundle_artifact


def _stage_events_fixture() -> list[dict[str, Any]]:
    path = Path(__file__).with_name("fixtures") / "audit" / "audit_stage_events_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_artifact() -> dict[str, Any]:
    events = _stage_events_fixture()
    run_context = build_run_context(
        "build_audit_bundle",
        {
            "input_hashes": [event["payload_hash"] for event in events],
            "policy_version": AUDIT_BUNDLE_POLICY_VERSION,
        },
        started_at=datetime(2026, 2, 12, tzinfo=UTC),
        nonce="audit-bundle-schema",
    )
    return build_audit_bundle_artifact(
        run_context=run_context,
        params={"artifacts_root": "artifacts"},
        inputs=events,
        timeline=events,
        quality_flags=("insufficient_calibration_data",),
        quality_warnings=(
            {
                "code": "insufficient_calibration_data",
                "message": "Calibration sample too small.",
                "source": "forecast_artifact.v1",
            },
        ),
        policy_version=AUDIT_BUNDLE_POLICY_VERSION,
    )


def test_audit_bundle_artifact_schema_valid_payload() -> None:
    artifact = _build_artifact()
    assert validate_audit_bundle_artifact(artifact) == []


def test_audit_bundle_artifact_schema_missing_required_field() -> None:
    artifact = _build_artifact()
    artifact.pop("timeline")

    errors = validate_audit_bundle_artifact(artifact)
    assert len(errors) >= 1
    assert errors[0]["code"] == "schema:required"
    assert errors[0]["path"] == "$"
    assert "timeline" in str(errors[0]["reason"])
