from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.run_context import build_run_context
from pmx.audit_bundle.artifact import AUDIT_BUNDLE_POLICY_VERSION, build_audit_bundle_artifact


def _stage_events_fixture() -> list[dict[str, Any]]:
    path = Path(__file__).with_name("fixtures") / "audit" / "audit_stage_events_sample.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build(events: list[dict[str, Any]]) -> dict[str, Any]:
    run_context = build_run_context(
        "build_audit_bundle",
        {
            "input_hashes": sorted(event["payload_hash"] for event in events),
            "policy_version": AUDIT_BUNDLE_POLICY_VERSION,
        },
        started_at=datetime(2026, 2, 12, tzinfo=UTC),
        nonce="audit-bundle-determinism",
    )
    return build_audit_bundle_artifact(
        run_context=run_context,
        params={"artifacts_root": "artifacts"},
        inputs=events,
        timeline=list(reversed(events)),
        quality_flags=("stale", "illiquid"),
        quality_warnings=(
            {"code": "stale", "message": "Book stale.", "source": "execution_artifact.v1"},
            {"code": "illiquid", "message": "Depth low.", "source": "forecast_artifact.v1"},
        ),
        policy_version=AUDIT_BUNDLE_POLICY_VERSION,
    )


def test_audit_bundle_hashes_are_deterministic_with_permuted_inputs() -> None:
    events = _stage_events_fixture()
    first = _build(events)
    second = _build(list(reversed(events)))

    assert first["bundle_hash"] == second["bundle_hash"]
    assert first["audit_bundle_policy_hash"] == second["audit_bundle_policy_hash"]
    assert first["audit_bundle_payload_hash"] == second["audit_bundle_payload_hash"]
    assert first["inputs"] == second["inputs"]
    assert first["timeline"] == second["timeline"]
