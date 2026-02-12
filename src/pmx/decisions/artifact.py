from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.decisions.canonical import canonical_hash

DECISION_ARTIFACT_SCHEMA_VERSION = "decision_artifact.v1"
DECISION_POLICY_VERSION = "decision_policy.v1"


def build_decision_artifact(
    *,
    run_context: RunContext,
    forecast_artifact: Mapping[str, Any],
    params: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
    policy_version: str = DECISION_POLICY_VERSION,
) -> dict[str, Any]:
    params_payload = _normalize_mapping(params)
    items_payload = [_normalize_mapping(item) for item in items]

    payload: dict[str, Any] = {
        "decision_schema_version": DECISION_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_context.run_id,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "input_forecast_run_id": _optional_text(forecast_artifact.get("run_id")),
        "forecast_payload_hash": _optional_hash(forecast_artifact.get("forecast_payload_hash")),
        "dataset_hash": _optional_hash(forecast_artifact.get("dataset_hash")),
        "model_hash": _optional_hash(forecast_artifact.get("model_hash")),
        "calibration_hash": _optional_hash(forecast_artifact.get("calibration_hash")),
        "uncertainty_hash": _optional_hash(forecast_artifact.get("uncertainty_hash")),
        "policy_version": policy_version,
        "params": params_payload,
        "items": items_payload,
    }
    payload["policy_hash"] = canonical_hash(
        {
            "policy_version": policy_version,
            "params": params_payload,
        }
    )
    payload["decision_items_hash"] = canonical_hash(items_payload)

    payload_without_self = dict(payload)
    payload["decision_payload_hash"] = canonical_hash(payload_without_self)
    return payload


def _normalize_mapping(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): value for key, value in raw.items()}


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _optional_hash(raw: Any) -> str | None:
    value = _optional_text(raw)
    if value is None:
        return None
    if len(value) != 64 or not all(char in "0123456789abcdef" for char in value):
        raise ValueError(f"Expected lowercase sha256 hash, got {value!r}")
    return value
