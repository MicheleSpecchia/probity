from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.trade_plan.canonical import canonical_hash

TRADE_PLAN_ARTIFACT_SCHEMA_VERSION = "trade_plan_artifact.v1"
TRADE_PLAN_POLICY_VERSION = "trade_plan_policy.v1"


def build_trade_plan_artifact(
    *,
    run_context: RunContext,
    decision_artifact: Mapping[str, Any],
    params: Mapping[str, Any],
    orders: Sequence[Mapping[str, Any]],
    skipped: Sequence[Mapping[str, Any]],
    policy_version: str = TRADE_PLAN_POLICY_VERSION,
    input_decision_artifact_hash: str | None = None,
) -> dict[str, Any]:
    params_payload = _normalize_mapping(params)
    orders_payload = [_normalize_mapping(item) for item in orders]
    skipped_payload = [_normalize_mapping(item) for item in skipped]
    resolved_input_hash = _resolve_input_decision_hash(
        decision_artifact,
        explicit_hash=input_decision_artifact_hash,
    )

    payload: dict[str, Any] = {
        "artifact_schema_version": TRADE_PLAN_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_context.run_id,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "generated_at_utc": run_context.started_at,
        "input_decision_run_id": _optional_text(decision_artifact.get("run_id")),
        "input_forecast_run_id": _optional_text(decision_artifact.get("input_forecast_run_id")),
        "input_decision_artifact_hash": resolved_input_hash,
        "forecast_payload_hash": _optional_hash(decision_artifact.get("forecast_payload_hash")),
        "dataset_hash": _optional_hash(decision_artifact.get("dataset_hash")),
        "model_hash": _optional_hash(decision_artifact.get("model_hash")),
        "calibration_hash": _optional_hash(decision_artifact.get("calibration_hash")),
        "uncertainty_hash": _optional_hash(decision_artifact.get("uncertainty_hash")),
        "policy_version": policy_version,
        "params": params_payload,
        "orders": orders_payload,
        "skipped": skipped_payload,
        "counts": {
            "n_total": len(orders_payload) + len(skipped_payload),
            "n_orders": len(orders_payload),
            "n_skipped": len(skipped_payload),
        },
    }
    payload["policy_hash"] = canonical_hash(
        {
            "policy_version": policy_version,
            "params": params_payload,
        }
    )
    payload["orders_hash"] = canonical_hash(orders_payload)
    payload_without_self = dict(payload)
    payload["trade_plan_payload_hash"] = canonical_hash(payload_without_self)
    return payload


def _resolve_input_decision_hash(
    decision_artifact: Mapping[str, Any],
    *,
    explicit_hash: str | None,
) -> str:
    explicit = _optional_hash(explicit_hash)
    if explicit is not None:
        return explicit

    payload_hash = _optional_hash(decision_artifact.get("decision_payload_hash"))
    if payload_hash is not None:
        return payload_hash
    return canonical_hash(decision_artifact)


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
