from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.risk.canonical import canonical_hash

RISK_ARTIFACT_SCHEMA_VERSION = "risk_artifact.v1"
RISK_POLICY_VERSION = "risk_policy.v1"


def build_risk_artifact(
    *,
    run_context: RunContext,
    trade_plan_artifact: Mapping[str, Any],
    params: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
    counts: Mapping[str, int],
    notional_summary: Mapping[str, float],
    quality_flags: Sequence[str],
    quality_warnings: Sequence[Mapping[str, Any]],
    performance_artifact: Mapping[str, Any] | None = None,
    policy_version: str = RISK_POLICY_VERSION,
) -> dict[str, Any]:
    params_payload = _normalize_mapping(params)
    items_payload = [_normalize_mapping(item) for item in items]
    counts_payload = _normalize_counts(counts)
    notional_payload = _normalize_notional_summary(notional_summary)
    flags_payload = _normalize_flags(quality_flags)
    warnings_payload = _normalize_warnings(quality_warnings)

    payload: dict[str, Any] = {
        "artifact_schema_version": RISK_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_context.run_id,
        "generated_at_utc": run_context.started_at,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "input_trade_plan_run_id": _optional_text(trade_plan_artifact.get("run_id")),
        "input_trade_plan_artifact_hash": _resolve_input_hash(
            trade_plan_artifact,
            primary_field="trade_plan_payload_hash",
        ),
        "input_performance_run_id": _optional_text(
            None if performance_artifact is None else performance_artifact.get("run_id")
        ),
        "input_performance_artifact_hash": _optional_hash(
            None
            if performance_artifact is None
            else _resolve_input_hash(
                performance_artifact,
                primary_field="performance_payload_hash",
            )
        ),
        "forecast_payload_hash": _optional_hash(trade_plan_artifact.get("forecast_payload_hash")),
        "dataset_hash": _optional_hash(trade_plan_artifact.get("dataset_hash")),
        "model_hash": _optional_hash(trade_plan_artifact.get("model_hash")),
        "calibration_hash": _optional_hash(trade_plan_artifact.get("calibration_hash")),
        "uncertainty_hash": _optional_hash(trade_plan_artifact.get("uncertainty_hash")),
        "decision_payload_hash": _optional_hash(
            trade_plan_artifact.get("input_decision_artifact_hash")
        ),
        "decision_policy_hash": _optional_hash(trade_plan_artifact.get("decision_policy_hash")),
        "trade_plan_payload_hash": _optional_hash(
            trade_plan_artifact.get("trade_plan_payload_hash")
        ),
        "trade_plan_policy_hash": _optional_hash(trade_plan_artifact.get("policy_hash")),
        "risk_policy_version": policy_version,
        "params": params_payload,
        "items": items_payload,
        "counts": counts_payload,
        "notional_summary": notional_payload,
        "quality_flags": flags_payload,
        "quality_warnings": warnings_payload,
    }
    payload["policy_hash"] = canonical_hash(
        {
            "risk_policy_version": policy_version,
            "params": params_payload,
        }
    )
    payload["items_hash"] = canonical_hash(items_payload)
    payload_without_self = dict(payload)
    payload["risk_payload_hash"] = canonical_hash(payload_without_self)
    return payload


def _normalize_mapping(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): value for key, value in raw.items()}


def _normalize_counts(raw: Mapping[str, int]) -> dict[str, int]:
    return {
        "n_total": _coerce_int(raw.get("n_total"), default=0),
        "n_allow": _coerce_int(raw.get("n_allow"), default=0),
        "n_block": _coerce_int(raw.get("n_block"), default=0),
        "n_downsize": _coerce_int(raw.get("n_downsize"), default=0),
    }


def _normalize_notional_summary(raw: Mapping[str, float]) -> dict[str, float]:
    return {
        "requested_total_usd": _round_money(
            _coerce_float(raw.get("requested_total_usd"), default=0.0)
        ),
        "approved_total_usd": _round_money(
            _coerce_float(raw.get("approved_total_usd"), default=0.0)
        ),
        "blocked_total_usd": _round_money(_coerce_float(raw.get("blocked_total_usd"), default=0.0)),
    }


def _normalize_flags(flags: Sequence[str]) -> list[str]:
    return sorted({value.strip() for value in flags if value and value.strip()})


def _normalize_warnings(warnings: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str], dict[str, str]] = {}
    for warning in warnings:
        code = _optional_text(warning.get("code")) or "unknown_warning"
        message = _optional_text(warning.get("message")) or ""
        deduped[(code, message)] = {
            "code": code,
            "message": message,
        }
    keys = sorted(deduped.keys(), key=lambda item: item)
    return [deduped[key] for key in keys]


def _resolve_input_hash(artifact: Mapping[str, Any], *, primary_field: str) -> str:
    value = _optional_hash(artifact.get(primary_field))
    if value is not None:
        return value
    return canonical_hash(artifact)


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


def _coerce_int(raw: Any, *, default: int) -> int:
    if raw is None or isinstance(raw, bool):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _coerce_float(raw: Any, *, default: float) -> float:
    if raw is None or isinstance(raw, bool):
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _round_money(value: float) -> float:
    return round(float(value), 2)
