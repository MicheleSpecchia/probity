from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.performance.canonical import canonical_hash

PERFORMANCE_REPORT_ARTIFACT_SCHEMA_VERSION = "performance_report_artifact.v1"
PERFORMANCE_POLICY_VERSION = "performance_report_policy.v1"


def build_performance_report_artifact(
    *,
    run_context: RunContext,
    params: Mapping[str, Any],
    inputs: Sequence[Mapping[str, Any]],
    per_run_metrics: Sequence[Mapping[str, Any]],
    aggregate_metrics: Mapping[str, Any],
    quality_flags: Sequence[str],
    quality_warnings: Sequence[Mapping[str, Any]],
    policy_version: str = PERFORMANCE_POLICY_VERSION,
) -> dict[str, Any]:
    params_payload = _normalize_mapping(params)
    inputs_payload = _normalize_inputs(inputs)
    per_run_payload = _normalize_per_run_metrics(per_run_metrics)
    aggregate_payload = _normalize_mapping(aggregate_metrics)
    flags_payload = _normalize_flags(quality_flags)
    warnings_payload = _normalize_warnings(quality_warnings)

    payload: dict[str, Any] = {
        "artifact_schema_version": PERFORMANCE_REPORT_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_context.run_id,
        "generated_at_utc": run_context.started_at,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "performance_policy_version": policy_version,
        "params": params_payload,
        "inputs": inputs_payload,
        "per_run_metrics": per_run_payload,
        "aggregate_metrics": aggregate_payload,
        "quality_flags": flags_payload,
        "quality_warnings": warnings_payload,
    }
    payload["performance_policy_hash"] = canonical_hash(
        {
            "performance_policy_version": policy_version,
            "params": params_payload,
        }
    )
    payload["performance_inputs_hash"] = canonical_hash(inputs_payload)
    payload_without_self = dict(payload)
    payload["performance_payload_hash"] = canonical_hash(payload_without_self)
    return payload


def _normalize_inputs(inputs: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for item in inputs:
        run_id = _require_text(item.get("portfolio_run_id"), "inputs.portfolio_run_id")
        payload_hash = _require_hash(
            item.get("portfolio_payload_hash"),
            "inputs.portfolio_payload_hash",
        )
        normalized.append(
            {
                "portfolio_run_id": run_id,
                "portfolio_payload_hash": payload_hash,
            }
        )
    normalized.sort(key=lambda entry: (entry["portfolio_run_id"], entry["portfolio_payload_hash"]))
    return normalized


def _normalize_per_run_metrics(
    per_run_metrics: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    normalized = [_normalize_mapping(item) for item in per_run_metrics]
    normalized.sort(
        key=lambda item: (
            _optional_text(item.get("portfolio_run_id")) or "",
            _optional_text(item.get("portfolio_payload_hash")) or "",
        )
    )
    return normalized


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


def _normalize_mapping(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): value for key, value in raw.items()}


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _require_text(raw: Any, path: str) -> str:
    value = _optional_text(raw)
    if value is None:
        raise ValueError(f"Missing required text at {path}")
    return value


def _require_hash(raw: Any, path: str) -> str:
    value = _require_text(raw, path)
    if len(value) != 64 or not all(char in "0123456789abcdef" for char in value):
        raise ValueError(f"Expected lowercase sha256 hash at {path}, got {value!r}")
    return value
