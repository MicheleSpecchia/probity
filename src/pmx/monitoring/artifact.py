from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.monitoring.canonical import canonical_hash

MONITORING_REPORT_ARTIFACT_SCHEMA_VERSION = "monitoring_report_artifact.v1"
MONITORING_POLICY_VERSION = "monitoring_policy.v1"


def build_monitoring_report_artifact(
    *,
    run_context: RunContext,
    pipeline_artifact: Mapping[str, Any],
    params: Mapping[str, Any],
    health_status: str,
    health_summary: Mapping[str, Any],
    quality_flags: Sequence[str],
    quality_warnings: Sequence[Mapping[str, Any]],
    forecast_artifact: Mapping[str, Any] | None = None,
    performance_artifact: Mapping[str, Any] | None = None,
    risk_artifact: Mapping[str, Any] | None = None,
    policy_version: str = MONITORING_POLICY_VERSION,
) -> dict[str, Any]:
    params_payload = _normalize_mapping(params)
    summary_payload = _normalize_mapping(health_summary)
    flags_payload = _normalize_flags(quality_flags)
    warnings_payload = _normalize_warnings(quality_warnings)

    payload: dict[str, Any] = {
        "artifact_schema_version": MONITORING_REPORT_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_context.run_id,
        "generated_at_utc": run_context.started_at,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "monitoring_policy_version": policy_version,
        "params": params_payload,
        "health_status": health_status,
        "health_summary": summary_payload,
        "quality_flags": flags_payload,
        "quality_warnings": warnings_payload,
        "input_pipeline_run_id": _optional_text(pipeline_artifact.get("run_id")),
        "input_pipeline_artifact_hash": _resolve_input_hash(
            pipeline_artifact,
            primary_field="pipeline_payload_hash",
        ),
        "input_forecast_run_id": (
            None if forecast_artifact is None else _optional_text(forecast_artifact.get("run_id"))
        ),
        "input_forecast_artifact_hash": (
            None
            if forecast_artifact is None
            else _optional_hash(
                _resolve_input_hash(forecast_artifact, primary_field="forecast_payload_hash")
            )
        ),
        "input_performance_run_id": (
            None
            if performance_artifact is None
            else _optional_text(performance_artifact.get("run_id"))
        ),
        "input_performance_artifact_hash": (
            None
            if performance_artifact is None
            else _optional_hash(
                _resolve_input_hash(
                    performance_artifact,
                    primary_field="performance_payload_hash",
                )
            )
        ),
        "input_risk_run_id": None
        if risk_artifact is None
        else _optional_text(risk_artifact.get("run_id")),
        "input_risk_artifact_hash": (
            None
            if risk_artifact is None
            else _optional_hash(
                _resolve_input_hash(risk_artifact, primary_field="risk_payload_hash")
            )
        ),
    }
    payload["monitoring_policy_hash"] = canonical_hash(
        {
            "monitoring_policy_version": policy_version,
            "params": params_payload,
        }
    )
    payload["monitoring_inputs_hash"] = canonical_hash(
        _input_hashes(
            pipeline_artifact=pipeline_artifact,
            forecast_artifact=forecast_artifact,
            performance_artifact=performance_artifact,
            risk_artifact=risk_artifact,
        )
    )
    payload_without_self = dict(payload)
    payload["monitoring_payload_hash"] = canonical_hash(payload_without_self)
    return payload


def _input_hashes(
    *,
    pipeline_artifact: Mapping[str, Any],
    forecast_artifact: Mapping[str, Any] | None,
    performance_artifact: Mapping[str, Any] | None,
    risk_artifact: Mapping[str, Any] | None,
) -> list[str]:
    hashes: list[str] = []
    hashes.append(_resolve_input_hash(pipeline_artifact, primary_field="pipeline_payload_hash"))
    if forecast_artifact is not None:
        hashes.append(_resolve_input_hash(forecast_artifact, primary_field="forecast_payload_hash"))
    if performance_artifact is not None:
        hashes.append(
            _resolve_input_hash(performance_artifact, primary_field="performance_payload_hash")
        )
    if risk_artifact is not None:
        hashes.append(_resolve_input_hash(risk_artifact, primary_field="risk_payload_hash"))
    hashes.sort()
    return hashes


def _resolve_input_hash(artifact: Mapping[str, Any], *, primary_field: str) -> str:
    value = _optional_hash(artifact.get(primary_field))
    if value is not None:
        return value
    return canonical_hash(artifact)


def _normalize_flags(flags: Sequence[str]) -> list[str]:
    return sorted({value.strip() for value in flags if value and value.strip()})


def _normalize_warnings(warnings: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str], dict[str, str]] = {}
    for warning in warnings:
        code = _optional_text(warning.get("code")) or "unknown_warning"
        message = _optional_text(warning.get("message")) or _optional_text(warning.get("detail"))
        source = _optional_text(warning.get("source")) or "monitoring"
        key = (code, message or "", source)
        payload: dict[str, str] = {"code": code, "source": source}
        if message is not None:
            payload["message"] = message
        deduped[key] = payload
    keys = sorted(deduped.keys(), key=lambda item: item)
    return [deduped[key] for key in keys]


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
