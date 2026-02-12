from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.audit_bundle.canonical import canonical_hash

AUDIT_BUNDLE_ARTIFACT_SCHEMA_VERSION = "audit_bundle_artifact.v1"
AUDIT_BUNDLE_POLICY_VERSION = "audit_bundle.v1"
_STAGE_ORDER = {
    "forecast": 0,
    "decision": 1,
    "trade_plan": 2,
    "execution": 3,
    "portfolio": 4,
    "pipeline": 5,
    "performance": 6,
    "risk": 7,
}


def build_audit_bundle_artifact(
    *,
    run_context: RunContext,
    params: Mapping[str, Any],
    inputs: Sequence[Mapping[str, Any]],
    timeline: Sequence[Mapping[str, Any]],
    quality_flags: Sequence[str],
    quality_warnings: Sequence[Mapping[str, Any]],
    policy_version: str = AUDIT_BUNDLE_POLICY_VERSION,
) -> dict[str, Any]:
    params_payload = _normalize_mapping(params)
    inputs_payload = _normalize_inputs(inputs)
    timeline_payload = _normalize_timeline(timeline)
    flags_payload = _normalize_flags(quality_flags)
    warnings_payload = _normalize_warnings(quality_warnings)

    payload: dict[str, Any] = {
        "artifact_schema_version": AUDIT_BUNDLE_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_context.run_id,
        "generated_at_utc": run_context.started_at,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "audit_bundle_policy_version": policy_version,
        "params": params_payload,
        "inputs": inputs_payload,
        "timeline": timeline_payload,
        "quality_flags": flags_payload,
        "quality_warnings": warnings_payload,
    }
    payload["bundle_hash"] = canonical_hash(
        {"inputs": inputs_payload, "timeline": timeline_payload}
    )
    payload["audit_bundle_policy_hash"] = canonical_hash(
        {
            "audit_bundle_policy_version": policy_version,
            "params": params_payload,
        }
    )
    payload_without_self = dict(payload)
    payload["audit_bundle_payload_hash"] = canonical_hash(payload_without_self)
    return payload


def stage_event_from_artifact(
    *,
    stage: str,
    artifact: Mapping[str, Any],
    artifact_path: str,
    payload_hash_field: str,
    policy_hash_field: str | None = None,
) -> dict[str, Any]:
    payload_hash = _resolve_hash(artifact, payload_hash_field)
    event: dict[str, Any] = {
        "stage": stage,
        "run_id": _optional_text(artifact.get("run_id")),
        "generated_at_utc": _optional_text(artifact.get("generated_at_utc")),
        "code_version": _optional_text(artifact.get("code_version")),
        "config_hash": _optional_hash(artifact.get("config_hash")),
        "artifact_path": artifact_path,
        "payload_hash": payload_hash,
        "policy_hash": None
        if policy_hash_field is None
        else _optional_hash(artifact.get(policy_hash_field)),
    }
    return event


def _normalize_inputs(inputs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [_normalize_mapping(item) for item in inputs]
    normalized.sort(
        key=lambda item: (
            _stage_rank(_optional_text(item.get("stage"))),
            _optional_text(item.get("run_id")) or "",
            _optional_text(item.get("payload_hash")) or "",
        )
    )
    return normalized


def _normalize_timeline(timeline: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [_normalize_mapping(item) for item in timeline]
    normalized.sort(
        key=lambda item: (
            _stage_rank(_optional_text(item.get("stage"))),
            _optional_text(item.get("generated_at_utc")) or "",
            _optional_text(item.get("run_id")) or "",
            _optional_text(item.get("payload_hash")) or "",
        )
    )
    return normalized


def _normalize_flags(flags: Sequence[str]) -> list[str]:
    return sorted({value.strip() for value in flags if value and value.strip()})


def _normalize_warnings(warnings: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str], dict[str, str]] = {}
    for warning in warnings:
        code = _optional_text(warning.get("code")) or "unknown_warning"
        message = _optional_text(warning.get("message")) or ""
        source = _optional_text(warning.get("source")) or "audit_bundle"
        deduped[(code, message, source)] = {
            "code": code,
            "message": message,
            "source": source,
        }
    keys = sorted(deduped.keys(), key=lambda item: item)
    return [deduped[key] for key in keys]


def _stage_rank(stage: str | None) -> int:
    if stage is None:
        return 999
    return _STAGE_ORDER.get(stage, 998)


def _resolve_hash(artifact: Mapping[str, Any], field_name: str) -> str:
    value = _optional_hash(artifact.get(field_name))
    if value is not None:
        return value
    return canonical_hash(artifact)


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
