from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.portfolio.canonical import canonical_hash

PORTFOLIO_ARTIFACT_SCHEMA_VERSION = "portfolio_artifact.v1"
PORTFOLIO_POLICY_VERSION = "portfolio_accounting.v1"

_CARRY_THROUGH_HASH_FIELDS = (
    "forecast_payload_hash",
    "dataset_hash",
    "model_hash",
    "calibration_hash",
    "uncertainty_hash",
    "decision_payload_hash",
    "decision_policy_hash",
    "trade_plan_payload_hash",
)


def build_portfolio_artifact(
    *,
    run_context: RunContext,
    execution_artifacts: Sequence[Mapping[str, Any]],
    params: Mapping[str, Any],
    ledger_entries: Sequence[Mapping[str, Any]],
    positions: Sequence[Mapping[str, Any]],
    valuation: Mapping[str, Any],
    quality_flags: Sequence[str] = (),
    quality_warnings: Sequence[Mapping[str, Any]] = (),
    policy_version: str = PORTFOLIO_POLICY_VERSION,
) -> dict[str, Any]:
    params_payload = _normalize_mapping(params)
    ledger_payload = [_normalize_mapping(item) for item in ledger_entries]
    positions_payload = [_normalize_mapping(item) for item in positions]
    valuation_payload = _normalize_mapping(valuation)

    input_execution_hashes = sorted(
        _resolve_input_execution_hash(artifact) for artifact in execution_artifacts
    )
    carry_through_warnings: list[dict[str, str]] = []
    carry_through_hashes: dict[str, str | None] = {}
    for field_name in _CARRY_THROUGH_HASH_FIELDS:
        value, warnings = _resolve_single_hash_field(execution_artifacts, field_name)
        carry_through_hashes[field_name] = value
        carry_through_warnings.extend(warnings)

    merged_flags = _normalize_quality_flags(quality_flags)
    merged_warnings = _normalize_quality_warnings(
        [*quality_warnings, *carry_through_warnings],
    )
    payload: dict[str, Any] = {
        "artifact_schema_version": PORTFOLIO_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_context.run_id,
        "generated_at_utc": run_context.started_at,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "input_execution_run_ids": _collect_text_values(execution_artifacts, "run_id"),
        "input_execution_hashes": input_execution_hashes,
        "input_trade_plan_run_ids": _collect_text_values(
            execution_artifacts, "input_trade_plan_run_id"
        ),
        "input_decision_run_ids": _collect_text_values(
            execution_artifacts, "input_decision_run_id"
        ),
        "input_forecast_run_ids": _collect_text_values(
            execution_artifacts, "input_forecast_run_id"
        ),
        "portfolio_policy_version": policy_version,
        "params": params_payload,
        "quality_flags": merged_flags,
        "quality_warnings": merged_warnings,
        "ledger_entries": ledger_payload,
        "positions": positions_payload,
        "valuation": valuation_payload,
        "counts": {
            "n_execution_inputs": len(execution_artifacts),
            "n_input_orders": _count_input_orders(execution_artifacts),
            "n_ledger_entries": len(ledger_payload),
            "n_positions": len(positions_payload),
            "n_quality_warnings": len(merged_warnings),
        },
        **carry_through_hashes,
    }
    payload["portfolio_policy_hash"] = canonical_hash(
        {
            "portfolio_policy_version": policy_version,
            "params": params_payload,
        }
    )
    payload["ledger_hash"] = canonical_hash(ledger_payload)
    payload["positions_hash"] = canonical_hash(positions_payload)
    payload["valuation_hash"] = canonical_hash(valuation_payload)
    payload_without_self = dict(payload)
    payload["portfolio_payload_hash"] = canonical_hash(payload_without_self)
    return payload


def _resolve_input_execution_hash(execution_artifact: Mapping[str, Any]) -> str:
    payload_hash = _optional_hash(execution_artifact.get("execution_payload_hash"))
    if payload_hash is not None:
        return payload_hash
    return canonical_hash(execution_artifact)


def _resolve_single_hash_field(
    execution_artifacts: Sequence[Mapping[str, Any]],
    field_name: str,
) -> tuple[str | None, list[dict[str, str]]]:
    values = sorted(
        {
            value
            for value in (
                _optional_hash(artifact.get(field_name)) for artifact in execution_artifacts
            )
            if value is not None
        }
    )
    if not values:
        return None, []
    if len(values) == 1:
        return values[0], []
    return None, [
        {
            "code": "mixed_hash_values",
            "field": field_name,
            "message": f"Input execution artifacts contain mixed hash values for {field_name}.",
        }
    ]


def _collect_text_values(
    execution_artifacts: Sequence[Mapping[str, Any]],
    field_name: str,
) -> list[str]:
    values = sorted(
        {
            value
            for value in (
                _optional_text(artifact.get(field_name)) for artifact in execution_artifacts
            )
            if value is not None
        }
    )
    return values


def _count_input_orders(execution_artifacts: Sequence[Mapping[str, Any]]) -> int:
    count = 0
    for artifact in execution_artifacts:
        raw_orders = artifact.get("orders")
        if isinstance(raw_orders, Sequence) and not isinstance(raw_orders, (str, bytes, bytearray)):
            count += len(raw_orders)
    return count


def _normalize_quality_flags(flags: Sequence[str]) -> list[str]:
    normalized = sorted({value.strip() for value in flags if value and value.strip()})
    return normalized


def _normalize_quality_warnings(
    warnings: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str, str, str], dict[str, str]] = {}
    for warning in warnings:
        code = _optional_text(warning.get("code")) or "unknown_warning"
        message = _optional_text(warning.get("message"))
        field = _optional_text(warning.get("field"))
        token_id = _optional_text(warning.get("token_id"))
        client_order_id = _optional_text(warning.get("client_order_id"))
        key = (
            code,
            message or "",
            field or "",
            token_id or "",
            client_order_id or "",
        )
        payload: dict[str, str] = {"code": code}
        if message is not None:
            payload["message"] = message
        if field is not None:
            payload["field"] = field
        if token_id is not None:
            payload["token_id"] = token_id
        if client_order_id is not None:
            payload["client_order_id"] = client_order_id
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
