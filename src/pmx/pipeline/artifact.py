from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.pipeline.canonical import canonical_hash

PIPELINE_RUN_ARTIFACT_SCHEMA_VERSION = "pipeline_run_artifact.v1"
PIPELINE_POLICY_VERSION = "pipeline_stub.v1"


def build_pipeline_run_artifact(
    *,
    run_context: RunContext,
    pipeline_params: Mapping[str, Any],
    forecast_input: Mapping[str, Any],
    decision_artifact: Mapping[str, Any],
    trade_plan_artifact: Mapping[str, Any],
    execution_artifact: Mapping[str, Any],
    portfolio_artifact: Mapping[str, Any],
    decision_artifact_path: str,
    trade_plan_artifact_path: str,
    execution_artifact_path: str,
    portfolio_artifact_path: str,
    quality_flags: Sequence[str],
    quality_warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    normalized_params = _normalize_mapping(pipeline_params)
    normalized_forecast_input = _normalize_mapping(forecast_input)
    outputs_payload = {
        "decision": _stage_payload(
            decision_artifact,
            artifact_path=decision_artifact_path,
            payload_hash_field="decision_payload_hash",
            policy_hash_field="policy_hash",
            extra_hashes=["decision_items_hash"],
        ),
        "trade_plan": _stage_payload(
            trade_plan_artifact,
            artifact_path=trade_plan_artifact_path,
            payload_hash_field="trade_plan_payload_hash",
            policy_hash_field="policy_hash",
            extra_hashes=["orders_hash"],
        ),
        "execution": _stage_payload(
            execution_artifact,
            artifact_path=execution_artifact_path,
            payload_hash_field="execution_payload_hash",
            policy_hash_field="execution_policy_hash",
            extra_hashes=["orders_hash"],
        ),
        "portfolio": _stage_payload(
            portfolio_artifact,
            artifact_path=portfolio_artifact_path,
            payload_hash_field="portfolio_payload_hash",
            policy_hash_field="portfolio_policy_hash",
            extra_hashes=["ledger_hash", "positions_hash", "valuation_hash"],
        ),
    }
    kpis_payload = _kpis(
        decision_artifact=decision_artifact,
        trade_plan_artifact=trade_plan_artifact,
        execution_artifact=execution_artifact,
        portfolio_artifact=portfolio_artifact,
    )
    normalized_flags = _normalize_flags(quality_flags)
    normalized_warnings = _normalize_warnings(quality_warnings)
    outputs_hash_list = [
        outputs_payload["decision"]["payload_hash"],
        outputs_payload["trade_plan"]["payload_hash"],
        outputs_payload["execution"]["payload_hash"],
        outputs_payload["portfolio"]["payload_hash"],
    ]

    payload: dict[str, Any] = {
        "artifact_schema_version": PIPELINE_RUN_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_context.run_id,
        "generated_at_utc": run_context.started_at,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "pipeline_policy_version": PIPELINE_POLICY_VERSION,
        "inputs": normalized_forecast_input,
        "outputs": outputs_payload,
        "kpis": kpis_payload,
        "quality_flags": normalized_flags,
        "quality_warnings": normalized_warnings,
        "carry_through_hashes": _carry_through_hashes(portfolio_artifact),
    }
    payload["pipeline_policy_hash"] = canonical_hash(
        {
            "pipeline_policy_version": PIPELINE_POLICY_VERSION,
            "params": normalized_params,
        }
    )
    payload["pipeline_outputs_hash"] = canonical_hash(outputs_hash_list)
    payload_without_self = dict(payload)
    payload["pipeline_payload_hash"] = canonical_hash(payload_without_self)
    return payload


def _stage_payload(
    artifact: Mapping[str, Any],
    *,
    artifact_path: str,
    payload_hash_field: str,
    policy_hash_field: str,
    extra_hashes: Sequence[str],
) -> dict[str, Any]:
    payload_hash = _require_hash(artifact.get(payload_hash_field), payload_hash_field)
    out: dict[str, Any] = {
        "run_id": _optional_text(artifact.get("run_id")),
        "artifact_path": artifact_path,
        "payload_hash": payload_hash,
        "policy_hash": _optional_hash(artifact.get(policy_hash_field)),
    }
    for field_name in extra_hashes:
        out[field_name] = _optional_hash(artifact.get(field_name))
    return out


def _kpis(
    *,
    decision_artifact: Mapping[str, Any],
    trade_plan_artifact: Mapping[str, Any],
    execution_artifact: Mapping[str, Any],
    portfolio_artifact: Mapping[str, Any],
) -> dict[str, Any]:
    n_decisions = _sequence_len(decision_artifact.get("items"))
    n_orders = _sequence_len(trade_plan_artifact.get("orders"))
    planned_notional = _sum_notional(trade_plan_artifact.get("orders"), status_field=None)
    n_rejected = _coerce_int(
        _mapping_value(_mapping_value(execution_artifact, "counts"), "n_rejected"),
        default=0,
    )
    executed_notional = _sum_notional(
        execution_artifact.get("orders"),
        status_field="status",
    )
    n_positions = _sequence_len(portfolio_artifact.get("positions"))
    unrealized = _coerce_float(
        _mapping_value(
            _mapping_value(_mapping_value(portfolio_artifact, "valuation"), "summary"),
            "total_unrealized_pnl_usd",
        ),
        default=0.0,
    )
    return {
        "n_decisions": n_decisions,
        "n_orders": n_orders,
        "n_rejected": n_rejected,
        "n_positions": n_positions,
        "total_notional_usd": {
            "planned": _round_money(planned_notional),
            "executed": _round_money(executed_notional),
        },
        "unrealized_pnl_usd": _round_money(unrealized),
    }


def _sum_notional(raw_orders: Any, *, status_field: str | None) -> float:
    if not isinstance(raw_orders, Sequence) or isinstance(raw_orders, (str, bytes, bytearray)):
        return 0.0
    total = 0.0
    for raw_order in raw_orders:
        if not isinstance(raw_order, Mapping):
            continue
        if status_field is not None:
            status = _optional_text(raw_order.get(status_field))
            if status != "SIMULATED_SUBMITTED":
                continue
        total += _coerce_float(raw_order.get("notional_usd"), default=0.0)
    return total


def _carry_through_hashes(portfolio_artifact: Mapping[str, Any]) -> dict[str, str | None]:
    field_names = (
        "forecast_payload_hash",
        "dataset_hash",
        "model_hash",
        "calibration_hash",
        "uncertainty_hash",
        "decision_payload_hash",
        "decision_policy_hash",
        "trade_plan_payload_hash",
    )
    out: dict[str, str | None] = {}
    for field_name in field_names:
        out[field_name] = _optional_hash(portfolio_artifact.get(field_name))
    return out


def _normalize_flags(flags: Sequence[str]) -> list[str]:
    return sorted({value.strip() for value in flags if value and value.strip()})


def _normalize_warnings(warnings: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str], dict[str, str]] = {}
    for warning in warnings:
        code = _optional_text(warning.get("code")) or "unknown_warning"
        message = _optional_text(warning.get("message"))
        source = _optional_text(warning.get("source"))
        payload: dict[str, str] = {"code": code}
        if message is not None:
            payload["message"] = message
        if source is not None:
            payload["source"] = source
        key = (code, message or "", source or "")
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


def _require_hash(raw: Any, path: str) -> str:
    value = _optional_hash(raw)
    if value is None:
        raise ValueError(f"Missing required hash at {path}")
    return value


def _mapping_value(raw: Any, key: str) -> Any:
    if isinstance(raw, Mapping):
        return raw.get(key)
    return None


def _sequence_len(raw: Any) -> int:
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        return len(raw)
    return 0


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
