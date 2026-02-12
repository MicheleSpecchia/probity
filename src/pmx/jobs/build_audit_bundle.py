from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.audit_bundle.artifact import (
    AUDIT_BUNDLE_POLICY_VERSION,
    build_audit_bundle_artifact,
    stage_event_from_artifact,
)
from pmx.audit_bundle.canonical import canonical_hash
from pmx.audit_bundle.validate_artifact import validate_audit_bundle_artifact
from pmx.decisions.validate_artifact import validate_decision_artifact
from pmx.execution.validate_artifact import validate_execution_artifact
from pmx.forecast.validate_artifact import validate_forecast_artifact
from pmx.performance.validate_artifact import validate_performance_report_artifact
from pmx.pipeline.validate_artifact import validate_pipeline_run_artifact
from pmx.portfolio.validate_artifact import validate_portfolio_artifact
from pmx.risk.validate_artifact import validate_risk_artifact
from pmx.trade_plan.validate_artifact import validate_trade_plan_artifact

JOB_NAME = "build_audit_bundle"


@dataclass(frozen=True, slots=True)
class BuildAuditBundleConfig:
    artifacts_root: str

    def as_hash_dict(self) -> dict[str, str]:
        return {"artifacts_root": self.artifacts_root}


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_build_audit_bundle_config(artifacts_root=args.artifacts_root)
    run_build_audit_bundle(
        pipeline_artifact_path=Path(args.pipeline_artifact),
        config=config,
        forecast_artifact_path=(
            None if args.forecast_artifact is None else Path(args.forecast_artifact)
        ),
        performance_artifact_path=(
            None if args.performance_artifact is None else Path(args.performance_artifact)
        ),
        risk_artifact_path=None if args.risk_artifact is None else Path(args.risk_artifact),
        nonce=args.nonce,
    )
    return 0


def run_build_audit_bundle(
    *,
    pipeline_artifact_path: Path,
    config: BuildAuditBundleConfig,
    forecast_artifact_path: Path | None = None,
    performance_artifact_path: Path | None = None,
    risk_artifact_path: Path | None = None,
    nonce: str | None = None,
) -> dict[str, Any]:
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    pipeline_artifact = _load_validated_artifact(
        pipeline_artifact_path,
        label="Pipeline artifact",
        validator=validate_pipeline_run_artifact,
    )

    decision_artifact = _load_required_stage_artifact(
        pipeline_artifact,
        stage_name="decision",
        validator=validate_decision_artifact,
    )
    trade_plan_artifact = _load_required_stage_artifact(
        pipeline_artifact,
        stage_name="trade_plan",
        validator=validate_trade_plan_artifact,
    )
    execution_artifact = _load_required_stage_artifact(
        pipeline_artifact,
        stage_name="execution",
        validator=validate_execution_artifact,
    )
    portfolio_artifact = _load_required_stage_artifact(
        pipeline_artifact,
        stage_name="portfolio",
        validator=validate_portfolio_artifact,
    )

    warnings: list[dict[str, str]] = []
    flags: set[str] = set()

    resolved_forecast_path = forecast_artifact_path or _pipeline_forecast_artifact_path(
        pipeline_artifact
    )
    forecast_artifact = _load_optional_artifact(
        resolved_forecast_path,
        label="Forecast artifact",
        validator=validate_forecast_artifact,
        missing_flag="missing_forecast_artifact",
        flags=flags,
        warnings=warnings,
    )
    performance_artifact = _load_optional_artifact(
        performance_artifact_path,
        label="Performance artifact",
        validator=validate_performance_report_artifact,
        missing_flag=None,
        flags=flags,
        warnings=warnings,
    )
    risk_artifact = _load_optional_artifact(
        risk_artifact_path,
        label="Risk artifact",
        validator=validate_risk_artifact,
        missing_flag=None,
        flags=flags,
        warnings=warnings,
    )

    stage_events: list[dict[str, Any]] = [
        stage_event_from_artifact(
            stage="pipeline",
            artifact=pipeline_artifact,
            artifact_path=str(pipeline_artifact_path),
            payload_hash_field="pipeline_payload_hash",
            policy_hash_field="pipeline_policy_hash",
        ),
        stage_event_from_artifact(
            stage="decision",
            artifact=decision_artifact,
            artifact_path=_pipeline_stage_artifact_path(pipeline_artifact, "decision"),
            payload_hash_field="decision_payload_hash",
            policy_hash_field="policy_hash",
        ),
        stage_event_from_artifact(
            stage="trade_plan",
            artifact=trade_plan_artifact,
            artifact_path=_pipeline_stage_artifact_path(pipeline_artifact, "trade_plan"),
            payload_hash_field="trade_plan_payload_hash",
            policy_hash_field="policy_hash",
        ),
        stage_event_from_artifact(
            stage="execution",
            artifact=execution_artifact,
            artifact_path=_pipeline_stage_artifact_path(pipeline_artifact, "execution"),
            payload_hash_field="execution_payload_hash",
            policy_hash_field="execution_policy_hash",
        ),
        stage_event_from_artifact(
            stage="portfolio",
            artifact=portfolio_artifact,
            artifact_path=_pipeline_stage_artifact_path(pipeline_artifact, "portfolio"),
            payload_hash_field="portfolio_payload_hash",
            policy_hash_field="portfolio_policy_hash",
        ),
    ]
    if forecast_artifact is not None and resolved_forecast_path is not None:
        stage_events.append(
            stage_event_from_artifact(
                stage="forecast",
                artifact=forecast_artifact,
                artifact_path=str(resolved_forecast_path),
                payload_hash_field="forecast_payload_hash",
                policy_hash_field=None,
            )
        )
    if performance_artifact is not None and performance_artifact_path is not None:
        stage_events.append(
            stage_event_from_artifact(
                stage="performance",
                artifact=performance_artifact,
                artifact_path=str(performance_artifact_path),
                payload_hash_field="performance_payload_hash",
                policy_hash_field="performance_policy_hash",
            )
        )
    if risk_artifact is not None and risk_artifact_path is not None:
        stage_events.append(
            stage_event_from_artifact(
                stage="risk",
                artifact=risk_artifact,
                artifact_path=str(risk_artifact_path),
                payload_hash_field="risk_payload_hash",
                policy_hash_field="policy_hash",
            )
        )

    signal_flags, signal_warnings = _collect_quality_signals(
        artifacts=[
            forecast_artifact,
            decision_artifact,
            trade_plan_artifact,
            execution_artifact,
            portfolio_artifact,
            performance_artifact,
            risk_artifact,
            pipeline_artifact,
        ]
    )
    flags.update(signal_flags)
    warnings.extend(signal_warnings)

    input_hashes = sorted(_require_payload_hash(event) for event in stage_events)
    started_at = _resolve_started_at(
        [
            forecast_artifact,
            decision_artifact,
            trade_plan_artifact,
            execution_artifact,
            portfolio_artifact,
            performance_artifact,
            risk_artifact,
            pipeline_artifact,
        ]
    )
    deterministic_nonce = nonce or canonical_hash(
        {
            "input_hashes": input_hashes,
            "params": config.as_hash_dict(),
            "policy_version": AUDIT_BUNDLE_POLICY_VERSION,
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "input_hashes": input_hashes,
            "params": config.as_hash_dict(),
            "policy_version": AUDIT_BUNDLE_POLICY_VERSION,
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "build_audit_bundle_started",
        run_context,
        pipeline_artifact_path=str(pipeline_artifact_path),
        forecast_artifact_path=(
            None if resolved_forecast_path is None else str(resolved_forecast_path)
        ),
        performance_artifact_path=(
            None if performance_artifact_path is None else str(performance_artifact_path)
        ),
        risk_artifact_path=None if risk_artifact_path is None else str(risk_artifact_path),
    )

    artifact = build_audit_bundle_artifact(
        run_context=run_context,
        params=config.as_hash_dict(),
        inputs=tuple(stage_events),
        timeline=tuple(stage_events),
        quality_flags=tuple(sorted(flags)),
        quality_warnings=_normalize_warning_records(warnings),
        policy_version=AUDIT_BUNDLE_POLICY_VERSION,
    )
    artifact_errors = validate_audit_bundle_artifact(artifact)
    if artifact_errors:
        raise ValueError(f"Invalid audit bundle artifact: {artifact_errors[0]}")

    artifact_path = _write_artifact(
        artifacts_root=config.artifacts_root,
        run_id=run_context.run_id,
        artifact=artifact,
    )
    _log(
        logger,
        logging.INFO,
        "build_audit_bundle_completed",
        run_context,
        artifact_path=str(artifact_path),
        n_inputs=len(artifact["inputs"]),
        n_timeline=len(artifact["timeline"]),
        bundle_hash=str(artifact["bundle_hash"]),
        audit_bundle_payload_hash=str(artifact["audit_bundle_payload_hash"]),
    )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def load_build_audit_bundle_config(*, artifacts_root: str | None) -> BuildAuditBundleConfig:
    resolved_root = artifacts_root or os.getenv("AUDIT_BUNDLE_ARTIFACTS_ROOT") or "artifacts"
    return BuildAuditBundleConfig(artifacts_root=resolved_root)


def _load_required_stage_artifact(
    pipeline_artifact: Mapping[str, Any],
    *,
    stage_name: str,
    validator: Any,
) -> dict[str, Any]:
    path = Path(_pipeline_stage_artifact_path(pipeline_artifact, stage_name))
    return _load_validated_artifact(path, label=f"{stage_name} artifact", validator=validator)


def _load_optional_artifact(
    path: Path | None,
    *,
    label: str,
    validator: Any,
    missing_flag: str | None,
    flags: set[str],
    warnings: list[dict[str, str]],
) -> dict[str, Any] | None:
    if path is None:
        return None
    if not path.exists():
        if missing_flag is None:
            raise FileNotFoundError(f"{label} not found: {path}")
        flags.add(missing_flag)
        warnings.append(
            {
                "code": missing_flag,
                "source": "audit_bundle",
                "message": f"{label} path does not exist: {path}",
            }
        )
        return None
    return _load_validated_artifact(path, label=label, validator=validator)


def _load_validated_artifact(path: Path, *, label: str, validator: Any) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"{label} root must be a JSON object: {path}")
    errors = validator(raw)
    if errors:
        raise ValueError(f"Invalid {label}: {errors[0]}")
    return raw


def _pipeline_forecast_artifact_path(pipeline_artifact: Mapping[str, Any]) -> Path | None:
    inputs = pipeline_artifact.get("inputs")
    if not isinstance(inputs, Mapping):
        return None
    value = _optional_text(inputs.get("forecast_artifact_path"))
    if value is None:
        return None
    return Path(value)


def _pipeline_stage_artifact_path(pipeline_artifact: Mapping[str, Any], stage_name: str) -> str:
    outputs = pipeline_artifact.get("outputs")
    if not isinstance(outputs, Mapping):
        raise ValueError("pipeline artifact missing outputs")
    stage = outputs.get(stage_name)
    if not isinstance(stage, Mapping):
        raise ValueError(f"pipeline artifact missing outputs.{stage_name}")
    path = _optional_text(stage.get("artifact_path"))
    if path is None:
        raise ValueError(f"pipeline artifact missing outputs.{stage_name}.artifact_path")
    return path


def _collect_quality_signals(
    *,
    artifacts: Sequence[Mapping[str, Any] | None],
) -> tuple[set[str], list[dict[str, str]]]:
    flags: set[str] = set()
    warnings: list[dict[str, str]] = []
    for artifact in artifacts:
        if artifact is None:
            continue
        source = _optional_text(artifact.get("artifact_schema_version")) or "unknown_artifact"
        raw_flags = artifact.get("quality_flags")
        if isinstance(raw_flags, Sequence) and not isinstance(raw_flags, (str, bytes, bytearray)):
            for raw in raw_flags:
                text = _optional_text(raw)
                if text is not None:
                    flags.add(text)

        raw_warnings = artifact.get("quality_warnings")
        if isinstance(raw_warnings, Sequence) and not isinstance(
            raw_warnings, (str, bytes, bytearray)
        ):
            for raw in raw_warnings:
                if not isinstance(raw, Mapping):
                    continue
                code = _optional_text(raw.get("code")) or "unknown_warning"
                message = _optional_text(raw.get("message")) or _optional_text(raw.get("detail"))
                payload: dict[str, str] = {"code": code, "source": source}
                if message is not None:
                    payload["message"] = message
                warnings.append(payload)
    return flags, warnings


def _normalize_warning_records(records: Sequence[Mapping[str, str]]) -> tuple[dict[str, str], ...]:
    deduped: dict[tuple[str, str, str], dict[str, str]] = {}
    for record in records:
        code = _optional_text(record.get("code")) or "unknown_warning"
        message = _optional_text(record.get("message")) or ""
        source = _optional_text(record.get("source")) or "audit_bundle"
        deduped[(code, message, source)] = {
            "code": code,
            "message": message,
            "source": source,
        }
    keys = sorted(deduped.keys(), key=lambda item: item)
    return tuple(deduped[key] for key in keys)


def _resolve_started_at(
    artifacts: Sequence[Mapping[str, Any] | None],
) -> datetime:
    parsed_values: list[datetime] = []
    for artifact in artifacts:
        if artifact is None:
            continue
        parsed = _parse_optional_datetime(_optional_text(artifact.get("generated_at_utc")))
        if parsed is not None:
            parsed_values.append(parsed)
    if parsed_values:
        return min(parsed_values)
    return datetime(1970, 1, 1, tzinfo=UTC)


def _parse_optional_datetime(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    normalized = raw.strip().replace("Z", "+00:00")
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _require_payload_hash(event: Mapping[str, Any]) -> str:
    value = _optional_text(event.get("payload_hash"))
    if value is None or len(value) != 64 or not all(char in "0123456789abcdef" for char in value):
        raise ValueError("Audit stage event missing payload_hash")
    return value


def _write_artifact(*, artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root) / "audit_bundles"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic audit bundle from pipeline artifact and optional stages."
    )
    parser.add_argument(
        "--pipeline-artifact",
        required=True,
        help="Path to pipeline run artifact JSON.",
    )
    parser.add_argument(
        "--forecast-artifact",
        default=None,
        help="Optional override path for forecast artifact.",
    )
    parser.add_argument(
        "--performance-artifact",
        default=None,
        help="Optional performance artifact path.",
    )
    parser.add_argument(
        "--risk-artifact",
        default=None,
        help="Optional risk artifact path.",
    )
    parser.add_argument(
        "--artifacts-root",
        default="artifacts",
        help="Artifacts root directory. Output is written under <root>/audit_bundles.",
    )
    parser.add_argument(
        "--nonce",
        default=None,
        help="Optional nonce override for deterministic run_id generation.",
    )
    return parser.parse_args(argv)


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _log(
    logger: logging.Logger,
    level: int,
    message: str,
    run_context: RunContext,
    **extra_fields: Any,
) -> None:
    payload: dict[str, Any] = dict(run_context.as_log_context())
    payload["extra_fields"] = extra_fields
    logger.log(level, message, extra=payload)


if __name__ == "__main__":
    raise SystemExit(main())
