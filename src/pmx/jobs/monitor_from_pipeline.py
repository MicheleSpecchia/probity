from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.forecast.validate_artifact import validate_forecast_artifact
from pmx.monitoring.artifact import MONITORING_POLICY_VERSION, build_monitoring_report_artifact
from pmx.monitoring.canonical import canonical_hash
from pmx.monitoring.policy import MonitoringPolicyConfig, evaluate_monitoring_health
from pmx.monitoring.validate_artifact import validate_monitoring_report_artifact
from pmx.performance.validate_artifact import validate_performance_report_artifact
from pmx.pipeline.validate_artifact import validate_pipeline_run_artifact
from pmx.risk.validate_artifact import validate_risk_artifact

JOB_NAME = "monitor_from_pipeline"


@dataclass(frozen=True, slots=True)
class MonitorFromPipelineConfig:
    artifacts_root: str
    fail_on_critical_block: bool
    warn_on_any_quality_signal: bool

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "artifacts_root": self.artifacts_root,
            "fail_on_critical_block": self.fail_on_critical_block,
            "warn_on_any_quality_signal": self.warn_on_any_quality_signal,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_monitor_from_pipeline_config(
        artifacts_root=args.artifacts_root,
        fail_on_critical_block=args.fail_on_critical_block,
        warn_on_any_quality_signal=args.warn_on_any_quality_signal,
    )
    run_monitor_from_pipeline(
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


def run_monitor_from_pipeline(
    *,
    pipeline_artifact_path: Path,
    config: MonitorFromPipelineConfig,
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

    missing_flags: set[str] = set()
    missing_warnings: list[dict[str, str]] = []
    resolved_forecast_path = forecast_artifact_path or _pipeline_forecast_artifact_path(
        pipeline_artifact
    )
    forecast_artifact = _load_optional_artifact(
        resolved_forecast_path,
        label="Forecast artifact",
        validator=validate_forecast_artifact,
        missing_flag="missing_forecast_artifact",
        missing_flags=missing_flags,
        missing_warnings=missing_warnings,
    )
    performance_artifact = _load_optional_artifact(
        performance_artifact_path,
        label="Performance artifact",
        validator=validate_performance_report_artifact,
        missing_flag=None,
        missing_flags=missing_flags,
        missing_warnings=missing_warnings,
    )
    risk_artifact = _load_optional_artifact(
        risk_artifact_path,
        label="Risk artifact",
        validator=validate_risk_artifact,
        missing_flag=None,
        missing_flags=missing_flags,
        missing_warnings=missing_warnings,
    )

    policy_config = MonitoringPolicyConfig(
        fail_on_critical_block=config.fail_on_critical_block,
        warn_on_any_quality_signal=config.warn_on_any_quality_signal,
    )
    input_hashes = [
        _resolve_payload_hash(pipeline_artifact, primary_field="pipeline_payload_hash"),
    ]
    if forecast_artifact is not None:
        input_hashes.append(
            _resolve_payload_hash(forecast_artifact, primary_field="forecast_payload_hash")
        )
    if performance_artifact is not None:
        input_hashes.append(
            _resolve_payload_hash(performance_artifact, primary_field="performance_payload_hash")
        )
    if risk_artifact is not None:
        input_hashes.append(_resolve_payload_hash(risk_artifact, primary_field="risk_payload_hash"))
    input_hashes.sort()

    started_at = _resolve_started_at(pipeline_artifact)
    deterministic_nonce = nonce or canonical_hash(
        {
            "input_hashes": input_hashes,
            "params": config.as_hash_dict(),
            "policy_version": MONITORING_POLICY_VERSION,
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "input_hashes": input_hashes,
            "params": config.as_hash_dict(),
            "policy_version": MONITORING_POLICY_VERSION,
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "monitor_from_pipeline_started",
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

    result = evaluate_monitoring_health(
        pipeline_artifact=pipeline_artifact,
        forecast_artifact=forecast_artifact,
        performance_artifact=performance_artifact,
        risk_artifact=risk_artifact,
        config=policy_config,
    )
    merged_flags = sorted({*result.quality_flags, *missing_flags})
    merged_warnings = _normalize_warning_records([*result.quality_warnings, *missing_warnings])
    artifact = build_monitoring_report_artifact(
        run_context=run_context,
        pipeline_artifact=pipeline_artifact,
        forecast_artifact=forecast_artifact,
        performance_artifact=performance_artifact,
        risk_artifact=risk_artifact,
        params=config.as_hash_dict(),
        health_status=result.health_status,
        health_summary=result.health_summary,
        quality_flags=merged_flags,
        quality_warnings=merged_warnings,
        policy_version=MONITORING_POLICY_VERSION,
    )
    artifact_errors = validate_monitoring_report_artifact(artifact)
    if artifact_errors:
        raise ValueError(f"Invalid monitoring artifact: {artifact_errors[0]}")

    artifact_path = _write_artifact(
        artifacts_root=config.artifacts_root,
        run_id=run_context.run_id,
        artifact=artifact,
    )
    _log(
        logger,
        logging.INFO,
        "monitor_from_pipeline_completed",
        run_context,
        artifact_path=str(artifact_path),
        health_status=str(artifact["health_status"]),
        n_flags=len(artifact["quality_flags"]),
        n_warnings=len(artifact["quality_warnings"]),
        monitoring_policy_hash=str(artifact["monitoring_policy_hash"]),
        monitoring_inputs_hash=str(artifact["monitoring_inputs_hash"]),
        monitoring_payload_hash=str(artifact["monitoring_payload_hash"]),
    )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def load_monitor_from_pipeline_config(
    *,
    artifacts_root: str | None,
    fail_on_critical_block: bool | None,
    warn_on_any_quality_signal: bool | None,
) -> MonitorFromPipelineConfig:
    resolved_root = artifacts_root or os.getenv("MONITORING_ARTIFACTS_ROOT") or "artifacts"
    resolved_fail = (
        fail_on_critical_block
        if fail_on_critical_block is not None
        else _load_bool("MONITOR_FAIL_ON_CRITICAL_BLOCK", True)
    )
    resolved_warn = (
        warn_on_any_quality_signal
        if warn_on_any_quality_signal is not None
        else _load_bool("MONITOR_WARN_ON_ANY_QUALITY_SIGNAL", True)
    )
    return MonitorFromPipelineConfig(
        artifacts_root=resolved_root,
        fail_on_critical_block=resolved_fail,
        warn_on_any_quality_signal=resolved_warn,
    )


def _load_optional_artifact(
    path: Path | None,
    *,
    label: str,
    validator: Any,
    missing_flag: str | None,
    missing_flags: set[str],
    missing_warnings: list[dict[str, str]],
) -> dict[str, Any] | None:
    if path is None:
        return None
    if not path.exists():
        if missing_flag is None:
            raise FileNotFoundError(f"{label} not found: {path}")
        missing_flags.add(missing_flag)
        missing_warnings.append(
            {
                "code": missing_flag,
                "source": "monitoring",
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


def _resolve_payload_hash(artifact: Mapping[str, Any], *, primary_field: str) -> str:
    value = _optional_text(artifact.get(primary_field))
    if value is not None and _is_sha256(value):
        return value
    return canonical_hash(artifact)


def _resolve_started_at(pipeline_artifact: Mapping[str, Any]) -> datetime:
    parsed = _parse_optional_datetime(_optional_text(pipeline_artifact.get("generated_at_utc")))
    if parsed is not None:
        return parsed
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


def _write_artifact(*, artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root) / "monitoring"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _normalize_warning_records(records: list[Mapping[str, str]]) -> tuple[dict[str, str], ...]:
    deduped: dict[tuple[str, str, str], dict[str, str]] = {}
    for record in records:
        code = _optional_text(record.get("code")) or "unknown_warning"
        message = _optional_text(record.get("message")) or ""
        source = _optional_text(record.get("source")) or "monitoring"
        deduped[(code, message, source)] = {
            "code": code,
            "message": message,
            "source": source,
        }
    keys = sorted(deduped.keys(), key=lambda item: item)
    return tuple(deduped[key] for key in keys)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic monitoring report from pipeline artifact."
    )
    parser.add_argument(
        "--pipeline-artifact",
        required=True,
        help="Path to pipeline run artifact JSON file.",
    )
    parser.add_argument(
        "--forecast-artifact",
        default=None,
        help="Optional override path for forecast artifact.",
    )
    parser.add_argument(
        "--performance-artifact",
        default=None,
        help="Optional performance report artifact path.",
    )
    parser.add_argument(
        "--risk-artifact",
        default=None,
        help="Optional risk artifact path.",
    )
    parser.add_argument(
        "--artifacts-root",
        default="artifacts",
        help="Artifacts root directory. Output is written under <root>/monitoring.",
    )
    parser.add_argument(
        "--fail-on-critical-block",
        dest="fail_on_critical_block",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--no-fail-on-critical-block",
        dest="fail_on_critical_block",
        action="store_false",
    )
    parser.add_argument(
        "--warn-on-any-quality-signal",
        dest="warn_on_any_quality_signal",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--no-warn-on-any-quality-signal",
        dest="warn_on_any_quality_signal",
        action="store_false",
    )
    parser.add_argument(
        "--nonce",
        default=None,
        help="Optional nonce override for deterministic run_id generation.",
    )
    return parser.parse_args(argv)


def _load_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean-like value, got {raw!r}")


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


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
