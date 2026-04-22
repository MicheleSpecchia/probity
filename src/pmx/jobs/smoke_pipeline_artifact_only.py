from __future__ import annotations

import argparse
import hashlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.audit_bundle.validate_artifact import validate_audit_bundle_artifact
from pmx.decisions.policy import RobustMode
from pmx.decisions.validate_artifact import validate_decision_artifact
from pmx.execution.policy import ExecutionMode
from pmx.execution.validate_artifact import validate_execution_artifact
from pmx.forecast.validate_artifact import validate_forecast_artifact
from pmx.jobs.build_audit_bundle import (
    load_build_audit_bundle_config,
    run_build_audit_bundle,
)
from pmx.jobs.decide_from_forecast import (
    load_decide_from_forecast_config,
    run_decide_from_forecast,
)
from pmx.jobs.execute_trade_plan_stub import (
    load_execute_trade_plan_stub_config,
    run_execute_trade_plan_stub,
)
from pmx.jobs.monitor_from_pipeline import (
    load_monitor_from_pipeline_config,
    run_monitor_from_pipeline,
)
from pmx.jobs.performance_from_portfolio import (
    load_performance_from_portfolio_config,
    run_performance_from_portfolio,
)
from pmx.jobs.portfolio_from_execution import (
    load_portfolio_from_execution_config,
    run_portfolio_from_execution,
)
from pmx.jobs.risk_from_trade_plan import (
    load_risk_from_trade_plan_config,
    run_risk_from_trade_plan,
)
from pmx.jobs.trade_plan_from_decision import (
    load_trade_plan_from_decision_config,
    run_trade_plan_from_decision,
)
from pmx.monitoring.validate_artifact import validate_monitoring_report_artifact
from pmx.performance.validate_artifact import validate_performance_report_artifact
from pmx.pipeline.artifact import build_pipeline_run_artifact
from pmx.pipeline.canonical import canonical_hash
from pmx.pipeline.validate_artifact import validate_pipeline_run_artifact
from pmx.portfolio.validate_artifact import validate_portfolio_artifact
from pmx.risk.validate_artifact import validate_risk_artifact
from pmx.trade_plan.policy import SizingMode
from pmx.trade_plan.validate_artifact import validate_trade_plan_artifact

JOB_NAME = "smoke_pipeline_artifact_only"
SMOKE_SUMMARY_SCHEMA_VERSION = "smoke_pipeline_artifact_only.v1"
SMOKE_POLICY_VERSION = "smoke_pipeline_artifact_only.v1"
DEFAULT_FORECAST_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "forecast"
    / "forecast_artifact_sample.json"
)
STEP_ORDER = (
    "decision",
    "trade_plan",
    "execution",
    "portfolio",
    "pipeline",
    "performance",
    "risk",
    "audit_bundle",
    "monitoring",
)


@dataclass(frozen=True, slots=True)
class SmokePipelineArtifactOnlyConfig:
    artifacts_root: str
    min_edge_bps: float
    robust_mode: RobustMode
    max_items: int
    sizing_mode: SizingMode
    fixed_notional_usd: float
    base_notional_usd: float
    target_edge_bps: float
    min_scale: float
    max_scale: float
    max_orders: int
    max_total_notional_usd: float
    max_notional_per_market_usd: float
    max_notional_per_category_usd: float
    execution_mode: ExecutionMode
    fee_bps: float
    fee_usd: float
    mark_source: str
    reference_prices_json: str | None
    fail_on_critical_block: bool
    warn_on_any_quality_signal: bool

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "artifacts_root": self.artifacts_root,
            "base_notional_usd": self.base_notional_usd,
            "execution_mode": self.execution_mode,
            "fail_on_critical_block": self.fail_on_critical_block,
            "fee_bps": self.fee_bps,
            "fee_usd": self.fee_usd,
            "fixed_notional_usd": self.fixed_notional_usd,
            "mark_source": self.mark_source,
            "max_items": self.max_items,
            "max_notional_per_category_usd": self.max_notional_per_category_usd,
            "max_notional_per_market_usd": self.max_notional_per_market_usd,
            "max_orders": self.max_orders,
            "max_scale": self.max_scale,
            "max_total_notional_usd": self.max_total_notional_usd,
            "min_edge_bps": self.min_edge_bps,
            "min_scale": self.min_scale,
            "reference_prices_json": self.reference_prices_json,
            "robust_mode": self.robust_mode,
            "sizing_mode": self.sizing_mode,
            "target_edge_bps": self.target_edge_bps,
            "warn_on_any_quality_signal": self.warn_on_any_quality_signal,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = SmokePipelineArtifactOnlyConfig(
        artifacts_root=args.artifacts_root,
        min_edge_bps=args.min_edge_bps,
        robust_mode=cast(RobustMode, args.robust_mode),
        max_items=args.max_items,
        sizing_mode=cast(SizingMode, args.sizing_mode),
        fixed_notional_usd=args.fixed_notional_usd,
        base_notional_usd=args.base_notional_usd,
        target_edge_bps=args.target_edge_bps,
        min_scale=args.min_scale,
        max_scale=args.max_scale,
        max_orders=args.max_orders,
        max_total_notional_usd=args.max_total_notional_usd,
        max_notional_per_market_usd=args.max_notional_per_market_usd,
        max_notional_per_category_usd=args.max_notional_per_category_usd,
        execution_mode=cast(ExecutionMode, args.execution_mode),
        fee_bps=args.fee_bps,
        fee_usd=args.fee_usd,
        mark_source=args.mark_source,
        reference_prices_json=args.reference_prices_json,
        fail_on_critical_block=args.fail_on_critical_block,
        warn_on_any_quality_signal=args.warn_on_any_quality_signal,
    )
    summary = run_smoke_pipeline_artifact_only(
        forecast_artifact_path=Path(args.forecast_artifact),
        config=config,
        nonce=args.nonce,
    )
    if args.strict and summary["overall_status"] == "FAIL":
        return 1
    return 0


def run_smoke_pipeline_artifact_only(
    *,
    forecast_artifact_path: Path,
    config: SmokePipelineArtifactOnlyConfig,
    nonce: str | None = "smoke",
) -> dict[str, Any]:
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    forecast_artifact = _load_json_object(forecast_artifact_path, label="Forecast artifact")
    forecast_errors = validate_forecast_artifact(forecast_artifact)
    if forecast_errors:
        raise ValueError(f"Invalid forecast artifact: {forecast_errors[0]}")

    forecast_payload_hash = _resolve_payload_hash(
        forecast_artifact,
        primary_field="forecast_payload_hash",
    )
    forecast_file_hash = _file_sha256(forecast_artifact_path)
    started_at = _resolve_started_at(forecast_artifact)

    deterministic_nonce = nonce or canonical_hash(
        {
            "forecast_artifact_hash": forecast_payload_hash,
            "forecast_artifact_path": str(forecast_artifact_path),
            "params": config.as_hash_dict(),
            "policy_version": SMOKE_POLICY_VERSION,
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "forecast_artifact_hash": forecast_payload_hash,
            "forecast_artifact_path": str(forecast_artifact_path),
            "params": config.as_hash_dict(),
            "policy_version": SMOKE_POLICY_VERSION,
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "smoke_pipeline_artifact_only_started",
        run_context,
        forecast_artifact_path=str(forecast_artifact_path),
        forecast_payload_hash=forecast_payload_hash,
        forecast_file_hash=forecast_file_hash,
    )

    step_outputs: dict[str, dict[str, Any]] = {}
    stage_artifacts: dict[str, dict[str, Any]] = {}
    stage_paths: dict[str, Path] = {}

    for step_name in STEP_ORDER:
        step_outputs[step_name] = _skipped_step_output(
            step=step_name,
            reason="not_executed",
        )

    failed_step: str | None = None

    decision_artifact = _run_step(
        step="decision",
        executor=lambda: run_decide_from_forecast(
            forecast_artifact_path=forecast_artifact_path,
            config=load_decide_from_forecast_config(
                min_edge_bps=config.min_edge_bps,
                robust_mode=config.robust_mode,
                max_items=config.max_items,
                artifacts_root=config.artifacts_root,
            ),
            nonce=f"{run_context.run_id}:decision",
        ),
        validator=validate_decision_artifact,
        payload_hash_field="decision_payload_hash",
        policy_hash_field="policy_hash",
    )
    if decision_artifact.failed:
        failed_step = "decision"
    else:
        stage_artifacts["decision"] = decision_artifact.artifact
        stage_paths["decision"] = decision_artifact.artifact_path
        _ensure_generated_at(
            artifact_path=stage_paths["decision"],
            artifact=stage_artifacts["decision"],
            fallback_timestamp=run_context.started_at,
        )
    step_outputs["decision"] = decision_artifact.output

    if failed_step is None:
        trade_plan_artifact = _run_step(
            step="trade_plan",
            executor=lambda: run_trade_plan_from_decision(
                decision_artifact_path=stage_paths["decision"],
                config=load_trade_plan_from_decision_config(
                    max_orders=config.max_orders,
                    max_total_notional_usd=config.max_total_notional_usd,
                    max_notional_per_market_usd=config.max_notional_per_market_usd,
                    max_notional_per_category_usd=config.max_notional_per_category_usd,
                    sizing_mode=config.sizing_mode,
                    fixed_notional_usd=config.fixed_notional_usd,
                    base_notional_usd=config.base_notional_usd,
                    target_edge_bps=config.target_edge_bps,
                    min_scale=config.min_scale,
                    max_scale=config.max_scale,
                    dry_run=True,
                    artifacts_root=config.artifacts_root,
                ),
                nonce=f"{run_context.run_id}:trade_plan",
            ),
            validator=validate_trade_plan_artifact,
            payload_hash_field="trade_plan_payload_hash",
            policy_hash_field="policy_hash",
        )
        step_outputs["trade_plan"] = trade_plan_artifact.output
        if trade_plan_artifact.failed:
            failed_step = "trade_plan"
        else:
            stage_artifacts["trade_plan"] = trade_plan_artifact.artifact
            stage_paths["trade_plan"] = trade_plan_artifact.artifact_path

    if failed_step is None:
        execution_artifact = _run_step(
            step="execution",
            executor=lambda: run_execute_trade_plan_stub(
                trade_plan_artifact_path=stage_paths["trade_plan"],
                config=load_execute_trade_plan_stub_config(
                    mode=config.execution_mode,
                    max_orders=config.max_orders,
                    simulate_reject_modulo=0,
                    simulate_reject_remainder=0,
                    artifacts_root=config.artifacts_root,
                ),
                nonce=f"{run_context.run_id}:execution",
            ),
            validator=validate_execution_artifact,
            payload_hash_field="execution_payload_hash",
            policy_hash_field="execution_policy_hash",
        )
        step_outputs["execution"] = execution_artifact.output
        if execution_artifact.failed:
            failed_step = "execution"
        else:
            stage_artifacts["execution"] = execution_artifact.artifact
            stage_paths["execution"] = execution_artifact.artifact_path

    if failed_step is None:
        portfolio_artifact = _run_step(
            step="portfolio",
            executor=lambda: run_portfolio_from_execution(
                execution_artifact_paths=[stage_paths["execution"]],
                config=load_portfolio_from_execution_config(
                    artifacts_root=config.artifacts_root,
                    fee_bps=config.fee_bps,
                    fee_usd=config.fee_usd,
                    mark_source=config.mark_source,
                    reference_prices_json=config.reference_prices_json,
                ),
                nonce=f"{run_context.run_id}:portfolio",
            ),
            validator=validate_portfolio_artifact,
            payload_hash_field="portfolio_payload_hash",
            policy_hash_field="portfolio_policy_hash",
        )
        step_outputs["portfolio"] = portfolio_artifact.output
        if portfolio_artifact.failed:
            failed_step = "portfolio"
        else:
            stage_artifacts["portfolio"] = portfolio_artifact.artifact
            stage_paths["portfolio"] = portfolio_artifact.artifact_path

    if failed_step is None:
        pipeline_build = _run_pipeline_step(
            run_context=run_context,
            config=config,
            forecast_artifact=forecast_artifact,
            forecast_artifact_path=forecast_artifact_path,
            decision_artifact=stage_artifacts["decision"],
            trade_plan_artifact=stage_artifacts["trade_plan"],
            execution_artifact=stage_artifacts["execution"],
            portfolio_artifact=stage_artifacts["portfolio"],
            decision_artifact_path=stage_paths["decision"],
            trade_plan_artifact_path=stage_paths["trade_plan"],
            execution_artifact_path=stage_paths["execution"],
            portfolio_artifact_path=stage_paths["portfolio"],
        )
        step_outputs["pipeline"] = pipeline_build.output
        if pipeline_build.failed:
            failed_step = "pipeline"
        else:
            stage_artifacts["pipeline"] = pipeline_build.artifact
            stage_paths["pipeline"] = pipeline_build.artifact_path

    if failed_step is None:
        performance_artifact = _run_step(
            step="performance",
            executor=lambda: run_performance_from_portfolio(
                portfolio_artifact_paths=[stage_paths["portfolio"]],
                config=load_performance_from_portfolio_config(
                    artifacts_root=config.artifacts_root,
                    window_from=None,
                    window_to=None,
                ),
                nonce=f"{run_context.run_id}:performance",
            ),
            validator=validate_performance_report_artifact,
            payload_hash_field="performance_payload_hash",
            policy_hash_field="performance_policy_hash",
        )
        step_outputs["performance"] = performance_artifact.output
        if performance_artifact.failed:
            failed_step = "performance"
        else:
            stage_artifacts["performance"] = performance_artifact.artifact
            stage_paths["performance"] = performance_artifact.artifact_path

    if failed_step is None:
        risk_artifact = _run_step(
            step="risk",
            executor=lambda: run_risk_from_trade_plan(
                trade_plan_artifact_path=stage_paths["trade_plan"],
                config=load_risk_from_trade_plan_config(
                    artifacts_root=config.artifacts_root,
                    max_total_notional_usd=None,
                    max_notional_per_market_usd=None,
                    max_notional_per_category_usd=None,
                    top1_share_cap=None,
                    top3_share_cap=None,
                    performance_top1_cap=None,
                    performance_top3_cap=None,
                    allow_downsize=None,
                    min_notional_usd=None,
                    blocking_quality_flags=None,
                    cooldown_block_flags=None,
                ),
                performance_artifact_path=stage_paths["performance"],
                hooks_json_path=None,
                nonce=f"{run_context.run_id}:risk",
            ),
            validator=validate_risk_artifact,
            payload_hash_field="risk_payload_hash",
            policy_hash_field="policy_hash",
        )
        step_outputs["risk"] = risk_artifact.output
        if risk_artifact.failed:
            failed_step = "risk"
        else:
            stage_artifacts["risk"] = risk_artifact.artifact
            stage_paths["risk"] = risk_artifact.artifact_path

    if failed_step is None:
        audit_bundle_artifact = _run_step(
            step="audit_bundle",
            executor=lambda: run_build_audit_bundle(
                pipeline_artifact_path=stage_paths["pipeline"],
                config=load_build_audit_bundle_config(
                    artifacts_root=config.artifacts_root,
                ),
                forecast_artifact_path=forecast_artifact_path,
                performance_artifact_path=stage_paths["performance"],
                risk_artifact_path=stage_paths["risk"],
                nonce=f"{run_context.run_id}:audit_bundle",
            ),
            validator=validate_audit_bundle_artifact,
            payload_hash_field="audit_bundle_payload_hash",
            policy_hash_field="audit_bundle_policy_hash",
        )
        step_outputs["audit_bundle"] = audit_bundle_artifact.output
        if audit_bundle_artifact.failed:
            failed_step = "audit_bundle"
        else:
            stage_artifacts["audit_bundle"] = audit_bundle_artifact.artifact
            stage_paths["audit_bundle"] = audit_bundle_artifact.artifact_path

    if failed_step is None:
        monitoring_artifact = _run_step(
            step="monitoring",
            executor=lambda: run_monitor_from_pipeline(
                pipeline_artifact_path=stage_paths["pipeline"],
                config=load_monitor_from_pipeline_config(
                    artifacts_root=config.artifacts_root,
                    fail_on_critical_block=config.fail_on_critical_block,
                    warn_on_any_quality_signal=config.warn_on_any_quality_signal,
                ),
                forecast_artifact_path=forecast_artifact_path,
                performance_artifact_path=stage_paths["performance"],
                risk_artifact_path=stage_paths["risk"],
                nonce=f"{run_context.run_id}:monitoring",
            ),
            validator=validate_monitoring_report_artifact,
            payload_hash_field="monitoring_payload_hash",
            policy_hash_field="monitoring_policy_hash",
        )
        step_outputs["monitoring"] = monitoring_artifact.output
        if monitoring_artifact.failed:
            failed_step = "monitoring"
        else:
            stage_artifacts["monitoring"] = monitoring_artifact.artifact
            stage_paths["monitoring"] = monitoring_artifact.artifact_path

    if failed_step is not None:
        seen_failure = False
        for step_name in STEP_ORDER:
            if step_name == failed_step:
                seen_failure = True
                continue
            if seen_failure:
                step_outputs[step_name] = _skipped_step_output(
                    step=step_name,
                    reason=f"dependency_failed:{failed_step}",
                )

    quality_flags, quality_warnings = _collect_quality_signals(
        artifacts=(
            forecast_artifact,
            *(stage_artifacts[step] for step in STEP_ORDER if step in stage_artifacts),
        ),
    )
    overall_status = _overall_status(
        step_outputs=step_outputs,
        monitoring_artifact=stage_artifacts.get("monitoring"),
        n_flags=len(quality_flags),
        n_warnings=len(quality_warnings),
    )

    summary = _build_smoke_summary(
        run_context=run_context,
        config=config,
        forecast_artifact_path=forecast_artifact_path,
        forecast_file_hash=forecast_file_hash,
        forecast_payload_hash=forecast_payload_hash,
        forecast_run_id=_optional_text(forecast_artifact.get("run_id")),
        nonce=deterministic_nonce,
        step_outputs=step_outputs,
        quality_flags=quality_flags,
        quality_warnings=quality_warnings,
        overall_status=overall_status,
    )
    summary_path = _write_summary_artifact(
        artifacts_root=config.artifacts_root,
        run_id=run_context.run_id,
        artifact=summary,
    )
    summary["artifact_path"] = str(summary_path)

    _log(
        logger,
        logging.INFO,
        "smoke_pipeline_artifact_only_completed",
        run_context,
        artifact_path=str(summary_path),
        overall_status=overall_status,
        n_steps_ok=summary["counts"]["n_steps_ok"],
        n_steps_fail=summary["counts"]["n_steps_fail"],
        n_steps_skipped=summary["counts"]["n_steps_skipped"],
        n_quality_flags=summary["counts"]["n_quality_flags"],
        n_quality_warnings=summary["counts"]["n_quality_warnings"],
        smoke_payload_hash=summary["smoke_payload_hash"],
    )
    return summary


@dataclass(frozen=True, slots=True)
class _StepExecutionResult:
    output: dict[str, Any]
    failed: bool
    artifact: dict[str, Any]
    artifact_path: Path


def _run_step(
    *,
    step: str,
    executor: Any,
    validator: Any,
    payload_hash_field: str,
    policy_hash_field: str,
) -> _StepExecutionResult:
    try:
        artifact = executor()
    except Exception as exc:
        return _StepExecutionResult(
            output=_failed_step_output(
                step=step,
                errors=(_exception_error(step=step, exc=exc),),
            ),
            failed=True,
            artifact={},
            artifact_path=Path(),
        )

    if not isinstance(artifact, Mapping):
        return _StepExecutionResult(
            output=_failed_step_output(
                step=step,
                errors=(
                    {
                        "step": step,
                        "code": "artifact_type",
                        "path": "$",
                        "reason": "Step executor did not return an object artifact",
                    },
                ),
            ),
            failed=True,
            artifact={},
            artifact_path=Path(),
        )

    copied_artifact = dict(artifact)
    artifact_path_text = _optional_text(copied_artifact.get("artifact_path"))
    if artifact_path_text is None:
        return _StepExecutionResult(
            output=_failed_step_output(
                step=step,
                errors=(
                    {
                        "step": step,
                        "code": "artifact_path_missing",
                        "path": "$.artifact_path",
                        "reason": "Step artifact is missing artifact_path",
                    },
                ),
            ),
            failed=True,
            artifact={},
            artifact_path=Path(),
        )

    errors = validator(copied_artifact)
    if errors:
        return _StepExecutionResult(
            output=_failed_step_output(
                step=step,
                errors=_schema_errors_to_step_errors(step=step, errors=errors),
            ),
            failed=True,
            artifact={},
            artifact_path=Path(),
        )

    payload_hash = _optional_text(copied_artifact.get(payload_hash_field))
    policy_hash = _optional_text(copied_artifact.get(policy_hash_field))
    output: dict[str, Any] = {
        "status": "OK",
        "artifact_path": artifact_path_text,
        "run_id": _optional_text(copied_artifact.get("run_id")),
        "payload_hash": payload_hash,
        "policy_hash": policy_hash,
        "errors": [],
    }
    return _StepExecutionResult(
        output=output,
        failed=False,
        artifact=copied_artifact,
        artifact_path=Path(artifact_path_text),
    )


def _run_pipeline_step(
    *,
    run_context: RunContext,
    config: SmokePipelineArtifactOnlyConfig,
    forecast_artifact: Mapping[str, Any],
    forecast_artifact_path: Path,
    decision_artifact: Mapping[str, Any],
    trade_plan_artifact: Mapping[str, Any],
    execution_artifact: Mapping[str, Any],
    portfolio_artifact: Mapping[str, Any],
    decision_artifact_path: Path,
    trade_plan_artifact_path: Path,
    execution_artifact_path: Path,
    portfolio_artifact_path: Path,
) -> _StepExecutionResult:
    try:
        quality_flags, quality_warnings = _collect_pipeline_quality_signals(
            forecast_artifact=forecast_artifact,
            decision_artifact=decision_artifact,
            trade_plan_artifact=trade_plan_artifact,
            execution_artifact=execution_artifact,
            portfolio_artifact=portfolio_artifact,
        )
        artifact = build_pipeline_run_artifact(
            run_context=run_context,
            pipeline_params=config.as_hash_dict(),
            forecast_input={
                "forecast_artifact_path": str(forecast_artifact_path),
                "forecast_artifact_hash": _resolve_payload_hash(
                    forecast_artifact,
                    primary_field="forecast_payload_hash",
                ),
                "forecast_run_id": _optional_text(forecast_artifact.get("run_id")),
            },
            decision_artifact=decision_artifact,
            trade_plan_artifact=trade_plan_artifact,
            execution_artifact=execution_artifact,
            portfolio_artifact=portfolio_artifact,
            decision_artifact_path=str(decision_artifact_path),
            trade_plan_artifact_path=str(trade_plan_artifact_path),
            execution_artifact_path=str(execution_artifact_path),
            portfolio_artifact_path=str(portfolio_artifact_path),
            quality_flags=quality_flags,
            quality_warnings=quality_warnings,
        )
        validation_errors = validate_pipeline_run_artifact(artifact)
        if validation_errors:
            return _StepExecutionResult(
                output=_failed_step_output(
                    step="pipeline",
                    errors=_schema_errors_to_step_errors(
                        step="pipeline",
                        errors=validation_errors,
                    ),
                ),
                failed=True,
                artifact={},
                artifact_path=Path(),
            )

        artifact_path = _write_pipeline_artifact(
            artifacts_root=config.artifacts_root,
            run_id=run_context.run_id,
            artifact=artifact,
        )
        artifact["artifact_path"] = str(artifact_path)
        return _StepExecutionResult(
            output={
                "status": "OK",
                "artifact_path": str(artifact_path),
                "run_id": _optional_text(artifact.get("run_id")),
                "payload_hash": _optional_text(artifact.get("pipeline_payload_hash")),
                "policy_hash": _optional_text(artifact.get("pipeline_policy_hash")),
                "errors": [],
            },
            failed=False,
            artifact=artifact,
            artifact_path=artifact_path,
        )
    except Exception as exc:
        return _StepExecutionResult(
            output=_failed_step_output(
                step="pipeline",
                errors=(_exception_error(step="pipeline", exc=exc),),
            ),
            failed=True,
            artifact={},
            artifact_path=Path(),
        )


def _collect_pipeline_quality_signals(
    *,
    forecast_artifact: Mapping[str, Any],
    decision_artifact: Mapping[str, Any],
    trade_plan_artifact: Mapping[str, Any],
    execution_artifact: Mapping[str, Any],
    portfolio_artifact: Mapping[str, Any],
) -> tuple[tuple[str, ...], tuple[dict[str, str], ...]]:
    flags: set[str] = set()
    warnings: dict[tuple[str, str, str], dict[str, str]] = {}

    def collect_from_mapping(raw: Mapping[str, Any], *, source: str) -> None:
        raw_flags = raw.get("quality_flags")
        if isinstance(raw_flags, Sequence) and not isinstance(raw_flags, (str, bytes, bytearray)):
            for value in raw_flags:
                text = _optional_text(value)
                if text is not None:
                    flags.add(text)

        raw_warnings = raw.get("quality_warnings")
        if isinstance(raw_warnings, Sequence) and not isinstance(
            raw_warnings,
            (str, bytes, bytearray),
        ):
            for raw_warning in raw_warnings:
                if not isinstance(raw_warning, Mapping):
                    continue
                code = _optional_text(raw_warning.get("code")) or "unknown_warning"
                message = _optional_text(raw_warning.get("message")) or _optional_text(
                    raw_warning.get("detail")
                )
                key = (code, message or "", source)
                payload: dict[str, str] = {"code": code, "source": source}
                if message is not None:
                    payload["message"] = message
                warnings[key] = payload

    collect_from_mapping(forecast_artifact, source="forecast")
    collect_from_mapping(portfolio_artifact, source="portfolio")

    raw_items = decision_artifact.get("items")
    if isinstance(raw_items, Sequence) and not isinstance(raw_items, (str, bytes, bytearray)):
        for raw_item in raw_items:
            if isinstance(raw_item, Mapping):
                collect_from_mapping(raw_item, source="decision_item")

    for source_name, artifact in (
        ("trade_plan_order", trade_plan_artifact.get("orders")),
        ("trade_plan_skipped", trade_plan_artifact.get("skipped")),
        ("execution_order", execution_artifact.get("orders")),
        ("execution_skipped", execution_artifact.get("skipped")),
    ):
        if isinstance(artifact, Sequence) and not isinstance(artifact, (str, bytes, bytearray)):
            for raw_item in artifact:
                if isinstance(raw_item, Mapping):
                    collect_from_mapping(raw_item, source=source_name)

    ordered_warning_keys = sorted(warnings.keys(), key=lambda item: item)
    return (
        tuple(sorted(flags)),
        tuple(warnings[key] for key in ordered_warning_keys),
    )


def _build_smoke_summary(
    *,
    run_context: RunContext,
    config: SmokePipelineArtifactOnlyConfig,
    forecast_artifact_path: Path,
    forecast_file_hash: str,
    forecast_payload_hash: str,
    forecast_run_id: str | None,
    nonce: str,
    step_outputs: Mapping[str, Mapping[str, Any]],
    quality_flags: Sequence[str],
    quality_warnings: Sequence[Mapping[str, str]],
    overall_status: str,
) -> dict[str, Any]:
    ordered_outputs = {
        step: {
            "status": _optional_text(step_outputs[step].get("status")),
            "artifact_path": _optional_text(step_outputs[step].get("artifact_path")),
            "run_id": _optional_text(step_outputs[step].get("run_id")),
            "payload_hash": _optional_text(step_outputs[step].get("payload_hash")),
            "policy_hash": _optional_text(step_outputs[step].get("policy_hash")),
            "errors": _normalize_step_errors(step_outputs[step].get("errors")),
        }
        for step in STEP_ORDER
    }
    n_steps_ok = sum(
        1 for step in STEP_ORDER if _optional_text(ordered_outputs[step].get("status")) == "OK"
    )
    n_steps_fail = sum(
        1 for step in STEP_ORDER if _optional_text(ordered_outputs[step].get("status")) == "FAIL"
    )
    n_steps_skipped = sum(
        1 for step in STEP_ORDER if _optional_text(ordered_outputs[step].get("status")) == "SKIPPED"
    )
    normalized_flags = sorted({flag for flag in quality_flags if flag})
    normalized_warnings = _normalize_warnings(quality_warnings)

    payload: dict[str, Any] = {
        "artifact_schema_version": SMOKE_SUMMARY_SCHEMA_VERSION,
        "run_id": run_context.run_id,
        "generated_at_utc": run_context.started_at,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "smoke_policy_version": SMOKE_POLICY_VERSION,
        "inputs": {
            "forecast_artifact_path": str(forecast_artifact_path),
            "forecast_file_hash": forecast_file_hash,
            "forecast_payload_hash": forecast_payload_hash,
            "forecast_run_id": forecast_run_id,
            "nonce": nonce,
        },
        "outputs": ordered_outputs,
        "overall_status": overall_status,
        "counts": {
            "n_steps_ok": n_steps_ok,
            "n_steps_fail": n_steps_fail,
            "n_steps_skipped": n_steps_skipped,
            "n_quality_flags": len(normalized_flags),
            "n_quality_warnings": len(normalized_warnings),
        },
        "quality_flags": normalized_flags,
        "quality_warnings": normalized_warnings,
    }
    payload["smoke_policy_hash"] = canonical_hash(
        {
            "policy_version": SMOKE_POLICY_VERSION,
            "params": config.as_hash_dict(),
        }
    )
    payload["smoke_outputs_hash"] = canonical_hash(
        [
            {
                "step": step,
                "status": ordered_outputs[step]["status"],
                "payload_hash": ordered_outputs[step]["payload_hash"],
                "policy_hash": ordered_outputs[step]["policy_hash"],
            }
            for step in STEP_ORDER
        ]
    )
    payload_without_self = dict(payload)
    payload["smoke_payload_hash"] = canonical_hash(payload_without_self)
    return payload


def _overall_status(
    *,
    step_outputs: Mapping[str, Mapping[str, Any]],
    monitoring_artifact: Mapping[str, Any] | None,
    n_flags: int,
    n_warnings: int,
) -> str:
    if any(_optional_text(step_outputs[step].get("status")) == "FAIL" for step in STEP_ORDER):
        return "FAIL"
    health_status = (
        None
        if monitoring_artifact is None
        else _optional_text(monitoring_artifact.get("health_status"))
    )
    if health_status == "FAIL":
        return "FAIL"
    if health_status == "WARN":
        return "WARN"
    if n_flags > 0 or n_warnings > 0:
        return "WARN"
    return "OK"


def _collect_quality_signals(
    *,
    artifacts: Sequence[Mapping[str, Any]],
) -> tuple[list[str], list[dict[str, str]]]:
    flags: set[str] = set()
    warnings: dict[tuple[str, str, str], dict[str, str]] = {}
    for artifact in artifacts:
        source = _optional_text(artifact.get("artifact_schema_version")) or "unknown_artifact"
        raw_flags = artifact.get("quality_flags")
        if isinstance(raw_flags, Sequence) and not isinstance(raw_flags, (str, bytes, bytearray)):
            for raw in raw_flags:
                text = _optional_text(raw)
                if text is not None:
                    flags.add(text)

        raw_warnings = artifact.get("quality_warnings")
        if isinstance(raw_warnings, Sequence) and not isinstance(
            raw_warnings,
            (str, bytes, bytearray),
        ):
            for raw in raw_warnings:
                if not isinstance(raw, Mapping):
                    continue
                code = _optional_text(raw.get("code")) or "unknown_warning"
                message = _optional_text(raw.get("message")) or _optional_text(raw.get("detail"))
                key = (code, message or "", source)
                payload: dict[str, str] = {"code": code, "source": source}
                if message is not None:
                    payload["message"] = message
                warnings[key] = payload

    warning_keys = sorted(warnings.keys(), key=lambda item: item)
    return (
        sorted(flags),
        [warnings[key] for key in warning_keys],
    )


def _resolve_started_at(forecast_artifact: Mapping[str, Any]) -> datetime:
    top_level = _parse_optional_datetime(forecast_artifact.get("from_ts"))
    if top_level is not None:
        return top_level
    forecasts_obj = forecast_artifact.get("forecasts")
    if isinstance(forecasts_obj, Sequence) and not isinstance(
        forecasts_obj,
        (str, bytes, bytearray),
    ):
        parsed_values = [
            parsed
            for parsed in (
                _parse_optional_datetime(item.get("decision_ts"))
                for item in forecasts_obj
                if isinstance(item, Mapping)
            )
            if parsed is not None
        ]
        if parsed_values:
            return min(parsed_values)
    return datetime(1970, 1, 1, tzinfo=UTC)


def _resolve_payload_hash(artifact: Mapping[str, Any], *, primary_field: str) -> str:
    value = _optional_text(artifact.get(primary_field))
    if value is not None and _is_sha256(value):
        return value
    return canonical_hash(artifact)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"{label} root must be a JSON object")
    return raw


def _write_pipeline_artifact(
    *,
    artifacts_root: str,
    run_id: str,
    artifact: dict[str, Any],
) -> Path:
    root = Path(artifacts_root) / "pipeline_runs"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _ensure_generated_at(
    *,
    artifact_path: Path,
    artifact: dict[str, Any],
    fallback_timestamp: str,
) -> None:
    if _optional_text(artifact.get("generated_at_utc")) is not None:
        return
    artifact["generated_at_utc"] = fallback_timestamp
    artifact_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _write_summary_artifact(
    *,
    artifacts_root: str,
    run_id: str,
    artifact: dict[str, Any],
) -> Path:
    root = Path(artifacts_root) / "smoke"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run deterministic artifact-only smoke pipeline and validate each artifact."
    )
    parser.add_argument(
        "--forecast-artifact",
        default=str(DEFAULT_FORECAST_FIXTURE),
        help="Input forecast artifact JSON (default: repository fixture).",
    )
    parser.add_argument("--artifacts-root", default="artifacts")
    parser.add_argument(
        "--nonce",
        default="smoke",
        help="Deterministic nonce. Same input + same nonce => same payload hashes.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with non-zero status when overall_status is FAIL.",
    )

    parser.add_argument("--min-edge-bps", type=float, default=50.0)
    parser.add_argument(
        "--robust-mode",
        choices=["require_positive_low90", "require_negative_high90", "none"],
        default="require_positive_low90",
    )
    parser.add_argument("--max-items", type=int, default=200)

    parser.add_argument(
        "--sizing-mode",
        choices=["fixed_notional", "scaled_by_edge"],
        default="fixed_notional",
    )
    parser.add_argument("--fixed-notional-usd", type=float, default=25.0)
    parser.add_argument("--base-notional-usd", type=float, default=25.0)
    parser.add_argument("--target-edge-bps", type=float, default=100.0)
    parser.add_argument("--min-scale", type=float, default=0.5)
    parser.add_argument("--max-scale", type=float, default=2.0)
    parser.add_argument("--max-orders", type=int, default=200)
    parser.add_argument("--max-total-notional-usd", type=float, default=5000.0)
    parser.add_argument("--max-notional-per-market-usd", type=float, default=500.0)
    parser.add_argument("--max-notional-per-category-usd", type=float, default=2000.0)

    parser.add_argument(
        "--execution-mode",
        choices=["dry_run"],
        default="dry_run",
        help="Execution mode is constrained to dry_run for smoke artifact-only runs.",
    )

    parser.add_argument(
        "--mark-source",
        choices=["execution_price", "execution_p_cal", "execution_price_prob"],
        default="execution_price",
    )
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--fee-usd", type=float, default=0.0)
    parser.add_argument("--reference-prices-json", default=None)

    parser.add_argument(
        "--fail-on-critical-block",
        dest="fail_on_critical_block",
        action="store_true",
        default=True,
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
        default=True,
    )
    parser.add_argument(
        "--no-warn-on-any-quality-signal",
        dest="warn_on_any_quality_signal",
        action="store_false",
    )

    return parser.parse_args(argv)


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_warnings(raw_warnings: Sequence[Mapping[str, str]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str], dict[str, str]] = {}
    for warning in raw_warnings:
        code = _optional_text(warning.get("code")) or "unknown_warning"
        message = _optional_text(warning.get("message")) or ""
        source = _optional_text(warning.get("source")) or "smoke"
        deduped[(code, message, source)] = {
            "code": code,
            "message": message,
            "source": source,
        }
    keys = sorted(deduped.keys(), key=lambda item: item)
    return [deduped[key] for key in keys]


def _normalize_step_errors(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return []
    out: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        step = _optional_text(item.get("step")) or "unknown_step"
        code = _optional_text(item.get("code")) or "unknown_error"
        path = _optional_text(item.get("path")) or "$"
        reason = _optional_text(item.get("reason")) or "unknown"
        out.append({"step": step, "code": code, "path": path, "reason": reason})
    out.sort(key=lambda item: (item["step"], item["code"], item["path"], item["reason"]))
    return out


def _schema_errors_to_step_errors(
    *,
    step: str,
    errors: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, str], ...]:
    normalized = [
        {
            "step": step,
            "code": _optional_text(error.get("code")) or "schema:error",
            "path": _optional_text(error.get("path")) or "$",
            "reason": _optional_text(error.get("reason")) or "schema validation failed",
        }
        for error in errors
    ]
    normalized.sort(key=lambda item: (item["code"], item["path"], item["reason"]))
    return tuple(normalized)


def _failed_step_output(
    *,
    step: str,
    errors: Sequence[Mapping[str, str]],
) -> dict[str, Any]:
    return {
        "status": "FAIL",
        "artifact_path": None,
        "run_id": None,
        "payload_hash": None,
        "policy_hash": None,
        "errors": _normalize_step_errors(
            [
                {
                    "step": step,
                    "code": _optional_text(error.get("code")) or "unknown_error",
                    "path": _optional_text(error.get("path")) or "$",
                    "reason": _optional_text(error.get("reason")) or "unknown",
                }
                for error in errors
            ]
        ),
    }


def _skipped_step_output(*, step: str, reason: str) -> dict[str, Any]:
    return {
        "status": "SKIPPED",
        "artifact_path": None,
        "run_id": None,
        "payload_hash": None,
        "policy_hash": None,
        "errors": [
            {
                "step": step,
                "code": "skipped:dependency_failed",
                "path": "$",
                "reason": reason,
            }
        ],
    }


def _exception_error(*, step: str, exc: Exception) -> dict[str, str]:
    return {
        "step": step,
        "code": f"exception:{type(exc).__name__}",
        "path": "$",
        "reason": str(exc),
    }


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _is_sha256(raw: str) -> bool:
    return len(raw) == 64 and all(char in "0123456789abcdef" for char in raw)


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
