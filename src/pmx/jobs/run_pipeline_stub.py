from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.decisions.policy import RobustMode
from pmx.execution.policy import ExecutionMode
from pmx.forecast.validate_artifact import validate_forecast_artifact
from pmx.jobs.decide_from_forecast import (
    load_decide_from_forecast_config,
    run_decide_from_forecast,
)
from pmx.jobs.execute_trade_plan_stub import (
    load_execute_trade_plan_stub_config,
    run_execute_trade_plan_stub,
)
from pmx.jobs.portfolio_from_execution import (
    load_portfolio_from_execution_config,
    run_portfolio_from_execution,
)
from pmx.jobs.trade_plan_from_decision import (
    load_trade_plan_from_decision_config,
    run_trade_plan_from_decision,
)
from pmx.pipeline.artifact import PIPELINE_POLICY_VERSION, build_pipeline_run_artifact
from pmx.pipeline.canonical import canonical_hash
from pmx.pipeline.validate_artifact import validate_pipeline_run_artifact
from pmx.portfolio.valuation import MarkSource
from pmx.trade_plan.policy import SizingMode

JOB_NAME = "run_pipeline_stub"


@dataclass(frozen=True, slots=True)
class RunPipelineStubConfig:
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
    mark_source: MarkSource
    reference_prices_json: str | None

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "artifacts_root": self.artifacts_root,
            "base_notional_usd": self.base_notional_usd,
            "execution_mode": self.execution_mode,
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
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    forecast_artifact_path = Path(args.forecast_artifact or args.forecast_fixture)
    config = RunPipelineStubConfig(
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
        mark_source=cast(MarkSource, args.mark_source),
        reference_prices_json=args.reference_prices_json,
    )
    run_pipeline_stub(
        forecast_artifact_path=forecast_artifact_path,
        config=config,
        nonce=args.nonce,
    )
    return 0


def run_pipeline_stub(
    *,
    forecast_artifact_path: Path,
    config: RunPipelineStubConfig,
    nonce: str | None = None,
) -> dict[str, Any]:
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    forecast_artifact = _load_forecast_artifact(forecast_artifact_path)
    forecast_errors = validate_forecast_artifact(forecast_artifact)
    if forecast_errors:
        raise ValueError(f"Invalid forecast artifact: {forecast_errors[0]}")

    forecast_hash = _resolve_forecast_hash(forecast_artifact)
    forecast_run_id = _optional_text(forecast_artifact.get("run_id"))
    started_at = _resolve_started_at(forecast_artifact)
    deterministic_nonce = nonce or canonical_hash(
        {
            "forecast_artifact_hash": forecast_hash,
            "forecast_run_id": forecast_run_id,
            "params": config.as_hash_dict(),
            "policy_version": PIPELINE_POLICY_VERSION,
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "forecast_artifact_hash": forecast_hash,
            "forecast_run_id": forecast_run_id,
            "params": config.as_hash_dict(),
            "policy_version": PIPELINE_POLICY_VERSION,
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "pipeline_stub_started",
        run_context,
        forecast_artifact_path=str(forecast_artifact_path),
        forecast_artifact_hash=forecast_hash,
    )

    decide_config = load_decide_from_forecast_config(
        min_edge_bps=config.min_edge_bps,
        robust_mode=config.robust_mode,
        max_items=config.max_items,
        artifacts_root=config.artifacts_root,
    )
    decision_artifact = run_decide_from_forecast(
        forecast_artifact_path=forecast_artifact_path,
        config=decide_config,
        nonce=f"{run_context.run_id}:decision",
    )
    decision_path = Path(
        _require_text(decision_artifact.get("artifact_path"), "decision.artifact_path")
    )
    _ensure_generated_at(
        artifact_path=decision_path,
        artifact=decision_artifact,
        fallback_timestamp=run_context.started_at,
    )

    trade_plan_config = load_trade_plan_from_decision_config(
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
    )
    trade_plan_artifact = run_trade_plan_from_decision(
        decision_artifact_path=decision_path,
        config=trade_plan_config,
        nonce=f"{run_context.run_id}:trade_plan",
    )
    trade_plan_path = Path(
        _require_text(trade_plan_artifact.get("artifact_path"), "trade_plan.artifact_path")
    )

    execute_config = load_execute_trade_plan_stub_config(
        mode=config.execution_mode,
        max_orders=config.max_orders,
        simulate_reject_modulo=0,
        simulate_reject_remainder=0,
        artifacts_root=config.artifacts_root,
    )
    execution_artifact = run_execute_trade_plan_stub(
        trade_plan_artifact_path=trade_plan_path,
        config=execute_config,
        nonce=f"{run_context.run_id}:execution",
    )
    execution_path = Path(
        _require_text(execution_artifact.get("artifact_path"), "execution.artifact_path")
    )

    portfolio_config = load_portfolio_from_execution_config(
        artifacts_root=config.artifacts_root,
        fee_bps=config.fee_bps,
        fee_usd=config.fee_usd,
        mark_source=config.mark_source,
        reference_prices_json=config.reference_prices_json,
    )
    portfolio_artifact = run_portfolio_from_execution(
        execution_artifact_paths=[execution_path],
        config=portfolio_config,
        nonce=f"{run_context.run_id}:portfolio",
    )
    portfolio_path = Path(
        _require_text(portfolio_artifact.get("artifact_path"), "portfolio.artifact_path")
    )

    quality_flags, quality_warnings = _collect_quality_signals(
        forecast_artifact=forecast_artifact,
        decision_artifact=decision_artifact,
        trade_plan_artifact=trade_plan_artifact,
        execution_artifact=execution_artifact,
        portfolio_artifact=portfolio_artifact,
    )
    summary_artifact = build_pipeline_run_artifact(
        run_context=run_context,
        pipeline_params=config.as_hash_dict(),
        forecast_input={
            "forecast_artifact_path": str(forecast_artifact_path),
            "forecast_artifact_hash": forecast_hash,
            "forecast_run_id": forecast_run_id,
        },
        decision_artifact=decision_artifact,
        trade_plan_artifact=trade_plan_artifact,
        execution_artifact=execution_artifact,
        portfolio_artifact=portfolio_artifact,
        decision_artifact_path=str(decision_path),
        trade_plan_artifact_path=str(trade_plan_path),
        execution_artifact_path=str(execution_path),
        portfolio_artifact_path=str(portfolio_path),
        quality_flags=quality_flags,
        quality_warnings=quality_warnings,
    )
    summary_errors = validate_pipeline_run_artifact(summary_artifact)
    if summary_errors:
        raise ValueError(f"Invalid pipeline-run artifact: {summary_errors[0]}")

    summary_path = _write_pipeline_artifact(
        artifacts_root=config.artifacts_root,
        run_id=run_context.run_id,
        artifact=summary_artifact,
    )
    _log(
        logger,
        logging.INFO,
        "pipeline_stub_completed",
        run_context,
        summary_artifact_path=str(summary_path),
        decision_payload_hash=str(summary_artifact["outputs"]["decision"]["payload_hash"]),
        trade_plan_payload_hash=str(summary_artifact["outputs"]["trade_plan"]["payload_hash"]),
        execution_payload_hash=str(summary_artifact["outputs"]["execution"]["payload_hash"]),
        portfolio_payload_hash=str(summary_artifact["outputs"]["portfolio"]["payload_hash"]),
        pipeline_payload_hash=str(summary_artifact["pipeline_payload_hash"]),
    )
    summary_artifact["artifact_path"] = str(summary_path)
    return summary_artifact


def _collect_quality_signals(
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
            raw_warnings, (str, bytes, bytearray)
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


def _load_forecast_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Forecast artifact not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("Forecast artifact root must be a JSON object")
    return raw


def _resolve_forecast_hash(forecast_artifact: Mapping[str, Any]) -> str:
    value = _optional_text(forecast_artifact.get("forecast_payload_hash"))
    if value is not None and len(value) == 64 and all(char in "0123456789abcdef" for char in value):
        return value
    return canonical_hash(forecast_artifact)


def _resolve_started_at(forecast_artifact: Mapping[str, Any]) -> datetime:
    top_level = _parse_optional_datetime(forecast_artifact.get("from_ts"))
    if top_level is not None:
        return top_level
    forecasts_obj = forecast_artifact.get("forecasts")
    if isinstance(forecasts_obj, Sequence) and not isinstance(
        forecasts_obj, (str, bytes, bytearray)
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
    return datetime.now(UTC)


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full artifact-only PMX pipeline sequence.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--forecast-artifact",
        default=None,
        help="Input forecast artifact JSON.",
    )
    input_group.add_argument(
        "--forecast-fixture",
        default=None,
        help="Alias of --forecast-artifact for fixture-driven runs.",
    )
    parser.add_argument("--artifacts-root", default="artifacts")

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
        help="Execution mode for stub runner (artifact-only).",
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
        "--nonce",
        default=None,
        help="Optional nonce override for deterministic pipeline run_id generation.",
    )
    return parser.parse_args(argv)


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _require_text(raw: Any, path: str) -> str:
    value = _optional_text(raw)
    if value is None:
        raise ValueError(f"{path} must be a non-empty string")
    return value


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
