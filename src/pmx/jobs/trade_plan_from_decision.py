from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.decisions.validate_artifact import validate_decision_artifact
from pmx.trade_plan.artifact import TRADE_PLAN_POLICY_VERSION, build_trade_plan_artifact
from pmx.trade_plan.canonical import canonical_hash
from pmx.trade_plan.policy import SizingMode, TradePlanPolicyConfig, build_trade_plan
from pmx.trade_plan.validate_artifact import validate_trade_plan_artifact

JOB_NAME = "trade_plan_from_decision"


@dataclass(frozen=True, slots=True)
class TradePlanFromDecisionConfig:
    max_orders: int
    max_total_notional_usd: float
    max_notional_per_market_usd: float
    max_notional_per_category_usd: float
    sizing_mode: SizingMode
    fixed_notional_usd: float
    base_notional_usd: float
    target_edge_bps: float
    min_scale: float
    max_scale: float
    dry_run: bool
    artifacts_root: str

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "artifacts_root": self.artifacts_root,
            "base_notional_usd": self.base_notional_usd,
            "dry_run": self.dry_run,
            "fixed_notional_usd": self.fixed_notional_usd,
            "max_notional_per_category_usd": self.max_notional_per_category_usd,
            "max_notional_per_market_usd": self.max_notional_per_market_usd,
            "max_orders": self.max_orders,
            "max_scale": self.max_scale,
            "max_total_notional_usd": self.max_total_notional_usd,
            "min_scale": self.min_scale,
            "sizing_mode": self.sizing_mode,
            "target_edge_bps": self.target_edge_bps,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_trade_plan_from_decision_config(
        max_orders=args.max_orders,
        max_total_notional_usd=args.max_total_notional_usd,
        max_notional_per_market_usd=args.max_notional_per_market_usd,
        max_notional_per_category_usd=args.max_notional_per_category_usd,
        sizing_mode=args.sizing_mode,
        fixed_notional_usd=args.fixed_notional_usd,
        base_notional_usd=args.base_notional_usd,
        target_edge_bps=args.target_edge_bps,
        min_scale=args.min_scale,
        max_scale=args.max_scale,
        dry_run=args.dry_run,
        artifacts_root=args.artifacts_root,
    )
    run_trade_plan_from_decision(
        decision_artifact_path=Path(args.decision_artifact),
        config=config,
    )
    return 0


def run_trade_plan_from_decision(
    *,
    decision_artifact_path: Path,
    config: TradePlanFromDecisionConfig,
    nonce: str | None = None,
) -> dict[str, Any]:
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    decision_artifact = _load_decision_artifact(decision_artifact_path)
    decision_errors = validate_decision_artifact(decision_artifact)
    if decision_errors:
        raise ValueError(f"Invalid decision artifact: {decision_errors[0]}")

    input_hash = _resolve_input_decision_artifact_hash(decision_artifact)
    started_at = _resolve_started_at(decision_artifact)
    deterministic_nonce = nonce or canonical_hash(
        {
            "input_decision_artifact_hash": input_hash,
            "input_decision_run_id": decision_artifact.get("run_id"),
            "params": config.as_hash_dict(),
            "policy_version": TRADE_PLAN_POLICY_VERSION,
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "input_decision_artifact_hash": input_hash,
            "input_decision_run_id": decision_artifact.get("run_id"),
            "params": config.as_hash_dict(),
            "policy_version": TRADE_PLAN_POLICY_VERSION,
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "trade_plan_from_decision_started",
        run_context,
        decision_artifact_path=str(decision_artifact_path),
        input_decision_artifact_hash=input_hash,
    )

    policy_config = TradePlanPolicyConfig(
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
    )
    result = build_trade_plan(decision_artifact, policy_config)
    artifact = build_trade_plan_artifact(
        run_context=run_context,
        decision_artifact=decision_artifact,
        params=policy_config.as_hash_dict(),
        orders=result.orders,
        skipped=result.skipped,
        policy_version=TRADE_PLAN_POLICY_VERSION,
        input_decision_artifact_hash=input_hash,
    )
    artifact["counts"] = result.counts

    artifact_errors = validate_trade_plan_artifact(artifact)
    if artifact_errors:
        raise ValueError(f"Invalid trade-plan artifact: {artifact_errors[0]}")

    artifact_path = _write_artifact(config.artifacts_root, run_context.run_id, artifact)
    _log(
        logger,
        logging.INFO,
        "trade_plan_from_decision_completed",
        run_context,
        artifact_path=str(artifact_path),
        n_total=result.counts["n_total"],
        n_orders=result.counts["n_orders"],
        n_skipped=result.counts["n_skipped"],
        orders_hash=str(artifact["orders_hash"]),
        trade_plan_payload_hash=str(artifact["trade_plan_payload_hash"]),
    )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def load_trade_plan_from_decision_config(
    *,
    max_orders: int | None,
    max_total_notional_usd: float | None,
    max_notional_per_market_usd: float | None,
    max_notional_per_category_usd: float | None,
    sizing_mode: str | None,
    fixed_notional_usd: float | None,
    base_notional_usd: float | None,
    target_edge_bps: float | None,
    min_scale: float | None,
    max_scale: float | None,
    dry_run: bool | None,
    artifacts_root: str | None,
) -> TradePlanFromDecisionConfig:
    resolved_max_orders = (
        max_orders if max_orders is not None else _load_positive_int("MAX_ORDERS", 200)
    )
    resolved_max_total = (
        max_total_notional_usd
        if max_total_notional_usd is not None
        else _load_positive_float("MAX_TOTAL_NOTIONAL_USD", 5000.0)
    )
    resolved_max_market = (
        max_notional_per_market_usd
        if max_notional_per_market_usd is not None
        else _load_positive_float("MAX_NOTIONAL_PER_MARKET_USD", 500.0)
    )
    resolved_max_category = (
        max_notional_per_category_usd
        if max_notional_per_category_usd is not None
        else _load_positive_float("MAX_NOTIONAL_PER_CATEGORY_USD", 2000.0)
    )
    resolved_mode = _parse_sizing_mode(
        sizing_mode or os.getenv("TRADE_PLAN_SIZING_MODE") or "fixed_notional"
    )
    resolved_fixed = (
        fixed_notional_usd
        if fixed_notional_usd is not None
        else _load_positive_float("FIXED_NOTIONAL_USD", 25.0)
    )
    resolved_base = (
        base_notional_usd
        if base_notional_usd is not None
        else _load_positive_float("BASE_NOTIONAL_USD", 25.0)
    )
    resolved_target_edge = (
        target_edge_bps
        if target_edge_bps is not None
        else _load_positive_float("TARGET_EDGE_BPS", 100.0)
    )
    resolved_min_scale = (
        min_scale if min_scale is not None else _load_positive_float("MIN_SCALE", 0.5)
    )
    resolved_max_scale = (
        max_scale if max_scale is not None else _load_positive_float("MAX_SCALE", 2.0)
    )
    if resolved_min_scale > resolved_max_scale:
        raise ValueError("min_scale must be <= max_scale")

    resolved_dry_run = dry_run if dry_run is not None else _load_bool("TRADE_PLAN_DRY_RUN", True)
    resolved_root = artifacts_root or os.getenv("TRADE_PLAN_ARTIFACTS_ROOT") or "artifacts"
    return TradePlanFromDecisionConfig(
        max_orders=resolved_max_orders,
        max_total_notional_usd=resolved_max_total,
        max_notional_per_market_usd=resolved_max_market,
        max_notional_per_category_usd=resolved_max_category,
        sizing_mode=resolved_mode,
        fixed_notional_usd=resolved_fixed,
        base_notional_usd=resolved_base,
        target_edge_bps=resolved_target_edge,
        min_scale=resolved_min_scale,
        max_scale=resolved_max_scale,
        dry_run=resolved_dry_run,
        artifacts_root=resolved_root,
    )


def _load_decision_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Decision artifact not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("Decision artifact root must be a JSON object")
    return raw


def _resolve_started_at(decision_artifact: dict[str, Any]) -> datetime:
    top_level = _parse_optional_datetime(decision_artifact.get("generated_at_utc"))
    if top_level is not None:
        return top_level

    fallback = _parse_optional_datetime(decision_artifact.get("started_at"))
    if fallback is not None:
        return fallback

    items = decision_artifact.get("items")
    if isinstance(items, list):
        candidates = [
            parsed
            for parsed in (
                _parse_optional_datetime(item.get("decision_ts"))
                for item in items
                if isinstance(item, dict)
            )
            if parsed is not None
        ]
        if candidates:
            return min(candidates)
    return datetime.now(UTC)


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
    return _as_utc_datetime(parsed)


def _resolve_input_decision_artifact_hash(decision_artifact: dict[str, Any]) -> str:
    value = decision_artifact.get("decision_payload_hash")
    if isinstance(value, str):
        normalized = value.strip()
        if len(normalized) == 64 and all(char in "0123456789abcdef" for char in normalized):
            return normalized
    return canonical_hash(decision_artifact)


def _write_artifact(artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root) / "trade_plans"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic trade-plan artifact from a decision artifact."
    )
    parser.add_argument(
        "--decision-artifact",
        required=True,
        help="Path to decision artifact JSON file.",
    )
    parser.add_argument("--max-orders", type=int, default=200)
    parser.add_argument("--max-total-notional-usd", type=float, default=5000.0)
    parser.add_argument("--max-notional-per-market-usd", type=float, default=500.0)
    parser.add_argument("--max-notional-per-category-usd", type=float, default=2000.0)
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
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="Artifact-only mode (default).",
    )
    parser.add_argument(
        "--no-dry-run",
        dest="dry_run",
        action="store_false",
        help="Reserved for future non-artifact execution integrations.",
    )
    parser.add_argument(
        "--artifacts-root",
        default="artifacts",
        help="Artifacts root directory. Output is written under <root>/trade_plans.",
    )
    return parser.parse_args(argv)


def _parse_sizing_mode(raw: str) -> SizingMode:
    value = raw.strip()
    allowed = {"fixed_notional", "scaled_by_edge"}
    if value not in allowed:
        raise ValueError(f"Unsupported sizing_mode: {raw!r}")
    return cast(SizingMode, value)


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _load_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be > 0, got {parsed}")
    return parsed


def _load_positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if parsed <= 0.0:
        raise ValueError(f"{name} must be > 0, got {parsed}")
    return parsed


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
