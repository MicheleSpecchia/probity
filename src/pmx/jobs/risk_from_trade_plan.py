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
from pmx.performance.validate_artifact import validate_performance_report_artifact
from pmx.risk.artifact import RISK_POLICY_VERSION, build_risk_artifact
from pmx.risk.canonical import canonical_hash
from pmx.risk.policy import RiskHooks, RiskPolicyConfig, evaluate_risk_policy
from pmx.risk.validate_artifact import validate_risk_artifact
from pmx.trade_plan.validate_artifact import validate_trade_plan_artifact

JOB_NAME = "risk_from_trade_plan"


@dataclass(frozen=True, slots=True)
class RiskFromTradePlanConfig:
    artifacts_root: str
    max_total_notional_usd: float
    max_notional_per_market_usd: float
    max_notional_per_category_usd: float
    top1_share_cap: float
    top3_share_cap: float
    performance_top1_cap: float
    performance_top3_cap: float
    allow_downsize: bool
    min_notional_usd: float
    blocking_quality_flags: tuple[str, ...]
    cooldown_block_flags: tuple[str, ...]

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "allow_downsize": self.allow_downsize,
            "artifacts_root": self.artifacts_root,
            "blocking_quality_flags": list(self.blocking_quality_flags),
            "cooldown_block_flags": list(self.cooldown_block_flags),
            "max_notional_per_category_usd": self.max_notional_per_category_usd,
            "max_notional_per_market_usd": self.max_notional_per_market_usd,
            "max_total_notional_usd": self.max_total_notional_usd,
            "min_notional_usd": self.min_notional_usd,
            "performance_top1_cap": self.performance_top1_cap,
            "performance_top3_cap": self.performance_top3_cap,
            "top1_share_cap": self.top1_share_cap,
            "top3_share_cap": self.top3_share_cap,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_risk_from_trade_plan_config(
        artifacts_root=args.artifacts_root,
        max_total_notional_usd=args.max_total_notional_usd,
        max_notional_per_market_usd=args.max_notional_per_market_usd,
        max_notional_per_category_usd=args.max_notional_per_category_usd,
        top1_share_cap=args.top1_share_cap,
        top3_share_cap=args.top3_share_cap,
        performance_top1_cap=args.performance_top1_cap,
        performance_top3_cap=args.performance_top3_cap,
        allow_downsize=args.allow_downsize,
        min_notional_usd=args.min_notional_usd,
        blocking_quality_flags=args.blocking_quality_flags,
        cooldown_block_flags=args.cooldown_block_flags,
    )
    run_risk_from_trade_plan(
        trade_plan_artifact_path=Path(args.trade_plan_artifact),
        config=config,
        performance_artifact_path=(
            None if args.performance_artifact is None else Path(args.performance_artifact)
        ),
        hooks_json_path=None if args.hooks_json is None else Path(args.hooks_json),
        nonce=args.nonce,
    )
    return 0


def run_risk_from_trade_plan(
    *,
    trade_plan_artifact_path: Path,
    config: RiskFromTradePlanConfig,
    performance_artifact_path: Path | None = None,
    hooks_json_path: Path | None = None,
    nonce: str | None = None,
) -> dict[str, Any]:
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    trade_plan_artifact = _load_json_object(trade_plan_artifact_path, label="Trade-plan artifact")
    trade_plan_errors = validate_trade_plan_artifact(trade_plan_artifact)
    if trade_plan_errors:
        raise ValueError(f"Invalid trade-plan artifact: {trade_plan_errors[0]}")

    performance_artifact = _load_optional_performance_artifact(performance_artifact_path)
    hooks = _load_risk_hooks(hooks_json_path)

    input_trade_plan_hash = _resolve_payload_hash(
        trade_plan_artifact,
        primary_field="trade_plan_payload_hash",
    )
    input_performance_hash = (
        None
        if performance_artifact is None
        else _resolve_payload_hash(
            performance_artifact,
            primary_field="performance_payload_hash",
        )
    )

    policy_config = RiskPolicyConfig(
        max_total_notional_usd=config.max_total_notional_usd,
        max_notional_per_market_usd=config.max_notional_per_market_usd,
        max_notional_per_category_usd=config.max_notional_per_category_usd,
        top1_share_cap=config.top1_share_cap,
        top3_share_cap=config.top3_share_cap,
        performance_top1_cap=config.performance_top1_cap,
        performance_top3_cap=config.performance_top3_cap,
        allow_downsize=config.allow_downsize,
        min_notional_usd=config.min_notional_usd,
        blocking_quality_flags=config.blocking_quality_flags,
        cooldown_block_flags=config.cooldown_block_flags,
    )
    started_at = _resolve_started_at(trade_plan_artifact)
    deterministic_nonce = nonce or canonical_hash(
        {
            "input_trade_plan_artifact_hash": input_trade_plan_hash,
            "input_performance_artifact_hash": input_performance_hash,
            "hooks": hooks_json_path.name if hooks_json_path is not None else None,
            "params": policy_config.as_hash_dict(),
            "policy_version": RISK_POLICY_VERSION,
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "input_trade_plan_artifact_hash": input_trade_plan_hash,
            "input_performance_artifact_hash": input_performance_hash,
            "hooks": hooks_json_path.name if hooks_json_path is not None else None,
            "params": policy_config.as_hash_dict(),
            "policy_version": RISK_POLICY_VERSION,
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "risk_from_trade_plan_started",
        run_context,
        trade_plan_artifact_path=str(trade_plan_artifact_path),
        performance_artifact_path=(
            None if performance_artifact_path is None else str(performance_artifact_path)
        ),
        hooks_json_path=None if hooks_json_path is None else str(hooks_json_path),
        input_trade_plan_hash=input_trade_plan_hash,
        input_performance_hash=input_performance_hash,
    )

    result = evaluate_risk_policy(
        trade_plan_artifact,
        policy_config,
        performance_artifact=performance_artifact,
        hooks=hooks,
    )
    artifact = build_risk_artifact(
        run_context=run_context,
        trade_plan_artifact=trade_plan_artifact,
        performance_artifact=performance_artifact,
        params=policy_config.as_hash_dict(),
        items=result.items,
        counts=result.counts,
        notional_summary=result.notional_summary,
        quality_flags=result.quality_flags,
        quality_warnings=result.quality_warnings,
        policy_version=RISK_POLICY_VERSION,
    )
    artifact_errors = validate_risk_artifact(artifact)
    if artifact_errors:
        raise ValueError(f"Invalid risk artifact: {artifact_errors[0]}")

    artifact_path = _write_artifact(
        artifacts_root=config.artifacts_root,
        run_id=run_context.run_id,
        artifact=artifact,
    )
    _log(
        logger,
        logging.INFO,
        "risk_from_trade_plan_completed",
        run_context,
        artifact_path=str(artifact_path),
        n_total=result.counts["n_total"],
        n_allow=result.counts["n_allow"],
        n_block=result.counts["n_block"],
        n_downsize=result.counts["n_downsize"],
        policy_hash=str(artifact["policy_hash"]),
        items_hash=str(artifact["items_hash"]),
        risk_payload_hash=str(artifact["risk_payload_hash"]),
    )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def load_risk_from_trade_plan_config(
    *,
    artifacts_root: str | None,
    max_total_notional_usd: float | None,
    max_notional_per_market_usd: float | None,
    max_notional_per_category_usd: float | None,
    top1_share_cap: float | None,
    top3_share_cap: float | None,
    performance_top1_cap: float | None,
    performance_top3_cap: float | None,
    allow_downsize: bool | None,
    min_notional_usd: float | None,
    blocking_quality_flags: str | None,
    cooldown_block_flags: str | None,
) -> RiskFromTradePlanConfig:
    defaults = RiskPolicyConfig()
    resolved_root = artifacts_root or os.getenv("RISK_ARTIFACTS_ROOT") or "artifacts"
    resolved_allow_downsize = (
        allow_downsize if allow_downsize is not None else _load_bool("RISK_ALLOW_DOWNSIZE", True)
    )
    resolved_blocking_flags = _parse_csv_flags(
        blocking_quality_flags
        or os.getenv("RISK_BLOCKING_QUALITY_FLAGS")
        or ",".join(defaults.blocking_quality_flags)
    )
    resolved_cooldown_flags = _parse_csv_flags(
        cooldown_block_flags
        or os.getenv("RISK_COOLDOWN_BLOCK_FLAGS")
        or ",".join(defaults.cooldown_block_flags)
    )
    return RiskFromTradePlanConfig(
        artifacts_root=resolved_root,
        max_total_notional_usd=_load_positive_float(
            direct=max_total_notional_usd,
            env_name="RISK_MAX_TOTAL_NOTIONAL_USD",
            default=defaults.max_total_notional_usd,
        ),
        max_notional_per_market_usd=_load_positive_float(
            direct=max_notional_per_market_usd,
            env_name="RISK_MAX_NOTIONAL_PER_MARKET_USD",
            default=defaults.max_notional_per_market_usd,
        ),
        max_notional_per_category_usd=_load_positive_float(
            direct=max_notional_per_category_usd,
            env_name="RISK_MAX_NOTIONAL_PER_CATEGORY_USD",
            default=defaults.max_notional_per_category_usd,
        ),
        top1_share_cap=_load_ratio(
            direct=top1_share_cap,
            env_name="RISK_TOP1_SHARE_CAP",
            default=defaults.top1_share_cap,
        ),
        top3_share_cap=_load_ratio(
            direct=top3_share_cap,
            env_name="RISK_TOP3_SHARE_CAP",
            default=defaults.top3_share_cap,
        ),
        performance_top1_cap=_load_ratio(
            direct=performance_top1_cap,
            env_name="RISK_PERFORMANCE_TOP1_CAP",
            default=defaults.performance_top1_cap,
        ),
        performance_top3_cap=_load_ratio(
            direct=performance_top3_cap,
            env_name="RISK_PERFORMANCE_TOP3_CAP",
            default=defaults.performance_top3_cap,
        ),
        allow_downsize=resolved_allow_downsize,
        min_notional_usd=_load_positive_float(
            direct=min_notional_usd,
            env_name="RISK_MIN_NOTIONAL_USD",
            default=defaults.min_notional_usd,
        ),
        blocking_quality_flags=resolved_blocking_flags,
        cooldown_block_flags=resolved_cooldown_flags,
    )


def _load_optional_performance_artifact(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    payload = _load_json_object(path, label="Performance artifact")
    errors = validate_performance_report_artifact(payload)
    if errors:
        raise ValueError(f"Invalid performance artifact: {errors[0]}")
    return payload


def _load_risk_hooks(path: Path | None) -> RiskHooks | None:
    if path is None:
        return None
    payload = _load_json_object(path, label="Risk hooks")
    return RiskHooks.from_mapping(payload)


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"{label} root must be a JSON object: {path}")
    return raw


def _resolve_payload_hash(artifact: Mapping[str, Any], *, primary_field: str) -> str:
    value = _optional_text(artifact.get(primary_field))
    if value is not None and _is_sha256(value):
        return value
    return canonical_hash(artifact)


def _resolve_started_at(trade_plan_artifact: Mapping[str, Any]) -> datetime:
    parsed = _parse_optional_datetime(_optional_text(trade_plan_artifact.get("generated_at_utc")))
    if parsed is not None:
        return parsed
    return datetime.now(UTC)


def _write_artifact(*, artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root) / "risks"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic risk artifact from trade-plan artifact."
    )
    parser.add_argument(
        "--trade-plan-artifact",
        required=True,
        help="Path to trade-plan artifact JSON file.",
    )
    parser.add_argument(
        "--performance-artifact",
        default=None,
        help="Optional performance report artifact used for concentration guards.",
    )
    parser.add_argument(
        "--hooks-json",
        default=None,
        help="Optional risk hooks JSON (current exposure + cooldown state).",
    )
    parser.add_argument("--max-total-notional-usd", type=float, default=None)
    parser.add_argument("--max-notional-per-market-usd", type=float, default=None)
    parser.add_argument("--max-notional-per-category-usd", type=float, default=None)
    parser.add_argument("--top1-share-cap", type=float, default=None)
    parser.add_argument("--top3-share-cap", type=float, default=None)
    parser.add_argument("--performance-top1-cap", type=float, default=None)
    parser.add_argument("--performance-top3-cap", type=float, default=None)
    parser.add_argument(
        "--allow-downsize",
        dest="allow_downsize",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--no-allow-downsize",
        dest="allow_downsize",
        action="store_false",
    )
    parser.add_argument("--min-notional-usd", type=float, default=None)
    parser.add_argument(
        "--blocking-quality-flags",
        default=None,
        help="Optional comma-separated override for blocking quality flags.",
    )
    parser.add_argument(
        "--cooldown-block-flags",
        default=None,
        help="Optional comma-separated override for cooldown blocking flags.",
    )
    parser.add_argument(
        "--artifacts-root",
        default="artifacts",
        help="Artifacts root directory. Output is written under <root>/risks.",
    )
    parser.add_argument(
        "--nonce",
        default=None,
        help="Optional nonce override for deterministic run_id generation.",
    )
    return parser.parse_args(argv)


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


def _load_positive_float(*, direct: float | None, env_name: str, default: float) -> float:
    if direct is not None:
        value = direct
    else:
        raw = os.getenv(env_name)
        if raw is None or raw.strip() == "":
            value = default
        else:
            try:
                value = float(raw)
            except ValueError as exc:
                raise ValueError(f"{env_name} must be a number, got {raw!r}") from exc
    if value <= 0.0:
        raise ValueError(f"{env_name} must be > 0, got {value}")
    return value


def _load_ratio(*, direct: float | None, env_name: str, default: float) -> float:
    if direct is not None:
        value = direct
    else:
        raw = os.getenv(env_name)
        if raw is None or raw.strip() == "":
            value = default
        else:
            try:
                value = float(raw)
            except ValueError as exc:
                raise ValueError(f"{env_name} must be a number, got {raw!r}") from exc
    if not 0.0 < value <= 1.0:
        raise ValueError(f"{env_name} must be in (0,1], got {value}")
    return value


def _load_bool(env_name: str, default: bool) -> bool:
    raw = os.getenv(env_name)
    if raw is None or raw.strip() == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{env_name} must be boolean-like, got {raw!r}")


def _parse_csv_flags(raw: str) -> tuple[str, ...]:
    parts = [value.strip() for value in raw.split(",")]
    values = sorted({value for value in parts if value})
    if not values:
        raise ValueError("Flag list cannot be empty")
    return tuple(values)


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
