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
from pmx.execution.artifact import EXECUTION_POLICY_VERSION, build_execution_artifact
from pmx.execution.canonical import canonical_hash
from pmx.execution.policy import ExecutionMode, ExecutionPolicyConfig, apply_execution_policy
from pmx.execution.validate_artifact import validate_execution_artifact
from pmx.trade_plan.validate_artifact import validate_trade_plan_artifact

JOB_NAME = "execute_trade_plan_stub"


@dataclass(frozen=True, slots=True)
class ExecuteTradePlanStubConfig:
    mode: ExecutionMode
    max_orders: int | None
    simulate_reject_modulo: int
    simulate_reject_remainder: int
    artifacts_root: str

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "artifacts_root": self.artifacts_root,
            "max_orders": self.max_orders,
            "mode": self.mode,
            "simulate_reject_modulo": self.simulate_reject_modulo,
            "simulate_reject_remainder": self.simulate_reject_remainder,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_execute_trade_plan_stub_config(
        mode=args.mode,
        max_orders=args.max_orders,
        simulate_reject_modulo=args.simulate_reject_modulo,
        simulate_reject_remainder=args.simulate_reject_remainder,
        artifacts_root=args.artifacts_root,
    )
    run_execute_trade_plan_stub(
        trade_plan_artifact_path=Path(args.trade_plan_artifact),
        config=config,
        nonce=args.nonce,
    )
    return 0


def run_execute_trade_plan_stub(
    *,
    trade_plan_artifact_path: Path,
    config: ExecuteTradePlanStubConfig,
    nonce: str | None = None,
) -> dict[str, Any]:
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    trade_plan_artifact = _load_trade_plan_artifact(trade_plan_artifact_path)
    trade_plan_errors = validate_trade_plan_artifact(trade_plan_artifact)
    if trade_plan_errors:
        raise ValueError(f"Invalid trade-plan artifact: {trade_plan_errors[0]}")

    input_trade_plan_hash = _resolve_input_trade_plan_hash(trade_plan_artifact)
    resolved_max_orders = _resolve_max_orders(trade_plan_artifact, config.max_orders)
    started_at = _resolve_started_at(trade_plan_artifact)
    policy_config = ExecutionPolicyConfig(
        mode=config.mode,
        max_orders=resolved_max_orders,
        simulate_reject_modulo=config.simulate_reject_modulo,
        simulate_reject_remainder=config.simulate_reject_remainder,
    )

    deterministic_nonce = nonce or canonical_hash(
        {
            "input_trade_plan_artifact_hash": input_trade_plan_hash,
            "input_trade_plan_run_id": trade_plan_artifact.get("run_id"),
            "policy_version": EXECUTION_POLICY_VERSION,
            "params": policy_config.as_hash_dict(),
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "input_trade_plan_artifact_hash": input_trade_plan_hash,
            "input_trade_plan_run_id": trade_plan_artifact.get("run_id"),
            "policy_version": EXECUTION_POLICY_VERSION,
            "params": policy_config.as_hash_dict(),
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "execute_trade_plan_stub_started",
        run_context,
        trade_plan_artifact_path=str(trade_plan_artifact_path),
        input_trade_plan_hash=input_trade_plan_hash,
    )

    execution_result = apply_execution_policy(
        trade_plan_artifact=trade_plan_artifact,
        config=policy_config,
    )
    artifact = build_execution_artifact(
        run_context=run_context,
        trade_plan_artifact=trade_plan_artifact,
        params=policy_config.as_hash_dict(),
        idempotency_key=execution_result.idempotency_key,
        orders=execution_result.orders,
        skipped=execution_result.skipped,
        policy_version=EXECUTION_POLICY_VERSION,
        input_trade_plan_artifact_hash=input_trade_plan_hash,
    )
    artifact["counts"] = execution_result.counts

    artifact_errors = validate_execution_artifact(artifact)
    if artifact_errors:
        raise ValueError(f"Invalid execution artifact: {artifact_errors[0]}")

    artifact_path = _write_artifact(config.artifacts_root, run_context.run_id, artifact)
    _log(
        logger,
        logging.INFO,
        "execute_trade_plan_stub_completed",
        run_context,
        artifact_path=str(artifact_path),
        input_trade_plan_hash=input_trade_plan_hash,
        execution_policy_hash=str(artifact["execution_policy_hash"]),
        execution_payload_hash=str(artifact["execution_payload_hash"]),
        orders_hash=str(artifact["orders_hash"]),
        n_total=execution_result.counts["n_total"],
        n_orders=execution_result.counts["n_orders"],
        n_skipped=execution_result.counts["n_skipped"],
    )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def load_execute_trade_plan_stub_config(
    *,
    mode: str | None,
    max_orders: int | None,
    simulate_reject_modulo: int | None,
    simulate_reject_remainder: int | None,
    artifacts_root: str | None,
) -> ExecuteTradePlanStubConfig:
    resolved_mode = _parse_mode(mode or os.getenv("EXECUTION_MODE") or "simulate_submit")
    resolved_max_orders = (
        max_orders if max_orders is not None else _load_optional_positive_int("MAX_ORDERS")
    )
    resolved_reject_modulo = (
        simulate_reject_modulo
        if simulate_reject_modulo is not None
        else _load_non_negative_int("SIMULATE_REJECT_MODULO", 0)
    )
    resolved_reject_remainder = (
        simulate_reject_remainder
        if simulate_reject_remainder is not None
        else _load_non_negative_int("SIMULATE_REJECT_REMAINDER", 0)
    )
    resolved_root = artifacts_root or os.getenv("EXECUTION_ARTIFACTS_ROOT") or "artifacts"

    return ExecuteTradePlanStubConfig(
        mode=resolved_mode,
        max_orders=resolved_max_orders,
        simulate_reject_modulo=resolved_reject_modulo,
        simulate_reject_remainder=resolved_reject_remainder,
        artifacts_root=resolved_root,
    )


def _load_trade_plan_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Trade-plan artifact not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("Trade-plan artifact root must be a JSON object")
    return raw


def _resolve_max_orders(trade_plan_artifact: dict[str, Any], configured: int | None) -> int:
    if configured is not None:
        if configured <= 0:
            raise ValueError("max_orders must be > 0")
        return configured
    params_raw = trade_plan_artifact.get("params")
    if isinstance(params_raw, dict):
        max_orders_raw = params_raw.get("max_orders")
        if max_orders_raw is not None:
            try:
                parsed = int(max_orders_raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("trade-plan params.max_orders must be an integer") from exc
            if parsed <= 0:
                raise ValueError("trade-plan params.max_orders must be > 0")
            return parsed
    return 200


def _resolve_started_at(trade_plan_artifact: dict[str, Any]) -> datetime:
    raw = trade_plan_artifact.get("generated_at_utc")
    if isinstance(raw, str):
        parsed = _parse_datetime(raw)
        if parsed is not None:
            return parsed
    return datetime.now(UTC)


def _parse_datetime(raw: str) -> datetime | None:
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


def _resolve_input_trade_plan_hash(trade_plan_artifact: dict[str, Any]) -> str:
    value = trade_plan_artifact.get("trade_plan_payload_hash")
    if isinstance(value, str):
        normalized = value.strip()
        if len(normalized) == 64 and all(char in "0123456789abcdef" for char in normalized):
            return normalized
    return canonical_hash(trade_plan_artifact)


def _write_artifact(artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root) / "executions"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic execution artifact from a trade-plan artifact."
    )
    parser.add_argument(
        "--trade-plan-artifact",
        required=True,
        help="Path to trade-plan artifact JSON file.",
    )
    parser.add_argument(
        "--artifacts-root",
        default="artifacts",
        help="Artifacts root directory. Output is written under <root>/executions.",
    )
    parser.add_argument(
        "--mode",
        choices=["dry_run", "simulate_submit"],
        default="simulate_submit",
    )
    parser.add_argument(
        "--max-orders",
        type=int,
        default=None,
        help="Override maximum executable orders. Default: artifact params.max_orders or 200.",
    )
    parser.add_argument("--simulate-reject-modulo", type=int, default=0)
    parser.add_argument("--simulate-reject-remainder", type=int, default=0)
    parser.add_argument(
        "--nonce",
        default=None,
        help="Optional nonce override for deterministic run_id generation.",
    )
    return parser.parse_args(argv)


def _parse_mode(raw: str) -> ExecutionMode:
    value = raw.strip()
    allowed = {"dry_run", "simulate_submit"}
    if value not in allowed:
        raise ValueError(f"Unsupported execution mode: {raw!r}")
    return cast(ExecutionMode, value)


def _load_optional_positive_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be > 0, got {parsed}")
    return parsed


def _load_non_negative_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be >= 0, got {parsed}")
    return parsed


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
