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
from pmx.decisions.artifact import DECISION_POLICY_VERSION, build_decision_artifact
from pmx.decisions.canonical import canonical_hash
from pmx.decisions.policy import (
    DecisionPolicyConfig,
    RobustMode,
    decide_from_forecast_artifact,
)
from pmx.decisions.validate_artifact import validate_decision_artifact
from pmx.forecast.validate_artifact import validate_forecast_artifact

JOB_NAME = "decide_from_forecast"


@dataclass(frozen=True, slots=True)
class DecideFromForecastConfig:
    min_edge_bps: float
    robust_mode: RobustMode
    max_items: int
    artifacts_root: str

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "artifacts_root": self.artifacts_root,
            "max_items": self.max_items,
            "min_edge_bps": self.min_edge_bps,
            "robust_mode": self.robust_mode,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_decide_from_forecast_config(
        min_edge_bps=args.min_edge_bps,
        robust_mode=args.robust_mode,
        max_items=args.max_items,
        artifacts_root=args.artifacts_root,
    )
    run_decide_from_forecast(
        forecast_artifact_path=Path(args.forecast_artifact),
        config=config,
    )
    return 0


def run_decide_from_forecast(
    *,
    forecast_artifact_path: Path,
    config: DecideFromForecastConfig,
    nonce: str | None = None,
) -> dict[str, Any]:
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    forecast_artifact = _load_forecast_artifact(forecast_artifact_path)
    forecast_errors = validate_forecast_artifact(forecast_artifact)
    if forecast_errors:
        raise ValueError(f"Invalid forecast artifact: {forecast_errors[0]}")

    started_at = _resolve_started_at(forecast_artifact)
    deterministic_nonce = nonce or canonical_hash(
        {
            "forecast_payload_hash": forecast_artifact.get("forecast_payload_hash"),
            "input_forecast_run_id": forecast_artifact.get("run_id"),
            "policy_version": DECISION_POLICY_VERSION,
            "params": config.as_hash_dict(),
        }
    )
    run_context = build_run_context(
        JOB_NAME,
        {
            "forecast_payload_hash": forecast_artifact.get("forecast_payload_hash"),
            "input_forecast_run_id": forecast_artifact.get("run_id"),
            "params": config.as_hash_dict(),
            "policy_version": DECISION_POLICY_VERSION,
        },
        started_at=started_at,
        nonce=deterministic_nonce,
    )
    _log(
        logger,
        logging.INFO,
        "decide_from_forecast_started",
        run_context,
        forecast_artifact_path=str(forecast_artifact_path),
    )

    policy_config = DecisionPolicyConfig(
        min_edge_bps=config.min_edge_bps,
        robust_mode=config.robust_mode,
        max_items=config.max_items,
    )
    items = decide_from_forecast_artifact(forecast_artifact, policy_config)
    artifact = build_decision_artifact(
        run_context=run_context,
        forecast_artifact=forecast_artifact,
        params=policy_config.as_hash_dict(),
        items=items,
        policy_version=DECISION_POLICY_VERSION,
    )
    counts = _decision_counts(items)
    artifact["counts"] = counts

    decision_errors = validate_decision_artifact(artifact)
    if decision_errors:
        raise ValueError(f"Invalid decision artifact: {decision_errors[0]}")

    artifact_path = _write_artifact(
        artifacts_root=config.artifacts_root,
        run_id=run_context.run_id,
        artifact=artifact,
    )
    _log(
        logger,
        logging.INFO,
        "decide_from_forecast_completed",
        run_context,
        artifact_path=str(artifact_path),
        n_total=counts["n_total"],
        n_trade=counts["n_trade"],
        n_no_trade=counts["n_no_trade"],
    )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def load_decide_from_forecast_config(
    *,
    min_edge_bps: float | None,
    robust_mode: str | None,
    max_items: int | None,
    artifacts_root: str | None,
) -> DecideFromForecastConfig:
    resolved_min_edge = (
        min_edge_bps if min_edge_bps is not None else _load_non_negative_float("MIN_EDGE_BPS", 50.0)
    )
    if resolved_min_edge < 0.0:
        raise ValueError("min_edge_bps must be >= 0")

    resolved_mode = _parse_robust_mode(
        robust_mode or os.getenv("DECISION_ROBUST_MODE") or "require_positive_low90"
    )

    resolved_max_items = (
        max_items if max_items is not None else _load_positive_int("MAX_ITEMS", 200)
    )
    if resolved_max_items <= 0:
        raise ValueError("max_items must be > 0")

    resolved_root = artifacts_root or os.getenv("DECISION_ARTIFACTS_ROOT") or "artifacts"
    return DecideFromForecastConfig(
        min_edge_bps=float(resolved_min_edge),
        robust_mode=resolved_mode,
        max_items=resolved_max_items,
        artifacts_root=resolved_root,
    )


def _load_forecast_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Forecast artifact not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("Forecast artifact root must be a JSON object")
    return raw


def _resolve_started_at(forecast_artifact: dict[str, Any]) -> datetime:
    top_level = _parse_optional_datetime(forecast_artifact.get("from_ts"))
    if top_level is not None:
        return top_level

    forecasts = forecast_artifact.get("forecasts")
    if isinstance(forecasts, list):
        candidates = [
            parsed
            for parsed in (_parse_optional_datetime(item.get("decision_ts")) for item in forecasts)
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


def _decision_counts(items: tuple[dict[str, Any], ...]) -> dict[str, int]:
    n_total = len(items)
    n_trade = sum(1 for item in items if item.get("action") in {"BUY_YES", "BUY_NO"})
    return {
        "n_total": n_total,
        "n_trade": n_trade,
        "n_no_trade": n_total - n_trade,
    }


def _write_artifact(artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root) / "decisions"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic decision artifact from a forecast artifact."
    )
    parser.add_argument(
        "--forecast-artifact",
        required=True,
        help="Path to forecast artifact JSON file.",
    )
    parser.add_argument(
        "--min-edge-bps",
        type=float,
        default=None,
        help="Minimum absolute edge in basis points to consider trade actions.",
    )
    parser.add_argument(
        "--robust-mode",
        choices=["require_positive_low90", "require_negative_high90", "none"],
        default=None,
        help="Robust decision check mode for interval gating.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Maximum number of ranked decision items in output artifact.",
    )
    parser.add_argument(
        "--artifacts-root",
        default=None,
        help="Artifacts root directory. Output is written under <root>/decisions.",
    )
    return parser.parse_args(argv)


def _parse_robust_mode(raw: str) -> RobustMode:
    value = raw.strip()
    allowed = {"require_positive_low90", "require_negative_high90", "none"}
    if value not in allowed:
        raise ValueError(f"Unsupported robust_mode: {raw!r}")
    return cast(RobustMode, value)


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


def _load_non_negative_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if parsed < 0.0:
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
