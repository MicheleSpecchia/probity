from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.backtest.asof_dataset import build_asof_dataset
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.forecast.pipeline import run_forecast_pipeline

JOB_NAME = "forecast_baseline_ensemble"
MODEL_VERSION = "forecast_baseline_ensemble_v1"


@dataclass(frozen=True, slots=True)
class ForecastBaselineEnsembleConfig:
    feature_set: str
    ingest_epsilon_seconds: int
    artifacts_root: str
    min_isotonic_samples: int
    min_conformal_samples: int
    driver_top_k: int
    calibration_n_bins: int
    calibration_min_eval: int
    calibration_ece_threshold: float
    model_version: str = MODEL_VERSION

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "feature_set": self.feature_set,
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
            "artifacts_root": self.artifacts_root,
            "min_isotonic_samples": self.min_isotonic_samples,
            "min_conformal_samples": self.min_conformal_samples,
            "driver_top_k": self.driver_top_k,
            "calibration_n_bins": self.calibration_n_bins,
            "calibration_min_eval": self.calibration_min_eval,
            "calibration_ece_threshold": self.calibration_ece_threshold,
            "model_version": self.model_version,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_forecast_baseline_ensemble_config(
        feature_set=args.feature_set,
        epsilon_seconds=args.epsilon_seconds,
        artifacts_root=args.artifacts_root,
        min_isotonic_samples=args.min_isotonic_samples,
        min_conformal_samples=args.min_conformal_samples,
        driver_top_k=args.driver_top_k,
        calibration_n_bins=args.calibration_n_bins,
        calibration_min_eval=args.calibration_min_eval,
        calibration_ece_threshold=args.calibration_ece_threshold,
    )
    from_ts = _parse_required_datetime_arg(args.from_ts, "--from")
    to_ts = _parse_required_datetime_arg(args.to_ts, "--to")
    if to_ts < from_ts:
        raise ValueError("--to must be >= --from")
    if args.step_hours <= 0:
        raise ValueError("--step-hours must be > 0")

    run_forecast_baseline_ensemble(
        config=config,
        token_ids=_parse_token_ids_arg(args.token_ids),
        max_tokens=args.max_tokens,
        from_ts=from_ts,
        to_ts=to_ts,
        step_hours=args.step_hours,
    )
    return 0


def run_forecast_baseline_ensemble(
    *,
    config: ForecastBaselineEnsembleConfig,
    token_ids: list[str] | None,
    max_tokens: int | None,
    from_ts: datetime,
    to_ts: datetime,
    step_hours: int,
    nonce: str | None = None,
) -> dict[str, Any]:
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")
    if max_tokens is not None and max_tokens <= 0:
        raise ValueError("--max-tokens must be > 0")
    if step_hours <= 0:
        raise ValueError("--step-hours must be > 0")

    from_utc = _as_utc_datetime(from_ts)
    to_utc = _as_utc_datetime(to_ts)
    decision_ts_list = _build_decision_grid(from_utc, to_utc, step_hours=step_hours)

    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "token_ids": token_ids or [],
            "max_tokens": max_tokens,
            "from_ts": from_utc.isoformat(),
            "to_ts": to_utc.isoformat(),
            "step_hours": step_hours,
        },
        started_at=from_utc,
        nonce=nonce,
    )
    run_uuid = UUID(run_context.run_id)
    _log(
        logger,
        logging.INFO,
        "forecast_baseline_ensemble_started",
        run_context,
        from_ts=from_utc.isoformat(),
        to_ts=to_utc.isoformat(),
        step_hours=step_hours,
        max_tokens=max_tokens,
    )

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        _insert_run(
            connection,
            run_id=run_uuid,
            run_type=JOB_NAME,
            decision_ts=from_utc,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )
        selected_tokens = _resolve_tokens(
            connection,
            explicit_token_ids=token_ids,
            max_tokens=max_tokens,
        )
        dataset = build_asof_dataset(
            connection,
            token_ids=selected_tokens,
            decision_ts_list=decision_ts_list,
            epsilon_s=config.ingest_epsilon_seconds,
            feature_set=config.feature_set,
            outcome_provider=None,
        )

    pipeline = run_forecast_pipeline(
        dataset.examples,
        min_isotonic_samples=config.min_isotonic_samples,
        min_conformal_samples=config.min_conformal_samples,
        driver_top_k=config.driver_top_k,
        calibration_n_bins=config.calibration_n_bins,
        calibration_min_eval=config.calibration_min_eval,
        calibration_ece_threshold=config.calibration_ece_threshold,
    )
    artifact = {
        "run_id": run_context.run_id,
        "job_name": JOB_NAME,
        "model_version": config.model_version,
        "code_version": run_context.code_version,
        "config_hash": run_context.config_hash,
        "feature_set": config.feature_set,
        "from_ts": from_utc.isoformat(),
        "to_ts": to_utc.isoformat(),
        "step_hours": step_hours,
        "dataset_hash": pipeline.dataset_hash,
        "model_hash": pipeline.model_hash,
        "calibration_hash": pipeline.calibration_hash,
        "uncertainty_hash": pipeline.uncertainty_hash,
        "forecast_payload_hash": pipeline.forecast_payload_hash,
        "counts": {
            "tokens_selected": len(selected_tokens),
            "examples": pipeline.example_count,
            "forecasts": len(pipeline.forecasts),
            "skipped_no_outcome": dataset.skipped_no_outcome,
            "skipped_missing_features": dataset.skipped_missing_features,
            "skipped_missing_price": dataset.skipped_missing_price,
        },
        "metrics": pipeline.metrics,
        "interval_report": pipeline.interval_report,
        "calibration_report": pipeline.calibration_report,
        "quality_flags": list(pipeline.quality_flags),
        "quality_warnings": list(pipeline.quality_warnings),
        "calibration_windows": [window.as_dict() for window in pipeline.calibration_windows],
        "forecasts": [record.as_dict() for record in pipeline.forecasts],
    }
    artifact_path = _write_artifact(config.artifacts_root, run_context.run_id, artifact)
    _log(
        logger,
        logging.INFO,
        "forecast_baseline_ensemble_completed",
        run_context,
        artifact_path=str(artifact_path),
        forecasts=len(pipeline.forecasts),
        brier_raw=pipeline.metrics["raw"].get("brier", 0.0),
        brier_cal=pipeline.metrics["calibrated"].get("brier", 0.0),
        quality_flags=list(pipeline.quality_flags),
    )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def load_forecast_baseline_ensemble_config(
    *,
    feature_set: str | None,
    epsilon_seconds: int | None,
    artifacts_root: str | None,
    min_isotonic_samples: int | None,
    min_conformal_samples: int | None,
    driver_top_k: int | None,
    calibration_n_bins: int | None,
    calibration_min_eval: int | None,
    calibration_ece_threshold: float | None,
) -> ForecastBaselineEnsembleConfig:
    resolved_feature_set = feature_set or os.getenv("FORECAST_FEATURE_SET") or "micro_v1"
    if epsilon_seconds is not None:
        if epsilon_seconds <= 0:
            raise ValueError("--epsilon-seconds must be > 0")
        resolved_epsilon = epsilon_seconds
    else:
        resolved_epsilon = _load_positive_int("INGEST_EPSILON_SECONDS", 300)

    resolved_artifacts_root = (
        artifacts_root or os.getenv("FORECAST_ARTIFACTS_ROOT") or "artifacts/forecasts"
    )
    resolved_min_isotonic = _resolve_positive("min_isotonic_samples", min_isotonic_samples, 30)
    resolved_min_conformal = _resolve_positive("min_conformal_samples", min_conformal_samples, 20)
    resolved_driver_top_k = _resolve_positive("driver_top_k", driver_top_k, 5)
    resolved_calibration_n_bins = _resolve_positive("calibration_n_bins", calibration_n_bins, 10)
    resolved_calibration_min_eval = _resolve_positive(
        "calibration_min_eval",
        calibration_min_eval,
        40,
    )
    resolved_calibration_ece_threshold = _resolve_probability(
        "calibration_ece_threshold",
        calibration_ece_threshold,
        0.08,
    )
    return ForecastBaselineEnsembleConfig(
        feature_set=resolved_feature_set,
        ingest_epsilon_seconds=resolved_epsilon,
        artifacts_root=resolved_artifacts_root,
        min_isotonic_samples=resolved_min_isotonic,
        min_conformal_samples=resolved_min_conformal,
        driver_top_k=resolved_driver_top_k,
        calibration_n_bins=resolved_calibration_n_bins,
        calibration_min_eval=resolved_calibration_min_eval,
        calibration_ece_threshold=resolved_calibration_ece_threshold,
    )


def _resolve_positive(name: str, value: int | None, default: int) -> int:
    if value is None:
        return default
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def _resolve_probability(name: str, value: float | None, default: float) -> float:
    if value is None:
        return default
    if value < 0.0 or value > 1.0:
        raise ValueError(f"{name} must be in [0,1]")
    return float(value)


def _write_artifact(artifacts_root: str, run_id: str, artifact: dict[str, Any]) -> Path:
    root = Path(artifacts_root)
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"{run_id}.json"
    output_path.write_text(
        json.dumps(artifact, sort_keys=True, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return output_path


def _resolve_tokens(
    conn: psycopg.Connection,
    *,
    explicit_token_ids: list[str] | None,
    max_tokens: int | None,
) -> list[str]:
    if explicit_token_ids:
        return sorted({token for token in explicit_token_ids if token})

    if max_tokens is None:
        query = "SELECT token_id FROM market_tokens ORDER BY token_id ASC"
        params: tuple[object, ...] = ()
    else:
        query = "SELECT token_id FROM market_tokens ORDER BY token_id ASC LIMIT %s"
        params = (max_tokens,)

    with conn.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [str(row[0]) for row in rows]


def _insert_run(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    run_type: str,
    decision_ts: datetime,
    ingest_epsilon_seconds: int,
    code_version: str,
    config_hash: str,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO runs (
                run_id,
                run_type,
                decision_ts,
                ingest_epsilon_seconds,
                code_version,
                config_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                run_type,
                _as_utc_datetime(decision_ts),
                ingest_epsilon_seconds,
                code_version,
                config_hash,
            ),
        )


def _build_decision_grid(from_ts: datetime, to_ts: datetime, *, step_hours: int) -> list[datetime]:
    out: list[datetime] = []
    current = from_ts
    step = timedelta(hours=step_hours)
    while current <= to_ts:
        out.append(current)
        current = current + step
    return out


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run deterministic baseline ensemble forecast over an as-of walk-forward grid."
    )
    parser.add_argument("--token-ids", default=None, help="Comma-separated token ids.")
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=None,
        help="Optional max selected tokens.",
    )
    parser.add_argument("--from", dest="from_ts", required=True, help="Start ISO timestamp.")
    parser.add_argument("--to", dest="to_ts", required=True, help="End ISO timestamp.")
    parser.add_argument("--step-hours", type=int, default=4, help="Walk-forward step in hours.")
    parser.add_argument(
        "--epsilon-seconds",
        type=int,
        default=None,
        help="As-of ingest epsilon seconds.",
    )
    parser.add_argument("--feature-set", default="micro_v1", help="Feature set id.")
    parser.add_argument(
        "--artifacts-root",
        default=None,
        help="Forecast artifact directory (default artifacts/forecasts).",
    )
    parser.add_argument(
        "--min-isotonic-samples",
        type=int,
        default=30,
        help="Minimum train sample count to enable isotonic calibration.",
    )
    parser.add_argument(
        "--min-conformal-samples",
        type=int,
        default=20,
        help="Minimum calibration sample count for conformal intervals.",
    )
    parser.add_argument(
        "--driver-top-k",
        type=int,
        default=5,
        help="Max number of deterministic top drivers per forecast.",
    )
    parser.add_argument(
        "--calibration-n-bins",
        type=int,
        default=10,
        help="Number of deterministic calibration bins in artifact report.",
    )
    parser.add_argument(
        "--calibration-min-eval",
        type=int,
        default=40,
        help="Minimum eval samples before calibration gate passes.",
    )
    parser.add_argument(
        "--calibration-ece-threshold",
        type=float,
        default=0.08,
        help="ECE threshold for soft calibration quality gate.",
    )
    return parser.parse_args(argv)


def _parse_token_ids_arg(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    values = [item.strip() for item in raw.split(",")]
    deduped = sorted({item for item in values if item})
    return deduped or None


def _parse_required_datetime_arg(raw: str, flag: str) -> datetime:
    text = raw.strip()
    if not text:
        raise ValueError(f"{flag} cannot be empty")
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid {flag} value: {raw!r}") from exc
    return _as_utc_datetime(parsed)


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
