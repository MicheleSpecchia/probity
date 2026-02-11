from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.db.feature_repository import FeatureRepository
from pmx.features.microstore import MicroFeatureStore
from pmx.features.spec_micro_v1 import FEATURE_SET_VERSION

JOB_NAME = "compute_micro_features"


@dataclass(frozen=True, slots=True)
class ComputeMicroFeaturesConfig:
    ingest_epsilon_seconds: int
    feature_set: str = FEATURE_SET_VERSION

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
            "feature_set": self.feature_set,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = load_compute_micro_features_config(epsilon_seconds=args.epsilon_seconds)
    decision_ts = _parse_optional_datetime_arg(args.decision_ts) or datetime.now(tz=UTC)

    stats = run_compute_micro_features(
        config=config,
        token_ids=_parse_token_ids_arg(args.token_ids),
        max_tokens=args.max_tokens,
        decision_ts=decision_ts,
    )
    return 0 if stats["token_errors"] == 0 else 1


def run_compute_micro_features(
    *,
    config: ComputeMicroFeaturesConfig,
    token_ids: list[str] | None,
    max_tokens: int | None,
    decision_ts: datetime,
) -> dict[str, int]:
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")
    if max_tokens is not None and max_tokens <= 0:
        raise ValueError("--max-tokens must be > 0")

    decision_ts_utc = _as_utc_datetime(decision_ts)
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "decision_ts": decision_ts_utc.isoformat(),
            "token_ids": token_ids or [],
            "max_tokens": max_tokens,
        },
        started_at=decision_ts_utc,
    )
    run_uuid = UUID(run_context.run_id)
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    _log(
        logger,
        logging.INFO,
        "compute_micro_features_started",
        run_context,
        decision_ts=decision_ts_utc.isoformat(),
        max_tokens=max_tokens,
        requested_tokens=len(token_ids or []),
    )

    stats = {
        "tokens_selected": 0,
        "tokens_processed": 0,
        "snapshots_upserted": 0,
        "token_errors": 0,
    }
    feature_store = MicroFeatureStore()

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = FeatureRepository(connection)
        repository.insert_run(
            run_id=run_uuid,
            run_type="compute_micro_features",
            decision_ts=decision_ts_utc,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )

        refs = repository.list_token_markets(token_ids=token_ids, max_tokens=max_tokens)
        refs_sorted = sorted(refs, key=lambda item: item.token_id)
        stats["tokens_selected"] = len(refs_sorted)

        for ref in refs_sorted:
            started = time.perf_counter()
            try:
                features = feature_store.compute_features(
                    connection,
                    ref.token_id,
                    decision_ts_utc,
                    config.ingest_epsilon_seconds,
                )
                repository.upsert_feature_snapshot(
                    run_id=run_uuid,
                    market_id=ref.market_id,
                    token_id=ref.token_id,
                    decision_ts=decision_ts_utc,
                    feature_set=config.feature_set,
                    features_json=features,
                    computed_at=decision_ts_utc,
                )
            except Exception as exc:
                stats["token_errors"] += 1
                _log(
                    logger,
                    logging.ERROR,
                    "compute_micro_features_token_failed",
                    run_context,
                    token_id=ref.token_id,
                    market_id=ref.market_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                continue

            stats["tokens_processed"] += 1
            stats["snapshots_upserted"] += 1
            latency_ms = int((time.perf_counter() - started) * 1000)
            _log(
                logger,
                logging.INFO,
                "compute_micro_features_token_processed",
                run_context,
                token_id=ref.token_id,
                market_id=ref.market_id,
                latency_ms=latency_ms,
            )

    _log(
        logger,
        logging.INFO,
        "compute_micro_features_completed",
        run_context,
        **stats,
    )
    return stats


def load_compute_micro_features_config(
    *,
    epsilon_seconds: int | None,
) -> ComputeMicroFeaturesConfig:
    if epsilon_seconds is not None:
        if epsilon_seconds <= 0:
            raise ValueError("--epsilon-seconds must be > 0")
        resolved_epsilon = epsilon_seconds
    else:
        resolved_epsilon = _load_positive_int("INGEST_EPSILON_SECONDS", 300)
    return ComputeMicroFeaturesConfig(
        ingest_epsilon_seconds=resolved_epsilon,
        feature_set=FEATURE_SET_VERSION,
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute as-of microstructure feature snapshots.")
    parser.add_argument(
        "--token-ids",
        default=None,
        help="Comma-separated token ids. If omitted, scans market_tokens deterministically.",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=None,
        help="Optional cap on selected tokens after deterministic ordering.",
    )
    parser.add_argument(
        "--decision-ts",
        default=None,
        help="ISO decision timestamp for as-of computation (default: now UTC).",
    )
    parser.add_argument(
        "--epsilon-seconds",
        type=int,
        default=None,
        help="As-of ingest epsilon in seconds (default: INGEST_EPSILON_SECONDS or 300).",
    )
    return parser.parse_args(argv)


def _parse_token_ids_arg(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    values = [token.strip() for token in raw.split(",")]
    deduped = sorted({token for token in values if token})
    return deduped or None


def _parse_optional_datetime_arg(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid --decision-ts value: {raw!r}") from exc
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
