from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.db.clob_repository import ClobRepository, TokenIngestStats
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.ingest.clob_client import (
    DEFAULT_CLOB_BASE_URL,
    DEFAULT_CLOB_RATE_LIMIT_RPS,
    DEFAULT_CLOB_TIMEOUT_SECONDS,
    DEFAULT_DATA_API_BASE_URL,
    ClobClientConfig,
    ClobHttpError,
    ClobRestClient,
    TradeRecord,
)

JOB_NAME = "clob_ingest_rest"
_ALLOWED_INTERVALS = {"1m", "5m", "1h"}
_ALLOWED_INGESTED_AT_MODES = {"now", "event_ts"}


@dataclass(frozen=True, slots=True)
class ClobIngestRestConfig:
    clob_base_url: str
    data_api_base_url: str
    clob_timeout_seconds: int
    clob_rate_limit_rps: float
    ingest_epsilon_seconds: int
    clob_orderbook_depth: int | None = None

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "clob_base_url": self.clob_base_url,
            "data_api_base_url": self.data_api_base_url,
            "clob_timeout_seconds": self.clob_timeout_seconds,
            "clob_rate_limit_rps": self.clob_rate_limit_rps,
            "clob_orderbook_depth": self.clob_orderbook_depth,
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    since_ts = _parse_optional_datetime_arg(args.since_ts)
    if args.max_tokens is not None and args.max_tokens <= 0:
        raise ValueError("--max-tokens must be > 0")
    if args.ingest_latency_seconds < 0:
        raise ValueError("--ingest-latency-seconds must be >= 0")

    config = load_clob_ingest_rest_config()
    stats = run_clob_ingest_rest(
        config=config,
        max_tokens=args.max_tokens,
        since_ts=since_ts,
        interval=args.interval,
        ingested_at_mode=args.ingested_at_mode,
        ingest_latency_seconds=args.ingest_latency_seconds,
    )
    return 0 if stats["token_errors"] == 0 else 1


def run_clob_ingest_rest(
    *,
    config: ClobIngestRestConfig,
    max_tokens: int | None,
    since_ts: datetime | None,
    interval: str,
    ingested_at_mode: str = "now",
    ingest_latency_seconds: int = 0,
) -> dict[str, int]:
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")
    if interval not in _ALLOWED_INTERVALS:
        allowed = ", ".join(sorted(_ALLOWED_INTERVALS))
        raise ValueError(f"Unsupported interval {interval!r}. Allowed: {allowed}")
    if ingested_at_mode not in _ALLOWED_INGESTED_AT_MODES:
        allowed_modes = ", ".join(sorted(_ALLOWED_INGESTED_AT_MODES))
        raise ValueError(
            f"Unsupported ingested_at_mode {ingested_at_mode!r}. Allowed: {allowed_modes}"
        )
    if ingest_latency_seconds < 0:
        raise ValueError("ingest_latency_seconds must be >= 0")

    started_at = datetime.now(tz=UTC)
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "max_tokens": max_tokens,
            "since_ts": since_ts.isoformat() if since_ts else None,
            "interval": interval,
            "ingested_at_mode": ingested_at_mode,
            "ingest_latency_seconds": ingest_latency_seconds,
        },
        started_at=started_at,
    )
    run_uuid = UUID(run_context.run_id)

    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    _log(
        logger,
        logging.INFO,
        "clob_ingest_rest_started",
        run_context,
        since_ts=since_ts.isoformat() if since_ts else None,
        max_tokens=max_tokens,
        interval=interval,
        ingested_at_mode=ingested_at_mode,
        ingest_latency_seconds=ingest_latency_seconds,
    )

    client = ClobRestClient(
        ClobClientConfig(
            base_url=config.clob_base_url,
            data_api_base_url=config.data_api_base_url,
            timeout_seconds=config.clob_timeout_seconds,
            rate_limit_rps=config.clob_rate_limit_rps,
            api_key=os.getenv("CLOB_API_KEY"),
            api_secret=os.getenv("CLOB_API_SECRET"),
            api_passphrase=os.getenv("CLOB_API_PASSPHRASE"),
            poly_address=os.getenv("POLY_ADDRESS"),
            orderbook_depth=config.clob_orderbook_depth,
        )
    )

    stats = {
        "candles_upserted": 0,
        "snapshots_upserted": 0,
        "tokens_processed": 0,
        "token_errors": 0,
        "trades_upserted": 0,
    }

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = ClobRepository(connection)
        repository.insert_run(
            run_id=run_uuid,
            run_type="clob_rest_ingest",
            decision_ts=started_at,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )
        token_refs = repository.list_token_ingest_refs(max_tokens=max_tokens)

        for token_ref in token_refs:
            token_id = token_ref.token_id
            token_started = time.perf_counter()
            try:
                token_stats, skip_reasons = _process_token(
                    token_id=token_id,
                    condition_id=token_ref.condition_id,
                    interval=interval,
                    since_ts=since_ts,
                    run_ingested_at=started_at,
                    ingested_at_mode=ingested_at_mode,
                    ingest_latency_seconds=ingest_latency_seconds,
                    client=client,
                    repository=repository,
                )
            except Exception as exc:
                stats["token_errors"] += 1
                reason_code = _classify_failure_reason(exc)
                _log(
                    logger,
                    logging.ERROR,
                    "clob_token_failed",
                    run_context,
                    token_id=token_id,
                    reason_code=reason_code,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                continue

            stats["tokens_processed"] += 1
            stats["snapshots_upserted"] += token_stats.snapshots_upserted
            stats["trades_upserted"] += token_stats.trades_upserted
            stats["candles_upserted"] += token_stats.candles_upserted
            latency_ms = int((time.perf_counter() - token_started) * 1000)

            if skip_reasons:
                _log(
                    logger,
                    logging.WARNING,
                    "clob_token_data_missing",
                    run_context,
                    token_id=token_id,
                    skip_reasons=skip_reasons,
                )

            _log(
                logger,
                logging.INFO,
                "clob_token_processed",
                run_context,
                token_id=token_id,
                latency_ms=latency_ms,
                snapshots_upserted=token_stats.snapshots_upserted,
                trades_upserted=token_stats.trades_upserted,
                candles_upserted=token_stats.candles_upserted,
                skip_reasons=skip_reasons,
            )

    elapsed_seconds = int((datetime.now(tz=UTC) - started_at).total_seconds())
    _log(
        logger,
        logging.INFO,
        "clob_ingest_rest_completed",
        run_context,
        elapsed_seconds=elapsed_seconds,
        **stats,
    )
    return stats


def _process_token(
    *,
    token_id: str,
    condition_id: str | None,
    interval: str,
    since_ts: datetime | None,
    run_ingested_at: datetime,
    ingested_at_mode: str,
    ingest_latency_seconds: int,
    client: ClobRestClient,
    repository: ClobRepository,
) -> tuple[TokenIngestStats, list[str]]:
    skip_reasons: list[str] = []
    snapshot_count = 0
    trades_count = 0
    candles_count = 0

    try:
        snapshot = client.get_orderbook(token_id, fallback_event_ts=run_ingested_at)
    except ClobHttpError as exc:
        if exc.path == "/book" and exc.status_code == 404:
            snapshot = None
            skip_reasons.append("book_not_found")
        else:
            raise
    if snapshot is not None:
        snapshot_ingested_at = _resolve_ingested_at(
            event_ts=snapshot.event_ts,
            run_ingested_at=run_ingested_at,
            ingested_at_mode=ingested_at_mode,
            ingest_latency_seconds=ingest_latency_seconds,
        )
        repository.upsert_orderbook_snapshot(snapshot, ingested_at=snapshot_ingested_at)
        snapshot_count += 1

    try:
        trades = sorted(
            client.get_trades(token_id, since_ts=since_ts),
            key=_trade_sort_key,
        )
    except ClobHttpError as exc:
        if exc.path in {"/trades", "/data/trades"} and exc.status_code == 401:
            trades = []
            if _has_l2_auth_material(client):
                skip_reasons.append("trades_unauthorized")
            else:
                skip_reasons.append("trades_unauthorized_missing_l2_auth_material")
        elif exc.path in {"/trades", "/data/trades"} and exc.status_code == 404:
            trades = []
            skip_reasons.append("trades_not_found")
        else:
            raise

    if not trades and condition_id:
        try:
            fallback_trades = sorted(
                client.get_market_trades(
                    condition_id,
                    token_id=token_id,
                    since_ts=since_ts,
                ),
                key=_trade_sort_key,
            )
        except ClobHttpError as exc:
            if exc.path == "/trades" and exc.status_code == 404:
                fallback_trades = []
                skip_reasons.append("trades_public_not_found")
            elif exc.path == "/trades" and exc.status_code == 401:
                fallback_trades = []
                skip_reasons.append("trades_public_unauthorized")
            else:
                raise
        if fallback_trades:
            trades = fallback_trades
            skip_reasons = [reason for reason in skip_reasons if not reason.startswith("trades_")]

    for trade in trades:
        trade_ingested_at = _resolve_ingested_at(
            event_ts=trade.event_ts,
            run_ingested_at=run_ingested_at,
            ingested_at_mode=ingested_at_mode,
            ingest_latency_seconds=ingest_latency_seconds,
        )
        repository.upsert_trade(trade, ingested_at=trade_ingested_at)
        trades_count += 1

    try:
        candles = sorted(
            client.get_candles(token_id, interval=interval, since_ts=since_ts),
            key=lambda candle: candle.start_ts,
        )
    except ClobHttpError as exc:
        if exc.path == "/candles" and exc.status_code == 404:
            candles = []
            skip_reasons.append("candles_not_found")
        elif exc.path == "/candles" and exc.status_code == 401:
            candles = []
            skip_reasons.append("candles_unauthorized")
        else:
            raise
    for candle in candles:
        candle_ingested_at = _resolve_ingested_at(
            event_ts=candle.start_ts,
            run_ingested_at=run_ingested_at,
            ingested_at_mode=ingested_at_mode,
            ingest_latency_seconds=ingest_latency_seconds,
        )
        repository.upsert_candle(candle, ingested_at=candle_ingested_at)
        candles_count += 1

    stats = TokenIngestStats(
        token_id=token_id,
        snapshots_upserted=snapshot_count,
        trades_upserted=trades_count,
        candles_upserted=candles_count,
    )
    return stats, sorted(set(skip_reasons))


def load_clob_ingest_rest_config() -> ClobIngestRestConfig:
    clob_base_url = os.getenv("CLOB_BASE_URL", DEFAULT_CLOB_BASE_URL)
    data_api_base_url = os.getenv("CLOB_DATA_API_BASE_URL", DEFAULT_DATA_API_BASE_URL)
    clob_timeout_seconds = _load_positive_int("CLOB_TIMEOUT_SECONDS", DEFAULT_CLOB_TIMEOUT_SECONDS)
    clob_rate_limit_rps = _load_positive_float("CLOB_RATE_LIMIT_RPS", DEFAULT_CLOB_RATE_LIMIT_RPS)
    clob_orderbook_depth = _load_optional_positive_int("CLOB_ORDERBOOK_DEPTH")
    ingest_epsilon_seconds = _load_positive_int("INGEST_EPSILON_SECONDS", 300)

    return ClobIngestRestConfig(
        clob_base_url=clob_base_url,
        data_api_base_url=data_api_base_url,
        clob_timeout_seconds=clob_timeout_seconds,
        clob_rate_limit_rps=clob_rate_limit_rps,
        clob_orderbook_depth=clob_orderbook_depth,
        ingest_epsilon_seconds=ingest_epsilon_seconds,
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Polymarket CLOB REST market data.")
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=None,
        help="Optional cap on number of tokens to ingest, processed in sorted order.",
    )
    parser.add_argument(
        "--since-ts",
        dest="since_ts",
        help=(
            "Optional ISO datetime lower bound (inclusive) for "
            "trades.event_ts and candles.start_ts."
        ),
    )
    parser.add_argument(
        "--interval",
        choices=sorted(_ALLOWED_INTERVALS),
        default="1m",
        help="Candle interval to ingest.",
    )
    parser.add_argument(
        "--ingested-at-mode",
        choices=sorted(_ALLOWED_INGESTED_AT_MODES),
        default="now",
        help="Ingested-at assignment mode: now (default) or event_ts backfill mode.",
    )
    parser.add_argument(
        "--ingest-latency-seconds",
        type=int,
        default=0,
        help=(
            "Applied when --ingested-at-mode=event_ts. "
            "Sets ingested_at = event_ts + latency_seconds."
        ),
    )
    return parser.parse_args(argv)


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
        raise ValueError(f"Invalid --since-ts value: {raw!r}") from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
        raise ValueError(f"{name} must be a float, got {raw!r}") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be > 0, got {parsed}")
    return parsed


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


def _trade_sort_key(trade: TradeRecord) -> tuple[datetime, int, str]:
    return (
        trade.event_ts,
        trade.seq if trade.seq is not None else 0,
        trade.trade_hash or "",
    )


def _classify_failure_reason(exc: Exception) -> str:
    if isinstance(exc, ClobHttpError):
        path_key = exc.path.lstrip("/").replace("/", "_") or "root"
        return f"http_{exc.status_code}_{path_key}"
    return "unexpected_error"


def _has_l2_auth_material(client: ClobRestClient) -> bool:
    return bool(
        client.config.api_key
        and client.config.api_secret
        and client.config.api_passphrase
        and client.config.poly_address
    )


def _resolve_ingested_at(
    *,
    event_ts: datetime,
    run_ingested_at: datetime,
    ingested_at_mode: str,
    ingest_latency_seconds: int,
) -> datetime:
    if ingested_at_mode == "event_ts":
        return _as_utc_datetime(event_ts) + timedelta(seconds=ingest_latency_seconds)
    return _as_utc_datetime(run_ingested_at)


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


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
