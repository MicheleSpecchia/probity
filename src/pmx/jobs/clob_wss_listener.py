from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.db.clob_repository import ClobRepository
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.ingest.clob_client import (
    DEFAULT_CLOB_BASE_URL,
    DEFAULT_CLOB_RATE_LIMIT_RPS,
    DEFAULT_CLOB_TIMEOUT_SECONDS,
    ClobClientConfig,
    ClobRestClient,
    OrderbookSnapshot,
    TradeRecord,
    build_trade_hash,
    normalize_orderbook,
)
from pmx.ingest.clob_wss_client import (
    DEFAULT_CLOB_WSS_SEQ_FIELDS,
    DEFAULT_CLOB_WSS_TIMEOUT_SECONDS,
    DEFAULT_CLOB_WSS_URL,
    ClobReconnectEvent,
    ClobStreamEvent,
    ClobWssClient,
    ClobWssConfig,
)
from pmx.ingest.reconciler import (
    ClobReconciler,
    ReconcileStrategyConfig,
    StreamTokenState,
)

JOB_NAME = "clob_wss_listener"
_PRICE_QUANT = Decimal("0.00000001")
_SIZE_QUANT = Decimal("0.00000001")


@dataclass(frozen=True, slots=True)
class ClobWssListenerConfig:
    clob_base_url: str
    clob_timeout_seconds: int
    clob_rate_limit_rps: float
    clob_orderbook_depth: int | None
    clob_wss_url: str
    clob_wss_timeout_seconds: int
    clob_wss_max_reconnect_attempts: int
    clob_wss_backoff_seconds: float
    clob_wss_max_backoff_seconds: float
    clob_wss_seq_field: str | None
    clob_wss_seq_fields: tuple[str, ...]
    clob_reconcile_gap_seconds: int
    clob_reconcile_mismatch_bps: int
    ingest_epsilon_seconds: int

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "clob_base_url": self.clob_base_url,
            "clob_timeout_seconds": self.clob_timeout_seconds,
            "clob_rate_limit_rps": self.clob_rate_limit_rps,
            "clob_orderbook_depth": self.clob_orderbook_depth,
            "clob_wss_url": self.clob_wss_url,
            "clob_wss_timeout_seconds": self.clob_wss_timeout_seconds,
            "clob_wss_max_reconnect_attempts": self.clob_wss_max_reconnect_attempts,
            "clob_wss_backoff_seconds": self.clob_wss_backoff_seconds,
            "clob_wss_max_backoff_seconds": self.clob_wss_max_backoff_seconds,
            "clob_wss_seq_field": self.clob_wss_seq_field,
            "clob_wss_seq_fields": list(self.clob_wss_seq_fields),
            "clob_reconcile_gap_seconds": self.clob_reconcile_gap_seconds,
            "clob_reconcile_mismatch_bps": self.clob_reconcile_mismatch_bps,
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    since_ts = _parse_optional_datetime_arg(args.since_ts)

    config = load_clob_wss_listener_config()
    run_clob_wss_listener(
        config=config,
        max_tokens=args.max_tokens,
        token_ids=args.token_ids,
        reconcile_every_seconds=args.reconcile_every_seconds,
        since_ts=since_ts,
        run_seconds=args.run_seconds,
    )
    return 0


def run_clob_wss_listener(
    *,
    config: ClobWssListenerConfig,
    max_tokens: int | None,
    token_ids: list[str] | None,
    reconcile_every_seconds: int,
    since_ts: datetime | None,
    run_seconds: int | None,
) -> dict[str, int | float]:
    if reconcile_every_seconds <= 0:
        raise ValueError("--reconcile-every-seconds must be > 0")
    if run_seconds is not None and run_seconds <= 0:
        raise ValueError("--run-seconds must be > 0 when provided")

    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")

    started_at = datetime.now(tz=UTC)
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "max_tokens": max_tokens,
            "token_ids": token_ids,
            "reconcile_every_seconds": reconcile_every_seconds,
            "since_ts": since_ts.isoformat() if since_ts else None,
            "run_seconds": run_seconds,
        },
        started_at=started_at,
    )
    run_uuid = UUID(run_context.run_id)
    logger = get_logger(f"pmx.jobs.{JOB_NAME}")

    _log(
        logger,
        logging.INFO,
        "clob_wss_listener_started",
        run_context,
        max_tokens=max_tokens,
        token_ids=token_ids,
        reconcile_every_seconds=reconcile_every_seconds,
        since_ts=since_ts.isoformat() if since_ts else None,
        run_seconds=run_seconds,
    )

    stats: dict[str, int | float] = {
        "messages_received": 0,
        "messages_dropped": 0,
        "reconnects": 0,
        "trades_upserted": 0,
        "snapshots_upserted": 0,
        "reconcile_cycles": 0,
        "reconcile_gaps": 0,
        "reconcile_mismatches": 0,
        "repair_trades_upserted": 0,
        "repair_snapshots_upserted": 0,
        "msg_rate_per_sec": 0.0,
    }

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = ClobRepository(connection)
        repository.insert_run(
            run_id=run_uuid,
            run_type="clob_wss_listener",
            decision_ts=started_at,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )

        selected_token_ids = _resolve_token_ids(
            repository=repository,
            requested_token_ids=token_ids,
            max_tokens=max_tokens,
        )
        if not selected_token_ids:
            raise ValueError("No token ids available for WSS listener.")

        states = {token_id: StreamTokenState() for token_id in selected_token_ids}
        rest_client = ClobRestClient(
            ClobClientConfig(
                base_url=config.clob_base_url,
                timeout_seconds=config.clob_timeout_seconds,
                rate_limit_rps=config.clob_rate_limit_rps,
                api_key=os.getenv("CLOB_API_KEY"),
                orderbook_depth=config.clob_orderbook_depth,
            )
        )
        wss_client = ClobWssClient(
            ClobWssConfig(
                base_url=config.clob_wss_url,
                timeout_seconds=config.clob_wss_timeout_seconds,
                max_reconnect_attempts=config.clob_wss_max_reconnect_attempts,
                backoff_seconds=config.clob_wss_backoff_seconds,
                max_backoff_seconds=config.clob_wss_max_backoff_seconds,
                seq_field=config.clob_wss_seq_field,
                seq_fields=config.clob_wss_seq_fields,
            )
        )
        reconciler = ClobReconciler(
            rest_client=rest_client,
            repository=repository,
            logger=logger,
            run_context=run_context,
            since_ts=since_ts,
            strategy=ReconcileStrategyConfig(
                seq_mode_enabled=bool(config.clob_wss_seq_fields),
                gap_seconds=config.clob_reconcile_gap_seconds,
                mismatch_bps=config.clob_reconcile_mismatch_bps,
            ),
        )

        started_monotonic = time.monotonic()
        last_reconcile_monotonic = started_monotonic

        for item in wss_client.listen(selected_token_ids, run_seconds=run_seconds):
            if isinstance(item, ClobReconnectEvent):
                stats["reconnects"] += 1
                _log(
                    logger,
                    logging.WARNING,
                    "wss_reconnect",
                    run_context,
                    attempt=item.attempt,
                    delay_seconds=item.delay_seconds,
                    error_type=item.error_type,
                    error_message=item.error_message,
                )
                continue

            stats["messages_received"] += 1
            token_state = states.get(item.token_id)
            if token_state is None:
                stats["messages_dropped"] += 1
                continue

            handled = _handle_stream_event(
                event=item,
                token_state=token_state,
                repository=repository,
                ingested_at=started_at,
                orderbook_depth=config.clob_orderbook_depth,
                seq_mode_enabled=bool(config.clob_wss_seq_fields),
            )
            if handled == "trade":
                stats["trades_upserted"] += 1
            elif handled == "orderbook":
                stats["snapshots_upserted"] += 1
            else:
                stats["messages_dropped"] += 1

            now_monotonic = time.monotonic()
            if now_monotonic - last_reconcile_monotonic >= reconcile_every_seconds:
                _run_reconcile_cycle(
                    token_ids=selected_token_ids,
                    states=states,
                    reconciler=reconciler,
                    ingested_at=started_at,
                    stats=stats,
                    logger=logger,
                    run_context=run_context,
                )
                last_reconcile_monotonic = now_monotonic

        _run_reconcile_cycle(
            token_ids=selected_token_ids,
            states=states,
            reconciler=reconciler,
            ingested_at=started_at,
            stats=stats,
            logger=logger,
            run_context=run_context,
        )

        elapsed_seconds = max(time.monotonic() - started_monotonic, 0.001)
        stats["msg_rate_per_sec"] = round(float(stats["messages_received"]) / elapsed_seconds, 3)

    _log(
        logger,
        logging.INFO,
        "clob_wss_listener_completed",
        run_context,
        **stats,
    )
    return stats


def load_clob_wss_listener_config() -> ClobWssListenerConfig:
    clob_base_url = os.getenv("CLOB_BASE_URL", DEFAULT_CLOB_BASE_URL)
    clob_timeout_seconds = _load_positive_int("CLOB_TIMEOUT_SECONDS", DEFAULT_CLOB_TIMEOUT_SECONDS)
    clob_rate_limit_rps = _load_positive_float("CLOB_RATE_LIMIT_RPS", DEFAULT_CLOB_RATE_LIMIT_RPS)
    clob_orderbook_depth = _load_optional_positive_int("CLOB_ORDERBOOK_DEPTH")

    clob_wss_url = os.getenv("CLOB_WSS_URL", DEFAULT_CLOB_WSS_URL)
    clob_wss_timeout_seconds = _load_positive_int(
        "CLOB_WSS_TIMEOUT_SECONDS",
        DEFAULT_CLOB_WSS_TIMEOUT_SECONDS,
    )
    clob_wss_max_reconnect_attempts = _load_positive_int("CLOB_WSS_MAX_RECONNECTS", 8)
    clob_wss_backoff_seconds = _load_positive_float("CLOB_WSS_BACKOFF_SECONDS", 0.5)
    clob_wss_max_backoff_seconds = _load_positive_float("CLOB_WSS_MAX_BACKOFF_SECONDS", 30.0)
    clob_wss_seq_field, clob_wss_seq_fields = _load_wss_seq_config()
    clob_reconcile_gap_seconds = _load_positive_int("CLOB_RECONCILE_GAP_SECONDS", 60)
    clob_reconcile_mismatch_bps = _load_non_negative_int("CLOB_RECONCILE_MISMATCH_BPS", 10)
    ingest_epsilon_seconds = _load_positive_int("INGEST_EPSILON_SECONDS", 300)

    return ClobWssListenerConfig(
        clob_base_url=clob_base_url,
        clob_timeout_seconds=clob_timeout_seconds,
        clob_rate_limit_rps=clob_rate_limit_rps,
        clob_orderbook_depth=clob_orderbook_depth,
        clob_wss_url=clob_wss_url,
        clob_wss_timeout_seconds=clob_wss_timeout_seconds,
        clob_wss_max_reconnect_attempts=clob_wss_max_reconnect_attempts,
        clob_wss_backoff_seconds=clob_wss_backoff_seconds,
        clob_wss_max_backoff_seconds=clob_wss_max_backoff_seconds,
        clob_wss_seq_field=clob_wss_seq_field,
        clob_wss_seq_fields=clob_wss_seq_fields,
        clob_reconcile_gap_seconds=clob_reconcile_gap_seconds,
        clob_reconcile_mismatch_bps=clob_reconcile_mismatch_bps,
        ingest_epsilon_seconds=ingest_epsilon_seconds,
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Listen to Polymarket CLOB WSS with periodic REST reconcile."
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=None,
        help="Optional cap on token count when loading from market_tokens.",
    )
    parser.add_argument(
        "--token-ids",
        help=(
            "Optional comma-separated token ids. "
            "When provided, these take precedence over --max-tokens."
        ),
    )
    parser.add_argument(
        "--reconcile-every-seconds",
        type=int,
        default=60,
        help="Periodic reconcile cadence against REST source of truth.",
    )
    parser.add_argument(
        "--since-ts",
        dest="since_ts",
        default=None,
        help="Optional ISO lower bound for reconcile REST trades query (inclusive).",
    )
    parser.add_argument(
        "--run-seconds",
        type=int,
        default=None,
        help="Optional runtime cap for dev/test.",
    )
    parsed = parser.parse_args(argv)
    parsed.token_ids = _parse_token_ids_arg(parsed.token_ids)
    return parsed


def _parse_token_ids_arg(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    items = [part.strip() for part in raw.split(",")]
    token_ids = sorted({item for item in items if item})
    return token_ids if token_ids else None


def _resolve_token_ids(
    *,
    repository: ClobRepository,
    requested_token_ids: list[str] | None,
    max_tokens: int | None,
) -> list[str]:
    if requested_token_ids is not None:
        resolved = sorted({token.strip() for token in requested_token_ids if token.strip()})
    else:
        resolved = sorted(repository.list_token_ids(max_tokens=max_tokens))

    if max_tokens is not None:
        if max_tokens <= 0:
            raise ValueError("--max-tokens must be > 0")
        resolved = resolved[:max_tokens]
    return resolved


def _handle_stream_event(
    *,
    event: ClobStreamEvent,
    token_state: StreamTokenState,
    repository: ClobRepository,
    ingested_at: datetime,
    orderbook_depth: int | None,
    seq_mode_enabled: bool,
) -> str:
    if event.channel == "trade":
        trade = _event_to_trade_record(event)
        if trade is None:
            return "drop"
        repository.upsert_trade(trade, ingested_at=ingested_at)
        token_state.observe_trade(trade, seq_mode_enabled=seq_mode_enabled)
        return "trade"

    if event.channel == "orderbook":
        snapshot = _event_to_orderbook_snapshot(
            event=event,
            fallback_event_ts=ingested_at,
            orderbook_depth=orderbook_depth,
        )
        if snapshot is None:
            return "drop"
        repository.upsert_orderbook_snapshot(snapshot, ingested_at=ingested_at)
        token_state.observe_orderbook(snapshot)
        return "orderbook"

    return "drop"


def _event_to_trade_record(event: ClobStreamEvent) -> TradeRecord | None:
    payload = event.payload
    event_ts = event.event_ts or _parse_optional_datetime(
        payload.get("event_ts")
        or payload.get("timestamp")
        or payload.get("ts")
        or payload.get("created_at")
        or payload.get("createdAt")
    )
    price = _parse_optional_decimal(payload.get("price"), quant=_PRICE_QUANT)
    size = _parse_optional_decimal(payload.get("size") or payload.get("amount"), quant=_SIZE_QUANT)
    if event_ts is None or price is None or size is None:
        return None

    side = _normalize_side(payload.get("side"))
    seq = event.seq
    if seq is None:
        seq = _parse_optional_int(
            payload.get("seq")
            or payload.get("sequence")
            or payload.get("offset")
        )
    trade_hash = _optional_text(
        payload.get("trade_hash")
        or payload.get("hash")
        or payload.get("id")
    )
    if seq is None and trade_hash is None:
        trade_hash = build_trade_hash(
            token_id=event.token_id,
            event_ts=event_ts,
            price=price,
            size=size,
            side=side,
            extra_fields=_trade_hash_extra_fields(payload),
        )

    return TradeRecord(
        token_id=event.token_id,
        event_ts=event_ts,
        price=price,
        size=size,
        side=side,
        trade_hash=trade_hash,
        seq=seq,
    )


def _event_to_orderbook_snapshot(
    *,
    event: ClobStreamEvent,
    fallback_event_ts: datetime,
    orderbook_depth: int | None,
) -> OrderbookSnapshot | None:
    payload = event.payload
    event_ts = event.event_ts or _parse_optional_datetime(
        payload.get("event_ts")
        or payload.get("timestamp")
        or payload.get("ts")
        or payload.get("updated_at")
        or payload.get("updatedAt")
    )
    if event_ts is None:
        event_ts = _as_utc_datetime(fallback_event_ts)

    bids, asks = normalize_orderbook(
        payload.get("bids"),
        payload.get("asks"),
        max_depth=orderbook_depth,
    )
    mid = _parse_optional_decimal(payload.get("mid"), quant=_PRICE_QUANT)
    if mid is None and not bids and not asks:
        return None

    return OrderbookSnapshot(
        token_id=event.token_id,
        event_ts=event_ts,
        bids=bids,
        asks=asks,
        mid=mid,
    )


def _trade_hash_extra_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    excluded_keys = {
        "token_id",
        "tokenId",
        "asset_id",
        "assetId",
        "market",
        "event_ts",
        "timestamp",
        "ts",
        "created_at",
        "createdAt",
        "price",
        "size",
        "amount",
        "side",
        "trade_hash",
        "hash",
        "id",
        "seq",
        "sequence",
    }
    extras: dict[str, Any] = {}
    for key in sorted(payload.keys(), key=str):
        key_text = str(key)
        if key_text in excluded_keys:
            continue
        value = payload.get(key)
        if value is None:
            continue
        extras[key_text] = value
    return extras


def _run_reconcile_cycle(
    *,
    token_ids: list[str],
    states: dict[str, StreamTokenState],
    reconciler: ClobReconciler,
    ingested_at: datetime,
    stats: dict[str, int | float],
    logger: logging.Logger,
    run_context: RunContext,
) -> None:
    stats["reconcile_cycles"] += 1
    for token_id in sorted(token_ids):
        result = reconciler.reconcile_token(
            token_id=token_id,
            state=states[token_id],
            ingested_at=ingested_at,
        )
        _log(
            logger,
            logging.INFO,
            "token_state_snapshot",
            run_context,
            token_id=token_id,
            state=states[token_id].as_log_dict(),
            action_taken=result.action_taken,
            window_start=result.window_start.isoformat() if result.window_start else None,
            window_end=result.window_end.isoformat(),
            rest_calls=result.rest_calls,
            rows_upserted=result.rows_upserted,
        )
        if result.action_taken == "none":
            continue
        if result.gap_detected:
            stats["reconcile_gaps"] += 1
        if result.mismatch_detected:
            stats["reconcile_mismatches"] += 1
        stats["repair_trades_upserted"] += result.trades_repaired
        stats["repair_snapshots_upserted"] += result.snapshots_repaired


def _normalize_side(raw: Any) -> str:
    text = _optional_text(raw)
    if text is None:
        return "unknown"
    lowered = text.lower()
    if lowered in {"buy", "bid", "b"}:
        return "buy"
    if lowered in {"sell", "ask", "s"}:
        return "sell"
    return "unknown"


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc_datetime(raw)
    if isinstance(raw, (int, float)):
        value = float(raw)
        if value > 10_000_000_000:
            value /= 1000.0
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return _as_utc_datetime(parsed)
    return None


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
    return _as_utc_datetime(parsed)


def _parse_optional_decimal(raw: Any, *, quant: Decimal) -> Decimal | None:
    if raw is None:
        return None
    try:
        return Decimal(str(raw)).quantize(quant)
    except (InvalidOperation, ValueError):
        return None


def _parse_optional_int(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


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


def _load_optional_text(name: str, default: str | None) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = raw.strip()
    return text if text else None


def _load_wss_seq_config() -> tuple[str | None, tuple[str, ...]]:
    legacy_raw = os.getenv("CLOB_WSS_SEQ_FIELD")
    if legacy_raw is not None:
        legacy_field = legacy_raw.strip()
        if not legacy_field:
            # Explicit empty legacy override disables seq-based detection.
            # This prevents interpreting '' as a valid field name.
            return None, ()
        return legacy_field, (legacy_field,)

    fields_raw = os.getenv("CLOB_WSS_SEQ_FIELDS")
    if fields_raw is None:
        return None, DEFAULT_CLOB_WSS_SEQ_FIELDS

    parsed_fields = _parse_seq_fields_csv(fields_raw)
    return None, parsed_fields


def _parse_seq_fields_csv(raw: str) -> tuple[str, ...]:
    fields: list[str] = []
    for item in raw.split(","):
        candidate = item.strip()
        if not candidate:
            continue
        if candidate not in fields:
            fields.append(candidate)
    return tuple(fields)


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


def _log(
    logger: logging.Logger,
    level: int,
    message: str,
    run_context: RunContext,
    **extra_fields: Any,
) -> None:
    payload = run_context.as_log_context()
    payload["extra_fields"] = extra_fields
    logger.log(level, message, extra=payload)


if __name__ == "__main__":
    raise SystemExit(main())
