from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from pmx.ingest.clob_client import CandleRecord, OrderbookSnapshot, TradeRecord, build_trade_hash


@dataclass(frozen=True, slots=True)
class TokenIngestStats:
    token_id: str
    snapshots_upserted: int = 0
    trades_upserted: int = 0
    candles_upserted: int = 0


class ClobRepository:
    def __init__(self, connection: psycopg.Connection) -> None:
        self.connection = connection

    def list_token_ids(self, *, max_tokens: int | None) -> list[str]:
        if max_tokens is None:
            query = "SELECT token_id FROM market_tokens ORDER BY token_id ASC"
            params: Sequence[object] = ()
        else:
            query = "SELECT token_id FROM market_tokens ORDER BY token_id ASC LIMIT %s"
            params = (max_tokens,)

        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        return [str(row[0]) for row in rows]

    def insert_run(
        self,
        *,
        run_id: UUID,
        run_type: str,
        decision_ts: datetime,
        ingest_epsilon_seconds: int,
        code_version: str,
        config_hash: str,
    ) -> None:
        decision_ts_utc = _as_utc_datetime(decision_ts)
        with self.connection.cursor() as cursor:
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
                    decision_ts_utc,
                    ingest_epsilon_seconds,
                    code_version,
                    config_hash,
                ),
            )

    def upsert_orderbook_snapshot(
        self,
        snapshot: OrderbookSnapshot,
        *,
        ingested_at: datetime,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO orderbook_snapshots (
                    token_id,
                    event_ts,
                    ingested_at,
                    bids,
                    asks,
                    mid
                )
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s)
                ON CONFLICT (token_id, event_ts) DO UPDATE
                SET ingested_at = EXCLUDED.ingested_at,
                    bids = EXCLUDED.bids,
                    asks = EXCLUDED.asks,
                    mid = EXCLUDED.mid
                """,
                (
                    snapshot.token_id,
                    snapshot.event_ts,
                    _as_utc_datetime(ingested_at),
                    Jsonb(snapshot.bids),
                    Jsonb(snapshot.asks),
                    snapshot.mid,
                ),
            )

    def upsert_trade(self, trade: TradeRecord, *, ingested_at: datetime) -> None:
        trade_hash = trade.trade_hash
        if trade_hash is None and trade.seq is None:
            trade_hash = build_trade_hash(
                token_id=trade.token_id,
                event_ts=trade.event_ts,
                price=trade.price,
                size=trade.size,
                side=trade.side,
            )
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO trades (
                    token_id,
                    event_ts,
                    ingested_at,
                    price,
                    size,
                    side,
                    trade_hash,
                    seq
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT trades_idempotency_uk DO UPDATE
                SET ingested_at = EXCLUDED.ingested_at,
                    price = EXCLUDED.price,
                    size = EXCLUDED.size,
                    side = EXCLUDED.side
                """,
                (
                    trade.token_id,
                    trade.event_ts,
                    _as_utc_datetime(ingested_at),
                    trade.price,
                    trade.size,
                    trade.side,
                    trade_hash,
                    trade.seq,
                ),
            )

    def upsert_candle(self, candle: CandleRecord, *, ingested_at: datetime) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO candles (
                    token_id,
                    interval,
                    start_ts,
                    end_ts,
                    ingested_at,
                    o,
                    h,
                    l,
                    c,
                    v
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (token_id, interval, start_ts) DO UPDATE
                SET end_ts = EXCLUDED.end_ts,
                    ingested_at = EXCLUDED.ingested_at,
                    o = EXCLUDED.o,
                    h = EXCLUDED.h,
                    l = EXCLUDED.l,
                    c = EXCLUDED.c,
                    v = EXCLUDED.v
                """,
                (
                    candle.token_id,
                    candle.interval,
                    candle.start_ts,
                    candle.end_ts,
                    _as_utc_datetime(ingested_at),
                    candle.o,
                    candle.h,
                    candle.low,
                    candle.c,
                    candle.v,
                ),
            )


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
