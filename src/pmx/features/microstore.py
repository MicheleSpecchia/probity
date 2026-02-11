from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg

from pmx.features.spec_micro_v1 import (
    BookLevel,
    BookSnapshotInput,
    CandleInput,
    TradeInput,
    compute_micro_v1_features,
)


class MicroFeatureStore:
    """Compute microstructure features with strict as-of filters."""

    def compute_features(
        self,
        conn: psycopg.Connection,
        token_id: str,
        decision_ts: datetime,
        epsilon_s: int,
    ) -> dict[str, Any]:
        decision_ts_utc = _as_utc_datetime(decision_ts)
        ingest_bound = decision_ts_utc + timedelta(seconds=epsilon_s)
        lower_5m = decision_ts_utc - timedelta(minutes=5)
        lower_1h = decision_ts_utc - timedelta(hours=1)

        book_snapshot = self._fetch_last_orderbook(
            conn,
            token_id=token_id,
            decision_ts=decision_ts_utc,
            ingest_bound=ingest_bound,
        )
        last_trade = self._fetch_last_trade(
            conn,
            token_id=token_id,
            decision_ts=decision_ts_utc,
            ingest_bound=ingest_bound,
        )
        trades_5m = self._fetch_trades_window(
            conn,
            token_id=token_id,
            start_ts=lower_5m,
            decision_ts=decision_ts_utc,
            ingest_bound=ingest_bound,
        )
        candles_1h = self._fetch_candles_window(
            conn,
            token_id=token_id,
            start_ts=lower_1h,
            decision_ts=decision_ts_utc,
            ingest_bound=ingest_bound,
        )
        anchor_price_5m = self._fetch_anchor_price(
            conn,
            token_id=token_id,
            anchor_ts=lower_5m,
            ingest_bound=ingest_bound,
        )

        features = compute_micro_v1_features(
            decision_ts=decision_ts_utc,
            book_snapshot=book_snapshot,
            last_trade=last_trade,
            trades_5m=trades_5m,
            candles_1h=candles_1h,
            anchor_price_5m=anchor_price_5m,
        )
        features["token_id"] = token_id
        features["decision_ts"] = decision_ts_utc.isoformat()
        features["ingest_bound_ts"] = ingest_bound.isoformat()
        return features

    def _fetch_last_orderbook(
        self,
        conn: psycopg.Connection,
        *,
        token_id: str,
        decision_ts: datetime,
        ingest_bound: datetime,
    ) -> BookSnapshotInput | None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT event_ts, bids, asks, mid
                FROM orderbook_snapshots
                WHERE token_id = %s
                  AND event_ts <= %s
                  AND ingested_at <= %s
                ORDER BY event_ts DESC, ingested_at DESC, snapshot_id DESC
                LIMIT 1
                """,
                (token_id, decision_ts, ingest_bound),
            )
            row = cursor.fetchone()
        if row is None:
            return None

        event_ts = _as_utc_datetime(row[0])
        bids = _parse_book_levels(row[1], descending=True)
        asks = _parse_book_levels(row[2], descending=False)
        mid = _to_decimal(row[3])
        return BookSnapshotInput(
            event_ts=event_ts,
            bids=bids,
            asks=asks,
            mid=mid,
        )

    def _fetch_last_trade(
        self,
        conn: psycopg.Connection,
        *,
        token_id: str,
        decision_ts: datetime,
        ingest_bound: datetime,
    ) -> TradeInput | None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT event_ts, price, size
                FROM trades
                WHERE token_id = %s
                  AND event_ts <= %s
                  AND ingested_at <= %s
                ORDER BY event_ts DESC, ingested_at DESC, trade_id DESC
                LIMIT 1
                """,
                (token_id, decision_ts, ingest_bound),
            )
            row = cursor.fetchone()
        if row is None:
            return None

        price = _to_decimal(row[1])
        size = _to_decimal(row[2])
        if price is None or size is None:
            return None

        return TradeInput(
            event_ts=_as_utc_datetime(row[0]),
            price=price,
            size=size,
        )

    def _fetch_trades_window(
        self,
        conn: psycopg.Connection,
        *,
        token_id: str,
        start_ts: datetime,
        decision_ts: datetime,
        ingest_bound: datetime,
    ) -> tuple[TradeInput, ...]:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT event_ts, price, size
                FROM trades
                WHERE token_id = %s
                  AND event_ts >= %s
                  AND event_ts <= %s
                  AND ingested_at <= %s
                ORDER BY event_ts ASC, ingested_at ASC, trade_id ASC
                """,
                (token_id, start_ts, decision_ts, ingest_bound),
            )
            rows = cursor.fetchall()

        output: list[TradeInput] = []
        for row in rows:
            price = _to_decimal(row[1])
            size = _to_decimal(row[2])
            if price is None or size is None:
                continue
            output.append(
                TradeInput(
                    event_ts=_as_utc_datetime(row[0]),
                    price=price,
                    size=size,
                )
            )
        return tuple(output)

    def _fetch_candles_window(
        self,
        conn: psycopg.Connection,
        *,
        token_id: str,
        start_ts: datetime,
        decision_ts: datetime,
        ingest_bound: datetime,
    ) -> tuple[CandleInput, ...]:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT start_ts, end_ts, c
                FROM candles
                WHERE token_id = %s
                  AND interval = '1m'
                  AND start_ts >= %s
                  AND start_ts <= %s
                  AND ingested_at <= %s
                ORDER BY start_ts ASC, ingested_at ASC, candle_id ASC
                """,
                (token_id, start_ts, decision_ts, ingest_bound),
            )
            rows = cursor.fetchall()

        output: list[CandleInput] = []
        for row in rows:
            close = _to_decimal(row[2])
            if close is None:
                continue
            output.append(
                CandleInput(
                    start_ts=_as_utc_datetime(row[0]),
                    end_ts=_as_utc_datetime(row[1]),
                    close=close,
                )
            )
        return tuple(output)

    def _fetch_anchor_price(
        self,
        conn: psycopg.Connection,
        *,
        token_id: str,
        anchor_ts: datetime,
        ingest_bound: datetime,
    ) -> Decimal | None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT price
                FROM trades
                WHERE token_id = %s
                  AND event_ts <= %s
                  AND ingested_at <= %s
                ORDER BY event_ts DESC, ingested_at DESC, trade_id DESC
                LIMIT 1
                """,
                (token_id, anchor_ts, ingest_bound),
            )
            trade_row = cursor.fetchone()
        if trade_row is not None:
            trade_price = _to_decimal(trade_row[0])
            if trade_price is not None:
                return trade_price

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT bids, asks, mid
                FROM orderbook_snapshots
                WHERE token_id = %s
                  AND event_ts <= %s
                  AND ingested_at <= %s
                ORDER BY event_ts DESC, ingested_at DESC, snapshot_id DESC
                LIMIT 1
                """,
                (token_id, anchor_ts, ingest_bound),
            )
            row = cursor.fetchone()
        if row is None:
            return None

        mid = _to_decimal(row[2])
        if mid is not None and mid > 0:
            return mid
        bids = _parse_book_levels(row[0], descending=True)
        asks = _parse_book_levels(row[1], descending=False)
        if not bids or not asks:
            return None
        best_bid = bids[0].price
        best_ask = asks[0].price
        if best_bid <= 0 or best_ask <= 0:
            return None
        return (best_bid + best_ask) / Decimal("2")


def _parse_book_levels(raw: Any, *, descending: bool) -> tuple[BookLevel, ...]:
    if not isinstance(raw, list):
        return ()
    parsed: list[BookLevel] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        price = _to_decimal(item.get("price"))
        size = _to_decimal(item.get("size"))
        if price is None or size is None:
            continue
        parsed.append(BookLevel(price=price, size=size))
    parsed.sort(key=lambda level: level.price, reverse=descending)
    return tuple(parsed)


def _to_decimal(raw: Any) -> Decimal | None:
    if raw is None:
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
