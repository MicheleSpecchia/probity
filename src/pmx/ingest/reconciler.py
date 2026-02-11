from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.db.clob_repository import ClobRepository
from pmx.ingest.clob_client import ClobRestClient, OrderbookSnapshot, TradeRecord

_MIN_MID_BASE = Decimal("0.00000001")


@dataclass(frozen=True, slots=True)
class ReconcileStrategyConfig:
    seq_mode_enabled: bool = True
    gap_seconds: int = 60
    mismatch_bps: int = 10


@dataclass(slots=True)
class StreamTokenState:
    last_seq: int | None = None
    last_trade_ts: datetime | None = None
    last_book_ts: datetime | None = None
    last_book_mid: Decimal | None = None
    last_reconcile_ts: datetime | None = None
    saw_sequence_gap: bool = False
    saw_timestamp_regression: bool = False

    def observe_trade(self, trade: TradeRecord, *, seq_mode_enabled: bool) -> None:
        trade_ts = _as_utc_datetime(trade.event_ts)
        if self.last_trade_ts is not None and trade_ts < self.last_trade_ts:
            self.saw_timestamp_regression = True

        if self.last_trade_ts is None or trade_ts >= self.last_trade_ts:
            self.last_trade_ts = trade_ts

        if seq_mode_enabled and trade.seq is not None:
            if self.last_seq is not None and trade.seq != self.last_seq + 1:
                self.saw_sequence_gap = True
            if self.last_seq is None or trade.seq > self.last_seq:
                self.last_seq = trade.seq

    def observe_orderbook(self, snapshot: OrderbookSnapshot) -> None:
        event_ts = _as_utc_datetime(snapshot.event_ts)
        if self.last_book_ts is not None and event_ts < self.last_book_ts:
            self.saw_timestamp_regression = True

        if self.last_book_ts is None or event_ts >= self.last_book_ts:
            self.last_book_ts = event_ts

        if snapshot.mid is not None:
            self.last_book_mid = snapshot.mid

    def mark_reconciled(self, reconciled_ts: datetime) -> None:
        normalized = _as_utc_datetime(reconciled_ts)
        if self.last_reconcile_ts is None or normalized >= self.last_reconcile_ts:
            self.last_reconcile_ts = normalized

    def as_log_dict(self) -> dict[str, Any]:
        return {
            "last_seq": self.last_seq,
            "last_trade_ts": self.last_trade_ts.isoformat() if self.last_trade_ts else None,
            "last_book_ts": self.last_book_ts.isoformat() if self.last_book_ts else None,
            "last_reconcile_ts": (
                self.last_reconcile_ts.isoformat() if self.last_reconcile_ts else None
            ),
        }


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    token_id: str
    action_taken: str
    gap_detected: bool
    mismatch_detected: bool
    trades_repaired: int
    snapshots_repaired: int
    window_start: datetime | None
    window_end: datetime
    rest_calls: int
    rows_upserted: int


class ClobReconciler:
    def __init__(
        self,
        *,
        rest_client: ClobRestClient,
        repository: ClobRepository,
        logger: logging.Logger,
        run_context: RunContext,
        since_ts: datetime | None,
        strategy: ReconcileStrategyConfig,
    ) -> None:
        self.rest_client = rest_client
        self.repository = repository
        self.logger = logger
        self.run_context = run_context
        self.since_ts = _as_utc_datetime(since_ts) if since_ts is not None else None
        self.strategy = strategy

    def reconcile_token(
        self,
        *,
        token_id: str,
        state: StreamTokenState,
        ingested_at: datetime,
    ) -> ReconcileResult:
        window_end = _as_utc_datetime(ingested_at)
        window_start = state.last_reconcile_ts or state.last_trade_ts or self.since_ts

        rest_calls = 0
        rest_calls += 1
        rest_trades = sorted(
            self.rest_client.get_trades(token_id, since_ts=window_start),
            key=_trade_sort_key,
        )
        rest_calls += 1
        rest_snapshot = self.rest_client.get_orderbook(token_id, fallback_event_ts=window_end)

        gap_reasons = _collect_gap_reasons(
            state=state,
            rest_trades=rest_trades,
            window_end=window_end,
            strategy=self.strategy,
        )
        gap_detected = len(gap_reasons) > 0
        mismatch_detected, mismatch_bps = _compute_orderbook_mismatch_bps(
            state=state,
            rest_snapshot=rest_snapshot,
            mismatch_bps_threshold=self.strategy.mismatch_bps,
        )
        should_repair = gap_detected or mismatch_detected

        trades_repaired = 0
        snapshots_repaired = 0
        action_taken = "none"

        if should_repair:
            for trade in rest_trades:
                self.repository.upsert_trade(trade, ingested_at=window_end)
                state.observe_trade(trade, seq_mode_enabled=self.strategy.seq_mode_enabled)
                trades_repaired += 1

            if rest_snapshot is not None:
                self.repository.upsert_orderbook_snapshot(rest_snapshot, ingested_at=window_end)
                state.observe_orderbook(rest_snapshot)
                snapshots_repaired += 1

            action_taken = "rest_refetch_upsert"
            rows_upserted = trades_repaired + snapshots_repaired

            event_name = "reconcile_gap" if gap_detected else "reconcile_mismatch"
            self._log(
                logging.WARNING,
                event_name,
                token_id=token_id,
                window_start=window_start.isoformat() if window_start else None,
                window_end=window_end.isoformat(),
                stream_state=state.as_log_dict(),
                rest_trades_count=len(rest_trades),
                rest_snapshot_present=rest_snapshot is not None,
                gap_reasons=gap_reasons,
                mismatch_bps=float(mismatch_bps) if mismatch_bps is not None else None,
                action_taken=action_taken,
                rest_calls=rest_calls,
                rows_upserted=rows_upserted,
                trades_repaired=trades_repaired,
                snapshots_repaired=snapshots_repaired,
            )
            state.saw_sequence_gap = False
            state.saw_timestamp_regression = False
        else:
            rows_upserted = 0

        state.mark_reconciled(window_end)

        return ReconcileResult(
            token_id=token_id,
            action_taken=action_taken,
            gap_detected=gap_detected,
            mismatch_detected=mismatch_detected,
            trades_repaired=trades_repaired,
            snapshots_repaired=snapshots_repaired,
            window_start=window_start,
            window_end=window_end,
            rest_calls=rest_calls,
            rows_upserted=rows_upserted,
        )

    def _log(self, level: int, message: str, **extra_fields: Any) -> None:
        payload: dict[str, Any] = dict(self.run_context.as_log_context())
        payload["extra_fields"] = extra_fields
        self.logger.log(level, message, extra=payload)


def _collect_gap_reasons(
    *,
    state: StreamTokenState,
    rest_trades: list[TradeRecord],
    window_end: datetime,
    strategy: ReconcileStrategyConfig,
) -> list[str]:
    reasons: list[str] = []
    if state.saw_timestamp_regression:
        reasons.append("timestamp_regression")

    if strategy.seq_mode_enabled:
        if state.saw_sequence_gap:
            reasons.append("sequence_gap")
        if state.last_seq is not None:
            rest_sequences = [trade.seq for trade in rest_trades if trade.seq is not None]
            if rest_sequences:
                rest_max_seq = max(rest_sequences)
                if rest_max_seq > state.last_seq + 1:
                    reasons.append("rest_seq_ahead")
        else:
            # Seq mode is enabled globally, but this token currently has no
            # usable seq signal; fallback to deterministic heuristic checks.
            reasons.extend(
                _collect_heuristic_gap_reasons(
                    state=state,
                    window_end=window_end,
                    strategy=strategy,
                )
            )
    else:
        reasons.extend(
            _collect_heuristic_gap_reasons(
                state=state,
                window_end=window_end,
                strategy=strategy,
            )
        )

    return reasons


def _collect_heuristic_gap_reasons(
    *,
    state: StreamTokenState,
    window_end: datetime,
    strategy: ReconcileStrategyConfig,
) -> list[str]:
    if state.last_trade_ts is not None:
        delta_seconds = int((window_end - state.last_trade_ts).total_seconds())
        if delta_seconds > strategy.gap_seconds:
            return ["heuristic_trade_stale"]
        return []
    return ["heuristic_reconcile_tick"]


def _compute_orderbook_mismatch_bps(
    *,
    state: StreamTokenState,
    rest_snapshot: OrderbookSnapshot | None,
    mismatch_bps_threshold: int,
) -> tuple[bool, Decimal | None]:
    if rest_snapshot is None or rest_snapshot.mid is None or state.last_book_mid is None:
        return False, None

    stream_mid = state.last_book_mid
    rest_mid = rest_snapshot.mid
    base = max(abs(stream_mid), abs(rest_mid), _MIN_MID_BASE)
    bps = (abs(rest_mid - stream_mid) / base) * Decimal("10000")
    return bps > Decimal(mismatch_bps_threshold), bps


def _trade_sort_key(trade: TradeRecord) -> tuple[datetime, int, str]:
    return (
        trade.event_ts,
        trade.seq if trade.seq is not None else 0,
        trade.trade_hash or "",
    )


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
