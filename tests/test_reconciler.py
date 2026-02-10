from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pmx.audit.run_context import RunContext
from pmx.ingest.clob_client import OrderbookSnapshot, TradeRecord
from pmx.ingest.reconciler import ClobReconciler, ReconcileStrategyConfig, StreamTokenState


class _FakeRepository:
    def __init__(self) -> None:
        self.trades: list[TradeRecord] = []
        self.snapshots: list[OrderbookSnapshot] = []

    def upsert_trade(self, trade: TradeRecord, *, ingested_at: datetime) -> None:
        _ = ingested_at
        self.trades.append(trade)

    def upsert_orderbook_snapshot(
        self,
        snapshot: OrderbookSnapshot,
        *,
        ingested_at: datetime,
    ) -> None:
        _ = ingested_at
        self.snapshots.append(snapshot)


class _FakeRestClient:
    def __init__(
        self,
        *,
        trades: list[TradeRecord],
        snapshot: OrderbookSnapshot | None,
    ) -> None:
        self._trades = list(trades)
        self._snapshot = snapshot

    def get_trades(self, token_id: str, *, since_ts: datetime | None) -> list[TradeRecord]:
        _ = token_id, since_ts
        return list(self._trades)

    def get_orderbook(
        self,
        token_id: str,
        *,
        fallback_event_ts: datetime | None = None,
    ) -> OrderbookSnapshot | None:
        _ = token_id, fallback_event_ts
        return self._snapshot


def _run_context() -> RunContext:
    return RunContext(
        run_id="test-run-id",
        job_name="clob_wss_listener",
        code_version="test",
        config_hash="cfg",
        started_at="2026-01-01T00:00:00+00:00",
    )


def test_reconciler_detects_gap_and_repairs(caplog: Any) -> None:
    token_id = "token-1"
    state = StreamTokenState()
    state.observe_trade(
        TradeRecord(
            token_id=token_id,
            event_ts=datetime(2026, 1, 1, 0, 0, 10, tzinfo=UTC),
            price=Decimal("0.50000000"),
            size=Decimal("1.00000000"),
            side="buy",
            trade_hash="h10",
            seq=10,
        ),
        seq_mode_enabled=True,
    )
    state.observe_trade(
        TradeRecord(
            token_id=token_id,
            event_ts=datetime(2026, 1, 1, 0, 0, 12, tzinfo=UTC),
            price=Decimal("0.51000000"),
            size=Decimal("1.00000000"),
            side="buy",
            trade_hash="h12",
            seq=12,
        ),
        seq_mode_enabled=True,
    )
    state.observe_orderbook(
        OrderbookSnapshot(
            token_id=token_id,
            event_ts=datetime(2026, 1, 1, 0, 0, 12, tzinfo=UTC),
            bids=[{"price": "0.50000000", "size": "1.00000000"}],
            asks=[{"price": "0.51000000", "size": "1.00000000"}],
            mid=Decimal("0.50500000"),
        )
    )

    rest_trades = [
        TradeRecord(
            token_id=token_id,
            event_ts=datetime(2026, 1, 1, 0, 0, 11, tzinfo=UTC),
            price=Decimal("0.50500000"),
            size=Decimal("1.00000000"),
            side="buy",
            trade_hash="h11",
            seq=11,
        ),
        TradeRecord(
            token_id=token_id,
            event_ts=datetime(2026, 1, 1, 0, 0, 12, tzinfo=UTC),
            price=Decimal("0.51000000"),
            size=Decimal("1.00000000"),
            side="buy",
            trade_hash="h12",
            seq=12,
        ),
    ]
    rest_snapshot = OrderbookSnapshot(
        token_id=token_id,
        event_ts=datetime(2026, 1, 1, 0, 0, 12, tzinfo=UTC),
        bids=[{"price": "0.50000000", "size": "1.00000000"}],
        asks=[{"price": "0.51000000", "size": "1.00000000"}],
        mid=Decimal("0.50500000"),
    )

    caplog.set_level(logging.WARNING)
    repository = _FakeRepository()
    reconciler = ClobReconciler(
        rest_client=_FakeRestClient(trades=rest_trades, snapshot=rest_snapshot),
        repository=repository,
        logger=logging.getLogger("tests.reconciler"),
        run_context=_run_context(),
        since_ts=None,
        strategy=ReconcileStrategyConfig(
            seq_mode_enabled=True,
            gap_seconds=60,
            mismatch_bps=10,
        ),
    )

    result = reconciler.reconcile_token(
        token_id=token_id,
        state=state,
        ingested_at=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert result.gap_detected is True
    assert result.mismatch_detected is False
    assert result.action_taken == "rest_refetch_upsert"
    assert result.trades_repaired == 2
    assert result.snapshots_repaired == 1
    assert result.rest_calls == 2
    assert result.rows_upserted == 3
    assert state.last_seq == 12
    assert state.last_reconcile_ts == datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    assert len(repository.trades) == 2
    assert len(repository.snapshots) == 1

    log_messages = [record.msg for record in caplog.records]
    assert "reconcile_gap" in log_messages
    gap_logs = [record for record in caplog.records if record.msg == "reconcile_gap"]
    assert len(gap_logs) == 1
    assert gap_logs[0].extra_fields["action_taken"] == "rest_refetch_upsert"
    assert gap_logs[0].extra_fields["rows_upserted"] == 3


def test_reconciler_heuristic_mode_repairs_on_mismatch(caplog: Any) -> None:
    token_id = "token-1"
    state = StreamTokenState(
        last_book_ts=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
        last_book_mid=Decimal("0.50000000"),
        last_reconcile_ts=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC),
    )

    rest_snapshot = OrderbookSnapshot(
        token_id=token_id,
        event_ts=datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC),
        bids=[{"price": "0.51000000", "size": "1.00000000"}],
        asks=[{"price": "0.52000000", "size": "1.00000000"}],
        mid=Decimal("0.51500000"),
    )

    caplog.set_level(logging.WARNING)
    repository = _FakeRepository()
    reconciler = ClobReconciler(
        rest_client=_FakeRestClient(trades=[], snapshot=rest_snapshot),
        repository=repository,
        logger=logging.getLogger("tests.reconciler"),
        run_context=_run_context(),
        since_ts=None,
        strategy=ReconcileStrategyConfig(
            seq_mode_enabled=False,
            gap_seconds=60,
            mismatch_bps=10,
        ),
    )

    result = reconciler.reconcile_token(
        token_id=token_id,
        state=state,
        ingested_at=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert result.gap_detected is True
    assert result.mismatch_detected is True
    assert result.action_taken == "rest_refetch_upsert"
    assert result.trades_repaired == 0
    assert result.snapshots_repaired == 1
    assert result.rest_calls == 2
    assert result.rows_upserted == 1
    assert state.last_reconcile_ts == datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    assert len(repository.snapshots) == 1

    log_messages = [record.msg for record in caplog.records]
    assert "reconcile_gap" in log_messages


def test_stream_token_state_ignores_missing_seq_in_seq_mode() -> None:
    token_id = "token-1"
    state = StreamTokenState()
    state.observe_trade(
        TradeRecord(
            token_id=token_id,
            event_ts=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            price=Decimal("0.50000000"),
            size=Decimal("1.00000000"),
            side="buy",
            trade_hash="h1",
            seq=1,
        ),
        seq_mode_enabled=True,
    )
    state.observe_trade(
        TradeRecord(
            token_id=token_id,
            event_ts=datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC),
            price=Decimal("0.50000000"),
            size=Decimal("1.00000000"),
            side="buy",
            trade_hash="h2",
            seq=None,
        ),
        seq_mode_enabled=True,
    )

    assert state.last_seq == 1
    assert state.saw_sequence_gap is False


def test_reconciler_seq_mode_without_seq_falls_back_to_heuristic() -> None:
    token_id = "token-1"
    state = StreamTokenState(
        last_trade_ts=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
    )
    repository = _FakeRepository()
    reconciler = ClobReconciler(
        rest_client=_FakeRestClient(trades=[], snapshot=None),
        repository=repository,
        logger=logging.getLogger("tests.reconciler"),
        run_context=_run_context(),
        since_ts=None,
        strategy=ReconcileStrategyConfig(
            seq_mode_enabled=True,
            gap_seconds=30,
            mismatch_bps=10,
        ),
    )

    result = reconciler.reconcile_token(
        token_id=token_id,
        state=state,
        ingested_at=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert result.gap_detected is True
    assert result.action_taken == "rest_refetch_upsert"
    assert result.rows_upserted == 0
    assert result.rest_calls == 2
