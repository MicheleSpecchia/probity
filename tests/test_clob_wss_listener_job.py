from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import pytest

from pmx.ingest.clob_wss_client import ClobReconnectEvent, ClobStreamEvent
from pmx.ingest.reconciler import ReconcileResult
from pmx.jobs.clob_wss_listener import (
    ClobWssListenerConfig,
    load_clob_wss_listener_config,
    run_clob_wss_listener,
)


class _FakeConnectionContext:
    def __enter__(self) -> "_FakeConnectionContext":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        return False


class _FakeRepository:
    instances: list["_FakeRepository"] = []

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.trade_rows: list[str] = []
        self.snapshot_rows: list[str] = []
        _FakeRepository.instances.append(self)

    def insert_run(self, **_: Any) -> None:
        return

    def list_token_ids(self, *, max_tokens: int | None) -> list[str]:
        base = ["token-b", "token-a"]
        if max_tokens is None:
            return base
        return base[:max_tokens]

    def upsert_trade(self, trade: Any, *, ingested_at: Any) -> None:
        _ = ingested_at
        self.trade_rows.append(trade.token_id)

    def upsert_orderbook_snapshot(self, snapshot: Any, *, ingested_at: Any) -> None:
        _ = ingested_at
        self.snapshot_rows.append(snapshot.token_id)


class _FakeWssClient:
    received_token_ids: list[str] = []

    def __init__(self, config: Any) -> None:
        self.config = config

    def listen(self, token_ids: list[str], *, run_seconds: int | None = None) -> Any:
        _ = run_seconds
        _FakeWssClient.received_token_ids = list(token_ids)
        yield ClobReconnectEvent(
            attempt=1,
            delay_seconds=0.55,
            error_type="RuntimeError",
            error_message="disconnected",
        )
        yield ClobStreamEvent(
            token_id="token-a",
            channel="trade",
            event_ts=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            seq=5,
            payload={
                "token_id": "token-a",
                "price": "0.50000000",
                "size": "1.00000000",
                "side": "buy",
                "seq": 5,
            },
        )
        yield ClobStreamEvent(
            token_id="token-a",
            channel="orderbook",
            event_ts=datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC),
            seq=None,
            payload={
                "token_id": "token-a",
                "bids": [{"price": "0.50000000", "size": "1.00000000"}],
                "asks": [{"price": "0.51000000", "size": "1.00000000"}],
                "mid": "0.50500000",
            },
        )


class _FakeRestClient:
    def __init__(self, config: Any) -> None:
        self.config = config


class _FakeReconciler:
    instances: list["_FakeReconciler"] = []

    def __init__(self, **_: Any) -> None:
        self.calls: list[str] = []
        _FakeReconciler.instances.append(self)

    def reconcile_token(
        self,
        *,
        token_id: str,
        state: Any,
        ingested_at: datetime,
    ) -> ReconcileResult:
        _ = state
        self.calls.append(token_id)
        if token_id == "token-a":
            return ReconcileResult(
                token_id=token_id,
                action_taken="rest_refetch_upsert",
                gap_detected=True,
                mismatch_detected=False,
                trades_repaired=2,
                snapshots_repaired=1,
                window_start=None,
                window_end=ingested_at,
                rest_calls=2,
                rows_upserted=3,
            )
        return ReconcileResult(
            token_id=token_id,
            action_taken="none",
            gap_detected=False,
            mismatch_detected=False,
            trades_repaired=0,
            snapshots_repaired=0,
            window_start=None,
            window_end=ingested_at,
            rest_calls=2,
            rows_upserted=0,
        )


def test_clob_wss_listener_processes_messages_and_reconcile(monkeypatch: Any, caplog: Any) -> None:
    _FakeRepository.instances.clear()
    _FakeReconciler.instances.clear()
    _FakeWssClient.received_token_ids = []

    monkeypatch.setattr("pmx.jobs.clob_wss_listener.get_database_url", lambda: "postgresql://fake")
    monkeypatch.setattr(
        "pmx.jobs.clob_wss_listener.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr("pmx.jobs.clob_wss_listener.ClobRepository", _FakeRepository)
    monkeypatch.setattr("pmx.jobs.clob_wss_listener.ClobWssClient", _FakeWssClient)
    monkeypatch.setattr("pmx.jobs.clob_wss_listener.ClobRestClient", _FakeRestClient)
    monkeypatch.setattr("pmx.jobs.clob_wss_listener.ClobReconciler", _FakeReconciler)

    caplog.set_level(logging.INFO)
    stats = run_clob_wss_listener(
        config=ClobWssListenerConfig(
            clob_base_url="https://clob.polymarket.com",
            clob_timeout_seconds=20,
            clob_rate_limit_rps=5.0,
            clob_orderbook_depth=None,
            clob_wss_url="wss://clob.polymarket.com/ws",
            clob_wss_timeout_seconds=20,
            clob_wss_max_reconnect_attempts=8,
            clob_wss_backoff_seconds=0.5,
            clob_wss_max_backoff_seconds=30.0,
            clob_wss_seq_field="seq",
            clob_wss_seq_fields=("seq",),
            clob_reconcile_gap_seconds=60,
            clob_reconcile_mismatch_bps=10,
            ingest_epsilon_seconds=300,
        ),
        max_tokens=None,
        token_ids=None,
        reconcile_every_seconds=300,
        since_ts=datetime(2026, 1, 1, tzinfo=UTC),
        run_seconds=1,
    )

    repo = _FakeRepository.instances[0]
    assert repo.trade_rows == ["token-a"]
    assert repo.snapshot_rows == ["token-a"]
    assert _FakeWssClient.received_token_ids == ["token-a", "token-b"]

    assert stats["messages_received"] == 2
    assert stats["reconnects"] == 1
    assert stats["trades_upserted"] == 1
    assert stats["snapshots_upserted"] == 1
    assert stats["reconcile_cycles"] >= 1
    assert stats["reconcile_gaps"] == 1
    assert stats["repair_trades_upserted"] == 2
    assert stats["repair_snapshots_upserted"] == 1

    reconnect_logs = [record for record in caplog.records if record.msg == "wss_reconnect"]
    assert len(reconnect_logs) == 1
    state_logs = [record for record in caplog.records if record.msg == "token_state_snapshot"]
    assert len(state_logs) >= 1


@pytest.mark.parametrize(
    ("legacy", "fields", "expected_legacy", "expected_fields"),
    [
        ("offset", "seq,sequence,offset", "offset", ("offset",)),
        ("", "seq,sequence,offset", None, ()),
        (None, "sequence,offset", None, ("sequence", "offset")),
        (None, None, None, ("seq", "sequence", "offset")),
    ],
)
def test_load_clob_wss_listener_config_seq_fields_precedence(
    monkeypatch: Any,
    legacy: str | None,
    fields: str | None,
    expected_legacy: str | None,
    expected_fields: tuple[str, ...],
) -> None:
    keys = [
        "CLOB_WSS_SEQ_FIELD",
        "CLOB_WSS_SEQ_FIELDS",
        "CLOB_BASE_URL",
        "CLOB_TIMEOUT_SECONDS",
        "CLOB_RATE_LIMIT_RPS",
        "CLOB_ORDERBOOK_DEPTH",
        "CLOB_WSS_URL",
        "CLOB_WSS_TIMEOUT_SECONDS",
        "CLOB_WSS_MAX_RECONNECTS",
        "CLOB_WSS_BACKOFF_SECONDS",
        "CLOB_WSS_MAX_BACKOFF_SECONDS",
        "CLOB_RECONCILE_GAP_SECONDS",
        "CLOB_RECONCILE_MISMATCH_BPS",
        "INGEST_EPSILON_SECONDS",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)

    if legacy is not None:
        monkeypatch.setenv("CLOB_WSS_SEQ_FIELD", legacy)
    if fields is not None:
        monkeypatch.setenv("CLOB_WSS_SEQ_FIELDS", fields)

    config = load_clob_wss_listener_config()

    assert config.clob_wss_seq_field == expected_legacy
    assert config.clob_wss_seq_fields == expected_fields


def test_load_clob_wss_listener_config_empty_legacy_disables_seq_mode(
    monkeypatch: Any,
) -> None:
    monkeypatch.delenv("CLOB_WSS_SEQ_FIELD", raising=False)
    monkeypatch.delenv("CLOB_WSS_SEQ_FIELDS", raising=False)
    monkeypatch.setenv("CLOB_WSS_SEQ_FIELD", "   ")
    monkeypatch.setenv("CLOB_WSS_SEQ_FIELDS", "seq,sequence,offset")

    config = load_clob_wss_listener_config()

    assert config.clob_wss_seq_field is None
    assert config.clob_wss_seq_fields == ()
