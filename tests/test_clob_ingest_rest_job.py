from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pmx.ingest.clob_client import CandleRecord, OrderbookSnapshot, TradeRecord
from pmx.jobs.clob_ingest_rest import ClobIngestRestConfig, run_clob_ingest_rest


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
        self.tokens_requested: list[str] = []
        _FakeRepository.instances.append(self)

    def insert_run(self, **_: Any) -> None:
        return

    def list_token_ids(self, *, max_tokens: int | None) -> list[str]:
        if max_tokens is None:
            return ["token-b", "token-a"]
        return ["token-b", "token-a"][:max_tokens]

    def upsert_orderbook_snapshot(self, snapshot: Any, *, ingested_at: Any) -> None:
        self.tokens_requested.append(snapshot.token_id)
        return

    def upsert_trade(self, trade: Any, *, ingested_at: Any) -> None:
        self.tokens_requested.append(trade.token_id)
        return

    def upsert_candle(self, candle: Any, *, ingested_at: Any) -> None:
        self.tokens_requested.append(candle.token_id)
        return


class _FakeClobClient:
    instances: list["_FakeClobClient"] = []

    def __init__(self, config: Any) -> None:
        self.config = config
        self.orderbook_fallbacks: list[datetime | None] = []
        _FakeClobClient.instances.append(self)

    def get_orderbook(
        self,
        token_id: str,
        *,
        fallback_event_ts: datetime | None = None,
    ) -> OrderbookSnapshot | None:
        self.orderbook_fallbacks.append(fallback_event_ts)
        if token_id == "token-b":
            raise RuntimeError("simulated token failure")
        return OrderbookSnapshot(
            token_id=token_id,
            event_ts=datetime(2026, 1, 1, tzinfo=UTC),
            bids=[{"price": "0.50000000", "size": "10.00000000"}],
            asks=[{"price": "0.51000000", "size": "10.00000000"}],
            mid=Decimal("0.50500000"),
        )

    def get_trades(self, token_id: str, *, since_ts: datetime | None) -> list[TradeRecord]:
        if token_id == "token-b":
            raise RuntimeError("simulated token failure")
        return [
            TradeRecord(
                token_id=token_id,
                event_ts=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
                price=Decimal("0.50000000"),
                size=Decimal("1.00000000"),
                side="buy",
                trade_hash="h1",
                seq=1,
            )
        ]

    def get_candles(
        self,
        token_id: str,
        *,
        interval: str,
        since_ts: datetime | None,
    ) -> list[CandleRecord]:
        if token_id == "token-b":
            raise RuntimeError("simulated token failure")
        return [
            CandleRecord(
                token_id=token_id,
                interval=interval,
                start_ts=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                end_ts=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
                o=Decimal("0.40000000"),
                h=Decimal("0.60000000"),
                l=Decimal("0.39000000"),
                c=Decimal("0.50000000"),
                v=Decimal("5.00000000"),
            )
        ]


def test_clob_ingest_rest_sorts_tokens_and_continues_on_token_error(
    monkeypatch: Any,
    caplog: Any,
) -> None:
    _FakeRepository.instances.clear()
    _FakeClobClient.instances.clear()
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.get_database_url", lambda: "postgresql://fake")
    monkeypatch.setattr(
        "pmx.jobs.clob_ingest_rest.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRepository", _FakeRepository)
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRestClient", _FakeClobClient)

    caplog.set_level(logging.INFO)

    stats = run_clob_ingest_rest(
        config=ClobIngestRestConfig(
            clob_base_url="https://clob.polymarket.com",
            clob_timeout_seconds=20,
            clob_rate_limit_rps=5.0,
            ingest_epsilon_seconds=300,
        ),
        max_tokens=None,
        since_ts=datetime(2026, 1, 1, tzinfo=UTC),
        interval="1m",
    )

    assert stats["tokens_processed"] == 1
    assert stats["token_errors"] == 1
    assert stats["snapshots_upserted"] == 1
    assert stats["trades_upserted"] == 1
    assert stats["candles_upserted"] == 1

    repo = _FakeRepository.instances[0]
    assert repo.tokens_requested == ["token-a", "token-a", "token-a"]
    assert _FakeClobClient.instances[0].orderbook_fallbacks[0] is not None

    failure_logs = [record for record in caplog.records if record.msg == "clob_token_failed"]
    assert len(failure_logs) == 1
    assert getattr(failure_logs[0], "extra_fields")["token_id"] == "token-b"
