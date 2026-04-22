from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, ClassVar

from pmx.ingest.clob_client import CandleRecord, ClobHttpError, OrderbookSnapshot, TradeRecord
from pmx.jobs.clob_ingest_rest import ClobIngestRestConfig, run_clob_ingest_rest


def _is_clob_token_failed_log(record: Any) -> bool:
    message = record.getMessage()
    return (
        message == "clob_token_failed"
        or record.msg == "clob_token_failed"
        or '"msg":"clob_token_failed"' in message
    )


class _FakeConnectionContext:
    def __enter__(self) -> _FakeConnectionContext:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        return False


class _FakeRepository:
    instances: ClassVar[list[_FakeRepository]] = []

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.tokens_requested: list[str] = []
        self.snapshot_ingested_at: list[datetime] = []
        self.trade_ingested_at: list[datetime] = []
        self.candle_ingested_at: list[datetime] = []
        _FakeRepository.instances.append(self)

    def insert_run(self, **_: Any) -> None:
        return

    def list_token_ids(self, *, max_tokens: int | None) -> list[str]:
        if max_tokens is None:
            return ["token-b", "token-a"]
        return ["token-b", "token-a"][:max_tokens]

    def list_token_ingest_refs(self, *, max_tokens: int | None) -> list[Any]:
        refs = [
            SimpleNamespace(token_id="token-b", condition_id="cond-b"),
            SimpleNamespace(token_id="token-a", condition_id="cond-a"),
        ]
        if max_tokens is None:
            return refs
        return refs[:max_tokens]

    def upsert_orderbook_snapshot(self, snapshot: Any, *, ingested_at: Any) -> None:
        self.tokens_requested.append(snapshot.token_id)
        self.snapshot_ingested_at.append(ingested_at)
        return

    def upsert_trade(self, trade: Any, *, ingested_at: Any) -> None:
        self.tokens_requested.append(trade.token_id)
        self.trade_ingested_at.append(ingested_at)
        return

    def upsert_candle(self, candle: Any, *, ingested_at: Any) -> None:
        self.tokens_requested.append(candle.token_id)
        self.candle_ingested_at.append(ingested_at)
        return


class _FakeRepositorySingleToken(_FakeRepository):
    def list_token_ingest_refs(self, *, max_tokens: int | None) -> list[Any]:
        return [SimpleNamespace(token_id="token-a", condition_id="cond-a")]


class _FakeClobClient:
    instances: ClassVar[list[_FakeClobClient]] = []

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

    def get_market_trades(
        self,
        condition_id: str,
        *,
        token_id: str,
        since_ts: datetime | None,
    ) -> list[TradeRecord]:
        return []

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
                low=Decimal("0.39000000"),
                c=Decimal("0.50000000"),
                v=Decimal("5.00000000"),
            )
        ]


class _FakeClobClientPartial:
    instances: ClassVar[list[_FakeClobClientPartial]] = []

    def __init__(self, config: Any) -> None:
        self.config = config
        _FakeClobClientPartial.instances.append(self)

    def get_orderbook(
        self,
        token_id: str,
        *,
        fallback_event_ts: datetime | None = None,
    ) -> OrderbookSnapshot | None:
        raise ClobHttpError(status_code=404, path="/book")

    def get_trades(self, token_id: str, *, since_ts: datetime | None) -> list[TradeRecord]:
        raise ClobHttpError(status_code=401, path="/trades")

    def get_market_trades(
        self,
        condition_id: str,
        *,
        token_id: str,
        since_ts: datetime | None,
    ) -> list[TradeRecord]:
        return []

    def get_candles(
        self,
        token_id: str,
        *,
        interval: str,
        since_ts: datetime | None,
    ) -> list[CandleRecord]:
        return [
            CandleRecord(
                token_id=token_id,
                interval=interval,
                start_ts=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                end_ts=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
                o=Decimal("0.40000000"),
                h=Decimal("0.60000000"),
                low=Decimal("0.39000000"),
                c=Decimal("0.50000000"),
                v=Decimal("5.00000000"),
            )
        ]


class _FakeClobClientPublicFallback:
    instances: ClassVar[list[_FakeClobClientPublicFallback]] = []

    def __init__(self, config: Any) -> None:
        self.config = config
        _FakeClobClientPublicFallback.instances.append(self)

    def get_orderbook(
        self,
        token_id: str,
        *,
        fallback_event_ts: datetime | None = None,
    ) -> OrderbookSnapshot | None:
        raise ClobHttpError(status_code=404, path="/book")

    def get_trades(self, token_id: str, *, since_ts: datetime | None) -> list[TradeRecord]:
        raise ClobHttpError(status_code=401, path="/data/trades")

    def get_market_trades(
        self,
        condition_id: str,
        *,
        token_id: str,
        since_ts: datetime | None,
    ) -> list[TradeRecord]:
        return [
            TradeRecord(
                token_id=token_id,
                event_ts=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
                price=Decimal("0.50000000"),
                size=Decimal("1.00000000"),
                side="buy",
                trade_hash="public-fallback-trade",
                seq=None,
            )
        ]

    def get_candles(
        self,
        token_id: str,
        *,
        interval: str,
        since_ts: datetime | None,
    ) -> list[CandleRecord]:
        return [
            CandleRecord(
                token_id=token_id,
                interval=interval,
                start_ts=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                end_ts=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
                o=Decimal("0.40000000"),
                h=Decimal("0.60000000"),
                low=Decimal("0.39000000"),
                c=Decimal("0.50000000"),
                v=Decimal("5.00000000"),
            )
        ]


class _FakeClobClientDeterministic:
    instances: ClassVar[list[_FakeClobClientDeterministic]] = []

    def __init__(self, config: Any) -> None:
        self.config = config
        _FakeClobClientDeterministic.instances.append(self)

    def get_orderbook(
        self,
        token_id: str,
        *,
        fallback_event_ts: datetime | None = None,
    ) -> OrderbookSnapshot | None:
        return OrderbookSnapshot(
            token_id=token_id,
            event_ts=datetime(2026, 1, 1, tzinfo=UTC),
            bids=[{"price": "0.50000000", "size": "10.00000000"}],
            asks=[{"price": "0.51000000", "size": "10.00000000"}],
            mid=Decimal("0.50500000"),
        )

    def get_trades(self, token_id: str, *, since_ts: datetime | None) -> list[TradeRecord]:
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

    def get_market_trades(
        self,
        condition_id: str,
        *,
        token_id: str,
        since_ts: datetime | None,
    ) -> list[TradeRecord]:
        return []

    def get_candles(
        self,
        token_id: str,
        *,
        interval: str,
        since_ts: datetime | None,
    ) -> list[CandleRecord]:
        return [
            CandleRecord(
                token_id=token_id,
                interval=interval,
                start_ts=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                end_ts=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
                o=Decimal("0.40000000"),
                h=Decimal("0.60000000"),
                low=Decimal("0.39000000"),
                c=Decimal("0.50000000"),
                v=Decimal("5.00000000"),
            )
        ]


def test_clob_ingest_rest_sorts_tokens_and_continues_on_token_error(
    monkeypatch: Any,
    caplog: Any,
    capsys: Any,
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
    caplog.set_level(logging.ERROR, logger="pmx.jobs.clob_ingest_rest")

    stats = run_clob_ingest_rest(
        config=ClobIngestRestConfig(
            clob_base_url="https://clob.polymarket.com",
            data_api_base_url="https://data-api.polymarket.com",
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

    failure_logs = [record for record in caplog.records if _is_clob_token_failed_log(record)]
    if failure_logs:
        extra_fields = getattr(failure_logs[0], "extra_fields", None)
        if not isinstance(extra_fields, dict):
            raise AssertionError("Expected structured extra_fields dict on log record")
        assert extra_fields.get("token_id") == "token-b"
    else:
        stderr_lines = [line for line in capsys.readouterr().err.splitlines() if line.strip()]
        parsed = []
        for line in stderr_lines:
            if not line.startswith("{"):
                continue
            try:
                parsed.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        failures = [item for item in parsed if item.get("msg") == "clob_token_failed"]
        assert len(failures) == 1
        assert failures[0].get("token_id") == "token-b"


def test_clob_ingest_rest_handles_book_404_and_trades_401_without_failing_token(
    monkeypatch: Any,
    caplog: Any,
) -> None:
    _FakeRepository.instances.clear()
    _FakeClobClientPartial.instances.clear()
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.get_database_url", lambda: "postgresql://fake")
    monkeypatch.setattr(
        "pmx.jobs.clob_ingest_rest.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRepository", _FakeRepository)
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRestClient", _FakeClobClientPartial)
    caplog.set_level(logging.WARNING, logger="pmx.jobs.clob_ingest_rest")

    stats = run_clob_ingest_rest(
        config=ClobIngestRestConfig(
            clob_base_url="https://clob.polymarket.com",
            data_api_base_url="https://data-api.polymarket.com",
            clob_timeout_seconds=20,
            clob_rate_limit_rps=5.0,
            ingest_epsilon_seconds=300,
        ),
        max_tokens=1,
        since_ts=datetime(2026, 1, 1, tzinfo=UTC),
        interval="1m",
    )

    assert stats["tokens_processed"] == 1
    assert stats["token_errors"] == 0
    assert stats["snapshots_upserted"] == 0
    assert stats["trades_upserted"] == 0
    assert stats["candles_upserted"] == 1

    missing_logs = [
        record for record in caplog.records if record.getMessage() == "clob_token_data_missing"
    ]
    assert len(missing_logs) == 1
    extra_fields = getattr(missing_logs[0], "extra_fields", None)
    if not isinstance(extra_fields, dict):
        raise AssertionError("Expected structured extra_fields dict on log record")
    assert extra_fields.get("skip_reasons") == [
        "book_not_found",
        "trades_unauthorized_missing_l2_auth_material",
    ]


def test_clob_ingest_rest_uses_public_market_trades_fallback(
    monkeypatch: Any,
    caplog: Any,
) -> None:
    _FakeRepository.instances.clear()
    _FakeClobClientPublicFallback.instances.clear()
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.get_database_url", lambda: "postgresql://fake")
    monkeypatch.setattr(
        "pmx.jobs.clob_ingest_rest.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRepository", _FakeRepository)
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRestClient", _FakeClobClientPublicFallback)
    caplog.set_level(logging.WARNING, logger="pmx.jobs.clob_ingest_rest")

    stats = run_clob_ingest_rest(
        config=ClobIngestRestConfig(
            clob_base_url="https://clob.polymarket.com",
            data_api_base_url="https://data-api.polymarket.com",
            clob_timeout_seconds=20,
            clob_rate_limit_rps=5.0,
            ingest_epsilon_seconds=300,
        ),
        max_tokens=1,
        since_ts=datetime(2026, 1, 1, tzinfo=UTC),
        interval="1m",
    )

    assert stats["tokens_processed"] == 1
    assert stats["token_errors"] == 0
    assert stats["snapshots_upserted"] == 0
    assert stats["trades_upserted"] == 1
    assert stats["candles_upserted"] == 1

    missing_logs = [
        record for record in caplog.records if record.getMessage() == "clob_token_data_missing"
    ]
    assert len(missing_logs) == 1
    extra_fields = getattr(missing_logs[0], "extra_fields", None)
    if not isinstance(extra_fields, dict):
        raise AssertionError("Expected structured extra_fields dict on log record")
    assert extra_fields.get("skip_reasons") == ["book_not_found"]


def test_clob_ingest_rest_default_now_mode_uses_single_run_ingested_at(monkeypatch: Any) -> None:
    _FakeRepository.instances.clear()
    _FakeClobClientDeterministic.instances.clear()
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.get_database_url", lambda: "postgresql://fake")
    monkeypatch.setattr(
        "pmx.jobs.clob_ingest_rest.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRepository", _FakeRepositorySingleToken)
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRestClient", _FakeClobClientDeterministic)

    before_run = datetime.now(tz=UTC)
    stats = run_clob_ingest_rest(
        config=ClobIngestRestConfig(
            clob_base_url="https://clob.polymarket.com",
            data_api_base_url="https://data-api.polymarket.com",
            clob_timeout_seconds=20,
            clob_rate_limit_rps=5.0,
            ingest_epsilon_seconds=300,
        ),
        max_tokens=1,
        since_ts=datetime(2026, 1, 1, tzinfo=UTC),
        interval="1m",
    )
    after_run = datetime.now(tz=UTC)

    assert stats["tokens_processed"] == 1
    assert stats["token_errors"] == 0
    repo = _FakeRepository.instances[0]
    all_ingested_at = repo.snapshot_ingested_at + repo.trade_ingested_at + repo.candle_ingested_at
    assert len(all_ingested_at) == 3
    assert len(set(all_ingested_at)) == 1
    assert before_run <= all_ingested_at[0] <= after_run


def test_clob_ingest_rest_event_ts_mode_assigns_event_ts_plus_latency(monkeypatch: Any) -> None:
    _FakeRepository.instances.clear()
    _FakeClobClientDeterministic.instances.clear()
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.get_database_url", lambda: "postgresql://fake")
    monkeypatch.setattr(
        "pmx.jobs.clob_ingest_rest.psycopg.connect",
        lambda *args, **kwargs: _FakeConnectionContext(),
    )
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRepository", _FakeRepositorySingleToken)
    monkeypatch.setattr("pmx.jobs.clob_ingest_rest.ClobRestClient", _FakeClobClientDeterministic)

    stats = run_clob_ingest_rest(
        config=ClobIngestRestConfig(
            clob_base_url="https://clob.polymarket.com",
            data_api_base_url="https://data-api.polymarket.com",
            clob_timeout_seconds=20,
            clob_rate_limit_rps=5.0,
            ingest_epsilon_seconds=300,
        ),
        max_tokens=1,
        since_ts=datetime(2026, 1, 1, tzinfo=UTC),
        interval="1m",
        ingested_at_mode="event_ts",
        ingest_latency_seconds=7,
    )

    assert stats["tokens_processed"] == 1
    assert stats["token_errors"] == 0
    repo = _FakeRepository.instances[0]
    assert repo.snapshot_ingested_at == [datetime(2026, 1, 1, 0, 0, 7, tzinfo=UTC)]
    assert repo.trade_ingested_at == [datetime(2026, 1, 1, 0, 0, 8, tzinfo=UTC)]
    assert repo.candle_ingested_at == [datetime(2026, 1, 1, 0, 0, 7, tzinfo=UTC)]
