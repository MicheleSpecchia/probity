from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pmx.ingest.clob_client import ClobClientConfig, ClobRestClient, build_trade_hash

_FIXTURES_DIR = Path(__file__).with_name("fixtures") / "clob"


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int,
        payload: Any,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []
        self.headers: dict[str, str] = {}

    def get(
        self,
        url: str,
        params: Any,
        timeout: int,
        headers: dict[str, str] | None = None,
    ) -> _FakeResponse:
        self.calls.append(
            {
                "url": url,
                "params": dict(params),
                "timeout": timeout,
                "headers": dict(headers or {}),
            }
        )
        if not self.responses:
            raise RuntimeError("No fake response available")
        return self.responses.pop(0)


def _load_fixture(name: str) -> Any:
    with (_FIXTURES_DIR / name).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def test_get_orderbook_parses_levels_and_mid() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                status_code=200,
                payload=_load_fixture("orderbook.json"),
            )
        ]
    )
    client = ClobRestClient(ClobClientConfig(base_url="https://example.clob"), session=session)

    snapshot = client.get_orderbook("token-1")

    assert snapshot is not None
    assert snapshot.token_id == "token-1"
    assert snapshot.event_ts == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert snapshot.bids == [{"price": "0.52000000", "size": "100.00000000"}]
    assert snapshot.asks == [{"price": "0.53000000", "size": "90.00000000"}]
    assert str(snapshot.mid) == "0.52500000"


def test_get_trades_normalizes_fields() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                status_code=200,
                payload=_load_fixture("trades.json"),
            )
        ]
    )
    client = ClobRestClient(ClobClientConfig(base_url="https://example.clob"), session=session)

    trades = client.get_trades("token-1", since_ts=datetime(2025, 12, 31, tzinfo=UTC))

    assert len(trades) == 1
    trade = trades[0]
    assert trade.token_id == "token-1"
    assert trade.side == "buy"
    assert trade.trade_hash == "trade-hash-1"
    assert trade.seq == 7
    assert str(trade.price) == "0.50000000"
    assert str(trade.size) == "10.00000000"
    assert session.calls[0]["params"]["asset_id"] == "token-1"
    assert session.calls[0]["params"]["next_cursor"] == "MA=="
    assert session.calls[0]["params"]["after"] == str(
        int(datetime(2025, 12, 31, tzinfo=UTC).timestamp())
    )


def test_get_candles_parses_ohlcv() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                status_code=200,
                payload=_load_fixture("candles.json"),
            )
        ]
    )
    client = ClobRestClient(ClobClientConfig(base_url="https://example.clob"), session=session)

    candles = client.get_candles("token-1", interval="1m", since_ts=None)

    assert len(candles) == 1
    candle = candles[0]
    assert candle.interval == "1m"
    assert candle.start_ts == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert candle.end_ts == datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    assert str(candle.o) == "0.45000000"
    assert str(candle.c) == "0.52000000"
    assert str(candle.v) == "123.45600000"


def test_get_trades_since_ts_is_inclusive() -> None:
    payload = {
        "trades": [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "price": "0.5",
                "size": "10",
                "side": "buy",
                "hash": "trade-t0",
            },
            {
                "timestamp": "2026-01-01T00:00:01Z",
                "price": "0.6",
                "size": "11",
                "side": "sell",
                "hash": "trade-t1",
            },
        ]
    }
    session = _FakeSession([_FakeResponse(status_code=200, payload=payload)])
    client = ClobRestClient(ClobClientConfig(base_url="https://example.clob"), session=session)

    trades = client.get_trades(
        "token-1",
        since_ts=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
    )

    assert [trade.trade_hash for trade in trades] == ["trade-t1"]
    assert session.calls[0]["params"]["asset_id"] == "token-1"
    assert session.calls[0]["params"]["next_cursor"] == "MA=="
    assert session.calls[0]["params"]["after"] == str(
        int(datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC).timestamp())
    )


def test_get_candles_since_ts_is_inclusive() -> None:
    payload = {
        "candles": [
            {
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T00:01:00Z",
                "open": "0.40",
                "high": "0.50",
                "low": "0.39",
                "close": "0.45",
                "volume": "100",
            },
            {
                "start": "2026-01-01T00:01:00Z",
                "end": "2026-01-01T00:02:00Z",
                "open": "0.45",
                "high": "0.60",
                "low": "0.44",
                "close": "0.55",
                "volume": "120",
            },
        ]
    }
    session = _FakeSession([_FakeResponse(status_code=200, payload=payload)])
    client = ClobRestClient(ClobClientConfig(base_url="https://example.clob"), session=session)

    candles = client.get_candles(
        "token-1",
        interval="1m",
        since_ts=datetime(2026, 1, 1, 0, 1, 0, tzinfo=UTC),
    )

    assert len(candles) == 1
    assert candles[0].start_ts == datetime(2026, 1, 1, 0, 1, 0, tzinfo=UTC)
    assert session.calls[0]["params"]["since"] == "2026-01-01T00:01:00+00:00"


def test_get_orderbook_falls_back_to_job_timestamp_when_api_timestamp_missing() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                status_code=200,
                payload={"bids": [["0.52", "100"]], "asks": [["0.53", "90"]]},
            )
        ]
    )
    client = ClobRestClient(ClobClientConfig(base_url="https://example.clob"), session=session)
    fallback_ts = datetime(2026, 2, 10, 8, 0, 0, tzinfo=UTC)

    snapshot = client.get_orderbook("token-1", fallback_event_ts=fallback_ts)

    assert snapshot is not None
    assert snapshot.event_ts == fallback_ts


def test_orderbook_normalization_is_sorted_quantized_and_depth_limited() -> None:
    payload = {
        "timestamp": "2026-01-01T00:00:00Z",
        "bids": [
            {"price": "0.5", "size": "3"},
            {"price": "0.7", "size": "1.111111111"},
            ["0.6", "2"],
        ],
        "asks": [
            {"price": "0.8", "size": "3"},
            ["0.75", "1.000000009"],
            {"price": "0.9", "size": "2"},
        ],
    }
    session = _FakeSession([_FakeResponse(status_code=200, payload=payload)])
    client = ClobRestClient(
        ClobClientConfig(base_url="https://example.clob", orderbook_depth=2),
        session=session,
    )

    snapshot = client.get_orderbook("token-1")

    assert snapshot is not None
    assert snapshot.bids == [
        {"price": "0.70000000", "size": "1.11111111"},
        {"price": "0.60000000", "size": "2.00000000"},
    ]
    assert snapshot.asks == [
        {"price": "0.75000000", "size": "1.00000001"},
        {"price": "0.80000000", "size": "3.00000000"},
    ]


def test_trades_missing_seq_and_hash_get_deterministic_fallback_hash() -> None:
    payload = {
        "trades": [
            {
                "timestamp": "2026-01-01T00:00:01Z",
                "price": "0.5",
                "size": "10",
                "side": "buy",
                "maker": "0xaaa",
            },
            {
                "timestamp": "2026-01-01T00:00:01Z",
                "price": "0.6",
                "size": "10",
                "side": "buy",
                "maker": "0xaaa",
            },
        ]
    }
    session = _FakeSession([_FakeResponse(status_code=200, payload=payload)])
    client = ClobRestClient(ClobClientConfig(base_url="https://example.clob"), session=session)

    first_pass = client.get_trades("token-1", since_ts=None)
    second_session = _FakeSession([_FakeResponse(status_code=200, payload=payload)])
    second_client = ClobRestClient(
        ClobClientConfig(base_url="https://example.clob"),
        session=second_session,
    )
    second_pass = second_client.get_trades("token-1", since_ts=None)

    assert len(first_pass) == 2
    assert first_pass[0].seq is None
    assert first_pass[0].trade_hash is not None
    assert first_pass[1].trade_hash is not None
    assert first_pass[0].trade_hash != first_pass[1].trade_hash
    assert [trade.trade_hash for trade in first_pass] == [trade.trade_hash for trade in second_pass]


def test_build_trade_hash_is_deterministic() -> None:
    event_ts = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    first = build_trade_hash(
        token_id="token-1",
        event_ts=event_ts,
        price=Decimal("0.50000000"),
        size=Decimal("10.00000000"),
        side="buy",
        extra_fields={"maker": "0xabc", "meta": {"a": 1, "b": [2, 3]}},
    )
    second = build_trade_hash(
        token_id="token-1",
        event_ts=event_ts,
        price=Decimal("0.50000000"),
        size=Decimal("10.00000000"),
        side="buy",
        extra_fields={"meta": {"b": [2, 3], "a": 1}, "maker": "0xabc"},
    )

    assert first == second
    assert len(first) == 64


def test_client_retries_on_429_and_uses_retry_after() -> None:
    session = _FakeSession(
        [
            _FakeResponse(status_code=429, payload={"error": "rate"}, headers={"Retry-After": "0"}),
            _FakeResponse(
                status_code=200,
                payload={"timestamp": "2026-01-01T00:00:00Z", "bids": [], "asks": []},
            ),
        ]
    )
    slept: list[float] = []

    client = ClobRestClient(
        # Disable rate-limit sleeps so the assertion isolates Retry-After behavior.
        ClobClientConfig(base_url="https://example.clob", rate_limit_rps=0),
        session=session,
        sleep_fn=slept.append,
        clock_fn=lambda: 1000.0,
    )

    snapshot = client.get_orderbook("token-1")

    assert snapshot is not None
    assert slept == [0.0]


def test_get_trades_adds_l2_headers_deterministically_when_credentials_present() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                status_code=200,
                payload=_load_fixture("trades.json"),
            )
        ]
    )
    raw_secret = b"test-secret-key-material"
    encoded_secret = base64.urlsafe_b64encode(raw_secret).decode("utf-8")
    fixed_ts = 1_700_000_000
    since_ts = datetime(2025, 12, 31, tzinfo=UTC)
    client = ClobRestClient(
        ClobClientConfig(
            base_url="https://example.clob",
            api_key="api-key",
            api_secret=encoded_secret,
            api_passphrase="passphrase",
            poly_address="0xabc",
        ),
        session=session,
        epoch_seconds_fn=lambda: fixed_ts,
    )

    _ = client.get_trades("token-1", since_ts=since_ts)

    request_headers = session.calls[0]["headers"]
    assert request_headers["POLY_API_KEY"] == "api-key"
    assert request_headers["POLY_PASSPHRASE"] == "passphrase"
    assert request_headers["POLY_ADDRESS"] == "0xabc"
    assert request_headers["POLY_TIMESTAMP"] == str(fixed_ts)

    request_path = "/data/trades"
    message = f"{fixed_ts}GET{request_path}".encode()
    expected_signature = base64.urlsafe_b64encode(
        hmac.new(raw_secret, message, hashlib.sha256).digest()
    ).decode("utf-8")
    assert request_headers["POLY_SIGNATURE"] == expected_signature


def test_get_trades_skips_l2_headers_when_credentials_are_incomplete() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                status_code=200,
                payload=_load_fixture("trades.json"),
            )
        ]
    )
    client = ClobRestClient(
        ClobClientConfig(
            base_url="https://example.clob",
            api_key="api-key-only",
        ),
        session=session,
        epoch_seconds_fn=lambda: 1_700_000_000,
    )

    _ = client.get_trades("token-1", since_ts=None)

    request_headers = session.calls[0]["headers"]
    assert "POLY_API_KEY" not in request_headers
    assert "POLY_SIGNATURE" not in request_headers


def test_get_market_trades_filters_rows_and_uses_data_api_url() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                status_code=200,
                payload=[
                    {
                        "asset": "token-1",
                        "timestamp": 1_773_309_803,
                        "price": 0.55,
                        "size": 5.0,
                        "side": "BUY",
                        "transactionHash": "0xaaa",
                    },
                    {
                        "asset": "token-2",
                        "timestamp": 1_773_309_804,
                        "price": 0.44,
                        "size": 8.0,
                        "side": "SELL",
                        "transactionHash": "0xbbb",
                    },
                ],
            ),
            _FakeResponse(status_code=200, payload=[]),
        ]
    )
    client = ClobRestClient(
        ClobClientConfig(
            base_url="https://example.clob",
            data_api_base_url="https://example.data-api",
            data_api_page_size=2,
            data_api_max_pages=2,
        ),
        session=session,
    )

    trades = client.get_market_trades(
        "0xcondition-1",
        token_id="token-1",
        since_ts=datetime(2026, 3, 10, tzinfo=UTC),
    )

    assert len(trades) == 1
    assert trades[0].token_id == "token-1"
    assert trades[0].side == "buy"
    assert trades[0].trade_hash is not None
    assert session.calls[0]["url"] == "https://example.data-api/trades"
    assert session.calls[0]["params"]["market"] == "0xcondition-1"
    assert session.calls[0]["params"]["limit"] == "2"
    assert session.calls[0]["params"]["offset"] == "0"
    assert session.calls[1]["params"]["offset"] == "2"


def test_get_market_trades_skips_invalid_condition_id_without_requests() -> None:
    session = _FakeSession([_FakeResponse(status_code=200, payload=[])])
    client = ClobRestClient(
        ClobClientConfig(
            base_url="https://example.clob",
            data_api_base_url="https://example.data-api",
        ),
        session=session,
    )

    trades = client.get_market_trades("not-a-condition-id", token_id="token-1", since_ts=None)

    assert trades == []
    assert session.calls == []
