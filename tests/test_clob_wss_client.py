from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from pmx.ingest.clob_wss_client import (
    ClobReconnectEvent,
    ClobStreamEvent,
    ClobWssClient,
    ClobWssConfig,
    extract_seq,
    parse_stream_message,
)


class _FakeConnection:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = list(messages)
        self.sent_payloads: list[dict[str, Any]] = []
        self.closed = False

    def send_json(self, payload: dict[str, Any]) -> None:
        self.sent_payloads.append(dict(payload))

    def recv_json(self, *, timeout_seconds: float | None = None) -> Any:
        _ = timeout_seconds
        if not self.messages:
            raise EOFError("connection closed")
        return self.messages.pop(0)

    def close(self) -> None:
        self.closed = True


class _FakeTransport:
    def __init__(self, connect_results: list[Any]) -> None:
        self.connect_results = list(connect_results)
        self.calls: list[dict[str, Any]] = []

    def connect(self, *, url: str, timeout_seconds: int) -> _FakeConnection:
        self.calls.append({"url": url, "timeout_seconds": timeout_seconds})
        if not self.connect_results:
            raise RuntimeError("no fake connection configured")
        result = self.connect_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_parse_stream_message_extracts_and_sorts_events() -> None:
    payload = {
        "type": "trades",
        "events": [
            {
                "token_id": "token-b",
                "timestamp": "2026-01-01T00:00:02Z",
                "price": "0.6",
                "size": "1",
                "seq": 2,
            },
            {
                "token_id": "token-a",
                "timestamp": "2026-01-01T00:00:01Z",
                "price": "0.5",
                "size": "1",
                "seq": 1,
            },
        ],
    }

    events = parse_stream_message(payload)

    assert len(events) == 2
    assert [event.token_id for event in events] == ["token-a", "token-b"]
    assert [event.channel for event in events] == ["trade", "trade"]
    assert events[0].event_ts == datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    assert events[1].seq == 2


def test_parse_stream_message_supports_configurable_seq_field() -> None:
    payload = {
        "token_id": "token-a",
        "type": "trade",
        "timestamp": "2026-01-01T00:00:01Z",
        "offset": 42,
        "price": "0.5",
        "size": "1",
    }

    events = parse_stream_message(payload, seq_field="offset")

    assert len(events) == 1
    assert events[0].seq == 42


def test_parse_stream_message_supports_default_seq_aliases() -> None:
    payload = {
        "events": [
            {
                "token_id": "token-a",
                "type": "trade",
                "timestamp": "2026-01-01T00:00:01Z",
                "sequence": 7,
                "price": "0.5",
                "size": "1",
            },
            {
                "token_id": "token-a",
                "type": "trade",
                "timestamp": "2026-01-01T00:00:02Z",
                "offset": 8,
                "price": "0.5",
                "size": "1",
            },
        ]
    }

    events = parse_stream_message(payload)

    assert len(events) == 2
    assert [event.seq for event in events] == [7, 8]


def test_parse_stream_message_can_disable_seq_parsing() -> None:
    payload = {
        "token_id": "token-a",
        "type": "trade",
        "timestamp": "2026-01-01T00:00:01Z",
        "seq": 42,
        "price": "0.5",
        "size": "1",
    }

    events = parse_stream_message(payload, seq_fields=())

    assert len(events) == 1
    assert events[0].seq is None


@pytest.mark.parametrize(
    ("field_name", "payload", "expected_seq"),
    [
        ("seq", {"seq": "10"}, 10),
        ("sequence", {"sequence": 11}, 11),
        ("offset", {"offset": 12}, 12),
        ("missing", {"event_id": "abc"}, None),
    ],
)
def test_extract_seq_uses_field_order(
    field_name: str,
    payload: dict[str, Any],
    expected_seq: int | None,
) -> None:
    fields = (field_name,) if field_name != "missing" else ("seq", "sequence", "offset")
    seq = extract_seq(payload, fields)
    assert seq == expected_seq


def test_extract_seq_prefers_first_configured_field() -> None:
    payload = {
        "seq": 3,
        "sequence": 2,
        "nested": {"offset": 1},
    }
    seq = extract_seq(payload, ("sequence", "seq", "offset"))
    assert seq == 2


def test_extract_seq_supports_nested_payloads() -> None:
    payload = {
        "token_id": "token-a",
        "data": {
            "trade": {
                "offset": "17",
            }
        },
    }
    seq = extract_seq(payload, ("seq", "sequence", "offset"))
    assert seq == 17


def test_extract_seq_ignores_blank_field_names() -> None:
    payload = {
        "": 999,
        "seq": 10,
    }
    seq = extract_seq(payload, ("", "   "))
    assert seq is None


def test_wss_client_reconnects_with_deterministic_backoff() -> None:
    connection = _FakeConnection(
        [
            {
                "token_id": "token-a",
                "type": "trade",
                "timestamp": "2026-01-01T00:00:01Z",
                "price": "0.5",
                "size": "1",
                "seq": 7,
            }
        ]
    )
    transport = _FakeTransport([RuntimeError("boom"), connection])
    slept: list[float] = []

    client = ClobWssClient(
        ClobWssConfig(
            base_url="wss://example.clob/ws",
            timeout_seconds=20,
            max_reconnect_attempts=3,
            backoff_seconds=0.5,
            max_backoff_seconds=30.0,
        ),
        transport=transport,
        sleep_fn=slept.append,
    )

    iterator = client.listen(["token-b", "token-a"], max_messages=1)
    first = next(iterator)
    second = next(iterator)

    assert isinstance(first, ClobReconnectEvent)
    assert first.attempt == 1
    assert first.delay_seconds == 0.55
    assert slept == [0.55]

    assert isinstance(second, ClobStreamEvent)
    assert second.token_id == "token-a"
    assert second.channel == "trade"

    assert connection.sent_payloads == [
        {
            "type": "subscribe",
            "token_ids": ["token-a", "token-b"],
        }
    ]
    assert connection.closed is True

    with pytest.raises(StopIteration):
        next(iterator)
