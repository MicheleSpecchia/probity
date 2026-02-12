from __future__ import annotations

import importlib
import json
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

DEFAULT_CLOB_WSS_URL = "wss://clob.polymarket.com/ws"
DEFAULT_CLOB_WSS_TIMEOUT_SECONDS = 20
DEFAULT_CLOB_WSS_SEQ_FIELDS: tuple[str, ...] = ("seq", "sequence", "offset")


class ClobWssError(RuntimeError):
    """Raised when the CLOB WSS listener cannot recover from disconnections."""


@dataclass(frozen=True, slots=True)
class ClobWssConfig:
    base_url: str = DEFAULT_CLOB_WSS_URL
    timeout_seconds: int = DEFAULT_CLOB_WSS_TIMEOUT_SECONDS
    max_reconnect_attempts: int = 8
    backoff_seconds: float = 0.5
    max_backoff_seconds: float = 30.0
    seq_field: str | None = None
    seq_fields: tuple[str, ...] = DEFAULT_CLOB_WSS_SEQ_FIELDS


@dataclass(frozen=True, slots=True)
class ClobStreamEvent:
    token_id: str
    channel: str
    event_ts: datetime | None
    seq: int | None
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ClobReconnectEvent:
    attempt: int
    delay_seconds: float
    error_type: str
    error_message: str


class WssConnection(Protocol):
    def send_json(self, payload: Mapping[str, Any]) -> None: ...

    def recv_json(self, *, timeout_seconds: float | None = None) -> Any: ...

    def close(self) -> None: ...


class WssTransport(Protocol):
    def connect(self, *, url: str, timeout_seconds: int) -> WssConnection: ...


class _WebsocketClientConnection:
    def __init__(self, ws: Any) -> None:
        self._ws = ws

    def send_json(self, payload: Mapping[str, Any]) -> None:
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=True)
        self._ws.send(encoded)

    def recv_json(self, *, timeout_seconds: float | None = None) -> Any:
        if timeout_seconds is not None and hasattr(self._ws, "settimeout"):
            self._ws.settimeout(timeout_seconds)
        raw = self._ws.recv()
        if raw is None:
            raise ClobWssError("CLOB WSS returned empty frame")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ClobWssError("CLOB WSS frame is not valid JSON") from exc

    def close(self) -> None:
        try:
            self._ws.close()
        except Exception:
            return


class WebsocketClientTransport:
    """Default websocket transport based on the optional websocket-client package."""

    def connect(self, *, url: str, timeout_seconds: int) -> WssConnection:
        try:
            websocket = importlib.import_module("websocket")
        except ImportError as exc:
            raise ClobWssError(
                "websocket-client is required for live WSS usage. Install 'websocket-client'."
            ) from exc

        ws = websocket.create_connection(url, timeout=timeout_seconds)
        return _WebsocketClientConnection(ws)


class ClobWssClient:
    def __init__(
        self,
        config: ClobWssConfig,
        *,
        transport: WssTransport | None = None,
        sleep_fn: Any = time.sleep,
        clock_fn: Any = time.monotonic,
    ) -> None:
        self.config = config
        self.transport = transport or WebsocketClientTransport()
        self.sleep_fn = sleep_fn
        self.clock_fn = clock_fn

    def listen(
        self,
        token_ids: Sequence[str],
        *,
        run_seconds: int | None = None,
        max_messages: int | None = None,
    ) -> Iterator[ClobStreamEvent | ClobReconnectEvent]:
        canonical_token_ids = sorted({token.strip() for token in token_ids if token.strip()})
        if not canonical_token_ids:
            return

        token_set = set(canonical_token_ids)
        reconnect_attempt = 0
        emitted_messages = 0
        deadline = self.clock_fn() + run_seconds if run_seconds is not None else None

        while True:
            if deadline is not None and self.clock_fn() >= deadline:
                return

            connection: WssConnection | None = None
            try:
                connection = self.transport.connect(
                    url=self.config.base_url,
                    timeout_seconds=self.config.timeout_seconds,
                )
                reconnect_attempt = 0
                self._send_subscribe(connection, canonical_token_ids)

                while True:
                    timeout_seconds: float | None = float(self.config.timeout_seconds)
                    if deadline is not None:
                        remaining = deadline - self.clock_fn()
                        if remaining <= 0:
                            return
                        timeout_seconds = min(timeout_seconds, remaining)

                    assert connection is not None
                    message = connection.recv_json(timeout_seconds=timeout_seconds)
                    for event in parse_stream_message(
                        message,
                        seq_field=self.config.seq_field,
                        seq_fields=self.config.seq_fields,
                    ):
                        if event.token_id not in token_set:
                            continue
                        emitted_messages += 1
                        should_stop = max_messages is not None and emitted_messages >= max_messages
                        if should_stop and connection is not None:
                            connection.close()
                            connection = None
                        yield event
                        if should_stop:
                            return
            except Exception as exc:
                reconnect_attempt += 1
                if reconnect_attempt > self.config.max_reconnect_attempts:
                    raise ClobWssError(
                        "CLOB WSS reconnect attempts exhausted after "
                        f"{self.config.max_reconnect_attempts}"
                    ) from exc

                delay_seconds = _reconnect_delay_seconds(
                    reconnect_attempt - 1,
                    self.config.backoff_seconds,
                    self.config.max_backoff_seconds,
                )
                yield ClobReconnectEvent(
                    attempt=reconnect_attempt,
                    delay_seconds=delay_seconds,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                self.sleep_fn(delay_seconds)
            finally:
                if connection is not None:
                    connection.close()

    def _send_subscribe(self, connection: WssConnection, token_ids: list[str]) -> None:
        payload = {
            "type": "subscribe",
            "token_ids": token_ids,
        }
        connection.send_json(payload)


def parse_stream_message(
    payload: Any,
    *,
    seq_field: str | None = None,
    seq_fields: Sequence[str] | None = None,
) -> list[ClobStreamEvent]:
    rows = _extract_rows(payload)
    resolved_seq_fields = _resolve_seq_fields(seq_field=seq_field, seq_fields=seq_fields)
    events: list[ClobStreamEvent] = []
    for row in rows:
        token_id = _optional_text(
            row.get("token_id")
            or row.get("tokenId")
            or row.get("asset_id")
            or row.get("assetId")
            or row.get("market")
        )
        if token_id is None:
            continue

        channel = _detect_channel(row)
        event_ts = _parse_optional_datetime(
            row.get("event_ts")
            or row.get("timestamp")
            or row.get("ts")
            or row.get("created_at")
            or row.get("createdAt")
            or row.get("updated_at")
            or row.get("updatedAt")
        )
        seq = extract_seq(row, resolved_seq_fields)

        events.append(
            ClobStreamEvent(
                token_id=token_id,
                channel=channel,
                event_ts=event_ts,
                seq=seq,
                payload=row,
            )
        )

    events.sort(
        key=lambda event: (
            event.token_id,
            event.event_ts or datetime.min.replace(tzinfo=UTC),
            event.seq if event.seq is not None else 0,
        )
    )
    return events


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        return []

    flattened = _flatten_row(payload)
    rows: list[dict[str, Any]] = []
    candidates: tuple[str, ...] = (
        "events",
        "data",
        "items",
        "results",
        "trades",
        "book_updates",
        "books",
    )
    for key in candidates:
        value = payload.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    rows.append(_flatten_row(item, inherited=flattened))
            if rows:
                return rows
        if isinstance(value, dict):
            rows.append(_flatten_row(value, inherited=flattened))
            return rows

    return [flattened]


def _flatten_row(
    row: Mapping[str, Any],
    *,
    inherited: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if inherited:
        excluded_keys = {
            "events",
            "items",
            "results",
            "trades",
            "book_updates",
            "books",
            "data",
            "seq",
            "sequence",
            "offset",
        }
        for key in sorted(inherited.keys(), key=str):
            key_text = str(key)
            if key_text in excluded_keys:
                continue
            inherited_value = inherited[key]
            if isinstance(inherited_value, (Mapping, list, tuple)):
                continue
            merged[key_text] = inherited_value

    data_payload = row.get("data")
    if isinstance(data_payload, Mapping):
        for key in sorted(data_payload.keys(), key=str):
            merged[str(key)] = data_payload[key]

    for key in sorted(row.keys(), key=str):
        key_text = str(key)
        if key_text == "data":
            continue
        merged[key_text] = row[key]
    return merged


def _detect_channel(row: Mapping[str, Any]) -> str:
    candidate = _optional_text(
        row.get("channel") or row.get("type") or row.get("event") or row.get("topic")
    )
    if candidate:
        lowered = candidate.lower()
        if "trade" in lowered:
            return "trade"
        if "book" in lowered or "orderbook" in lowered:
            return "orderbook"

    has_trade_shape = row.get("price") is not None and (
        row.get("size") is not None or row.get("amount") is not None
    )
    if has_trade_shape:
        return "trade"
    if row.get("bids") is not None or row.get("asks") is not None:
        return "orderbook"
    return "unknown"


def extract_seq(row: Mapping[str, Any], fields: Sequence[str]) -> int | None:
    normalized_fields = _normalize_seq_fields(fields)
    if not normalized_fields:
        return None

    for field_name in normalized_fields:
        for value in _iter_field_values(row, field_name):
            parsed = _parse_optional_int(value)
            if parsed is not None:
                return parsed
    return None


def _resolve_seq_fields(
    *,
    seq_field: str | None,
    seq_fields: Sequence[str] | None,
) -> tuple[str, ...]:
    if seq_field is not None:
        legacy = seq_field.strip()
        if not legacy:
            return ()
        return (legacy,)

    if seq_fields is None:
        return DEFAULT_CLOB_WSS_SEQ_FIELDS
    return _normalize_seq_fields(seq_fields)


def _normalize_seq_fields(fields: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for field in fields:
        text = str(field).strip()
        if not text:
            continue
        if text not in normalized:
            normalized.append(text)
    return tuple(normalized)


def _iter_field_values(value: Any, field_name: str) -> Iterator[Any]:
    if isinstance(value, Mapping):
        for key in sorted(value.keys(), key=str):
            child = value[key]
            if str(key) == field_name:
                yield child
            if isinstance(child, (Mapping, list)):
                yield from _iter_field_values(child, field_name)
        return

    if isinstance(value, list):
        for child in value:
            yield from _iter_field_values(child, field_name)


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


def _reconnect_delay_seconds(
    attempt: int,
    base_backoff_seconds: float,
    max_backoff_seconds: float,
) -> float:
    exponential = base_backoff_seconds * (2**attempt)
    deterministic_jitter = min(0.05 * (attempt + 1), 0.25)
    return float(min(exponential + deterministic_jitter, max_backoff_seconds))
