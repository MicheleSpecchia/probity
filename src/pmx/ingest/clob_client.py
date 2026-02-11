from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import requests

DEFAULT_CLOB_BASE_URL = "https://clob.polymarket.com"
DEFAULT_CLOB_TIMEOUT_SECONDS = 20
DEFAULT_CLOB_RATE_LIMIT_RPS = 5.0
DEFAULT_CLOB_USER_AGENT = "pmx-clob-client/0.1"

_PRICE_QUANT = Decimal("0.00000001")
_SIZE_QUANT = Decimal("0.00000001")


class ClobClientError(RuntimeError):
    """Raised when CLOB API requests fail after retry attempts."""


@dataclass(frozen=True, slots=True)
class ClobClientConfig:
    base_url: str = DEFAULT_CLOB_BASE_URL
    timeout_seconds: int = DEFAULT_CLOB_TIMEOUT_SECONDS
    rate_limit_rps: float = DEFAULT_CLOB_RATE_LIMIT_RPS
    max_retries: int = 4
    backoff_seconds: float = 0.5
    api_key: str | None = None
    orderbook_depth: int | None = None


@dataclass(frozen=True, slots=True)
class OrderbookSnapshot:
    token_id: str
    event_ts: datetime
    bids: list[dict[str, str]]
    asks: list[dict[str, str]]
    mid: Decimal | None


@dataclass(frozen=True, slots=True)
class TradeRecord:
    token_id: str
    event_ts: datetime
    price: Decimal
    size: Decimal
    side: str
    trade_hash: str | None
    seq: int | None


@dataclass(frozen=True, slots=True)
class CandleRecord:
    token_id: str
    interval: str
    start_ts: datetime
    end_ts: datetime
    o: Decimal
    h: Decimal
    low: Decimal
    c: Decimal
    v: Decimal


class ClobRestClient:
    def __init__(
        self,
        config: ClobClientConfig,
        *,
        session: requests.Session | None = None,
        sleep_fn: Any = time.sleep,
        clock_fn: Any = time.monotonic,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.sleep_fn = sleep_fn
        self.clock_fn = clock_fn
        self._last_request_monotonic: float | None = None

        headers = {
            "Accept": "application/json",
            "User-Agent": DEFAULT_CLOB_USER_AGENT,
        }
        if config.api_key:
            headers["Authorization"] = f"Bearer {config.api_key}"
        self.session.headers.update(headers)

    def get_orderbook(
        self,
        token_id: str,
        *,
        fallback_event_ts: datetime | None = None,
    ) -> OrderbookSnapshot | None:
        payload = self._request_json("/book", {"token_id": token_id})
        event_ts = _parse_optional_datetime(
            payload.get("event_ts")
            or payload.get("timestamp")
            or payload.get("ts")
            or payload.get("updated_at")
            or payload.get("updatedAt")
        )
        if event_ts is None and fallback_event_ts is not None:
            event_ts = _as_utc_datetime(fallback_event_ts)
        if event_ts is None:
            return None

        bids, asks = normalize_orderbook(
            payload.get("bids"),
            payload.get("asks"),
            max_depth=self.config.orderbook_depth,
        )
        mid = _parse_optional_decimal(payload.get("mid"), quant=_PRICE_QUANT)

        return OrderbookSnapshot(
            token_id=token_id,
            event_ts=event_ts,
            bids=bids,
            asks=asks,
            mid=mid,
        )

    def get_trades(
        self,
        token_id: str,
        *,
        since_ts: datetime | None = None,
    ) -> list[TradeRecord]:
        since_bound = _as_utc_datetime(since_ts) if since_ts is not None else None
        params: dict[str, str] = {"token_id": token_id}
        if since_bound is not None:
            params["since"] = since_bound.isoformat()

        payload = self._request_json("/trades", params)
        rows = _extract_rows(payload, preferred_key="trades")

        output: list[TradeRecord] = []
        for row in rows:
            event_ts = _parse_optional_datetime(
                row.get("event_ts")
                or row.get("timestamp")
                or row.get("ts")
                or row.get("created_at")
                or row.get("createdAt")
            )
            price = _parse_optional_decimal(row.get("price"), quant=_PRICE_QUANT)
            size = _parse_optional_decimal(
                row.get("size") or row.get("amount"),
                quant=_SIZE_QUANT,
            )
            if event_ts is None or price is None or size is None:
                continue
            if since_bound is not None and event_ts < since_bound:
                continue

            side = _normalize_side(row.get("side"))
            seq = _parse_optional_int(row.get("seq") or row.get("sequence"))
            trade_hash = _optional_text(row.get("trade_hash") or row.get("hash") or row.get("id"))
            if seq is None and trade_hash is None:
                trade_hash = build_trade_hash(
                    token_id=token_id,
                    event_ts=event_ts,
                    price=price,
                    size=size,
                    side=side,
                    extra_fields=_trade_identity_extra_fields(row),
                )

            output.append(
                TradeRecord(
                    token_id=token_id,
                    event_ts=event_ts,
                    price=price,
                    size=size,
                    side=side,
                    trade_hash=trade_hash,
                    seq=seq,
                )
            )

        return output

    def get_candles(
        self,
        token_id: str,
        *,
        interval: str,
        since_ts: datetime | None = None,
    ) -> list[CandleRecord]:
        since_bound = _as_utc_datetime(since_ts) if since_ts is not None else None
        params: dict[str, str] = {"token_id": token_id, "interval": interval}
        if since_bound is not None:
            params["since"] = since_bound.isoformat()

        payload = self._request_json("/candles", params)
        rows = _extract_rows(payload, preferred_key="candles")

        output: list[CandleRecord] = []
        for row in rows:
            start_ts = _parse_optional_datetime(
                row.get("start_ts") or row.get("start") or row.get("startTime") or row.get("t")
            )
            end_ts = _parse_optional_datetime(
                row.get("end_ts") or row.get("end") or row.get("endTime")
            )
            o = _parse_optional_decimal(row.get("o") or row.get("open"), quant=_PRICE_QUANT)
            h = _parse_optional_decimal(row.get("h") or row.get("high"), quant=_PRICE_QUANT)
            low = _parse_optional_decimal(row.get("l") or row.get("low"), quant=_PRICE_QUANT)
            c = _parse_optional_decimal(row.get("c") or row.get("close"), quant=_PRICE_QUANT)
            v = _parse_optional_decimal(row.get("v") or row.get("volume"), quant=_SIZE_QUANT)

            if None in (start_ts, end_ts, o, h, low, c, v):
                continue
            if since_bound is not None and start_ts < since_bound:
                continue

            output.append(
                CandleRecord(
                    token_id=token_id,
                    interval=interval,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    o=o,
                    h=h,
                    low=low,
                    c=c,
                    v=v,
                )
            )

        return output

    def _request_json(self, path: str, params: Mapping[str, str]) -> Any:
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"

        for attempt in range(self.config.max_retries + 1):
            self._wait_for_rate_limit()
            try:
                response = self.session.get(
                    url,
                    params=dict(params),
                    timeout=self.config.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.config.max_retries:
                    raise ClobClientError(f"CLOB request failed for {path}") from exc
                self.sleep_fn(_retry_delay_seconds(attempt, None, self.config.backoff_seconds))
                continue
            finally:
                self._last_request_monotonic = self.clock_fn()

            if response.status_code == 429 or response.status_code >= 500:
                if attempt >= self.config.max_retries:
                    raise ClobClientError(
                        f"CLOB request failed with status={response.status_code} for {path}"
                    )
                retry_after = response.headers.get("Retry-After")
                self.sleep_fn(
                    _retry_delay_seconds(
                        attempt,
                        retry_after,
                        self.config.backoff_seconds,
                    )
                )
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                raise ClobClientError(
                    f"CLOB request failed with status={response.status_code} for {path}"
                ) from exc

            try:
                return response.json()
            except ValueError as exc:
                raise ClobClientError(f"CLOB response is not valid JSON for {path}") from exc

        raise ClobClientError(f"CLOB request exhausted retries for {path}")

    def _wait_for_rate_limit(self) -> None:
        if self.config.rate_limit_rps <= 0:
            return
        if self._last_request_monotonic is None:
            return

        min_interval = 1.0 / self.config.rate_limit_rps
        elapsed = self.clock_fn() - self._last_request_monotonic
        remaining = min_interval - elapsed
        if remaining > 0:
            self.sleep_fn(remaining)


def _extract_rows(payload: Any, *, preferred_key: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    candidates = (
        preferred_key,
        "data",
        "results",
        "items",
    )
    for key in candidates:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    return []


def normalize_orderbook(
    raw_bids: Any,
    raw_asks: Any,
    *,
    max_depth: int | None = None,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    bids = _normalize_order_levels(raw_bids, descending=True, max_depth=max_depth)
    asks = _normalize_order_levels(raw_asks, descending=False, max_depth=max_depth)
    return bids, asks


def build_trade_hash(
    *,
    token_id: str,
    event_ts: datetime,
    price: Decimal,
    size: Decimal,
    side: str,
    extra_fields: Mapping[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "token_id": token_id,
        "event_ts": _as_utc_datetime(event_ts).isoformat(timespec="microseconds"),
        "price": _decimal_to_str(price),
        "size": _decimal_to_str(size),
        "side": side,
    }
    if extra_fields:
        payload["extra"] = _normalize_for_hash(extra_fields)

    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_order_levels(
    raw: Any,
    *,
    descending: bool,
    max_depth: int | None,
) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        return []

    levels: list[tuple[Decimal, Decimal]] = []
    for item in raw:
        if isinstance(item, Mapping):
            price = _parse_optional_decimal(item.get("price"), quant=_PRICE_QUANT)
            size = _parse_optional_decimal(
                item.get("size") or item.get("amount"),
                quant=_SIZE_QUANT,
            )
            if price is None or size is None:
                continue
            levels.append((price, size))
            continue

        if isinstance(item, (list, tuple)) and len(item) >= 2:
            price = _parse_optional_decimal(item[0], quant=_PRICE_QUANT)
            size = _parse_optional_decimal(item[1], quant=_SIZE_QUANT)
            if price is None or size is None:
                continue
            levels.append((price, size))

    levels.sort(key=lambda level: (level[0], level[1]), reverse=descending)
    if max_depth is not None:
        if max_depth <= 0:
            return []
        levels = levels[:max_depth]

    return [
        {
            "price": _decimal_to_str(price),
            "size": _decimal_to_str(size),
        }
        for price, size in levels
    ]


def _trade_identity_extra_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    excluded_keys = {
        "event_ts",
        "timestamp",
        "ts",
        "created_at",
        "createdAt",
        "price",
        "size",
        "amount",
        "side",
        "trade_hash",
        "hash",
        "id",
        "seq",
        "sequence",
    }
    extras: dict[str, Any] = {}
    for key in sorted(row.keys(), key=str):
        normalized_key = str(key)
        if normalized_key in excluded_keys:
            continue
        value = row.get(key)
        if value is None:
            continue
        extras[normalized_key] = value
    return extras


def _normalize_for_hash(value: Any) -> Any:
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda item: str(item[0]))
        return {str(key): _normalize_for_hash(item_value) for key, item_value in items}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_hash(item_value) for item_value in value]
    if isinstance(value, datetime):
        return _as_utc_datetime(value).isoformat(timespec="microseconds")
    if isinstance(value, Decimal):
        return _decimal_to_str(value)
    return value


def _normalize_side(raw: Any) -> str:
    text = _optional_text(raw)
    if text is None:
        return "unknown"
    lowered = text.lower()
    if lowered in {"buy", "bid", "b"}:
        return "buy"
    if lowered in {"sell", "ask", "s"}:
        return "sell"
    return "unknown"


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
        stripped = raw.strip()
        if not stripped:
            return None
        normalized = stripped.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return _as_utc_datetime(parsed)
    return None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_optional_decimal(raw: Any, *, quant: Decimal) -> Decimal | None:
    if raw is None:
        return None
    try:
        decimal = Decimal(str(raw)).quantize(quant)
    except (InvalidOperation, ValueError):
        return None
    return decimal


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


def _decimal_to_str(value: Decimal) -> str:
    return format(value, "f")


def _retry_delay_seconds(
    attempt: int,
    retry_after_header: str | None,
    base_backoff_seconds: float,
) -> float:
    if retry_after_header:
        try:
            retry_after = float(retry_after_header)
            if retry_after >= 0:
                return retry_after
        except ValueError:
            pass

    exponential = base_backoff_seconds * (2**attempt)
    deterministic_jitter = min(0.05 * (attempt + 1), 0.25)
    return exponential + deterministic_jitter
