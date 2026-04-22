from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

import requests

DEFAULT_CLOB_BASE_URL = "https://clob.polymarket.com"
DEFAULT_DATA_API_BASE_URL = "https://data-api.polymarket.com"
DEFAULT_CLOB_TIMEOUT_SECONDS = 20
DEFAULT_CLOB_RATE_LIMIT_RPS = 5.0
DEFAULT_CLOB_USER_AGENT = "pmx-clob-client/0.1"
_DATA_API_MAX_HISTORICAL_OFFSET = 3000

_PRICE_QUANT = Decimal("0.00000001")
_SIZE_QUANT = Decimal("0.00000001")


class ClobClientError(RuntimeError):
    """Raised when CLOB API requests fail after retry attempts."""


class ClobHttpError(ClobClientError):
    """Raised for non-retriable HTTP errors returned by CLOB."""

    def __init__(self, *, status_code: int, path: str, body_snippet: str | None = None) -> None:
        self.status_code = status_code
        self.path = path
        self.body_snippet = body_snippet

        message = f"CLOB request failed with status={status_code} for {path}"
        if body_snippet:
            message = f"{message}: {body_snippet}"
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ClobClientConfig:
    base_url: str = DEFAULT_CLOB_BASE_URL
    data_api_base_url: str = DEFAULT_DATA_API_BASE_URL
    timeout_seconds: int = DEFAULT_CLOB_TIMEOUT_SECONDS
    rate_limit_rps: float = DEFAULT_CLOB_RATE_LIMIT_RPS
    max_retries: int = 4
    backoff_seconds: float = 0.5
    data_api_page_size: int = 200
    data_api_max_pages: int = 16
    api_key: str | None = None
    api_secret: str | None = None
    api_passphrase: str | None = None
    poly_address: str | None = None
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
        epoch_seconds_fn: Any = time.time,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.sleep_fn = sleep_fn
        self.clock_fn = clock_fn
        self.epoch_seconds_fn = epoch_seconds_fn
        self._last_request_monotonic: float | None = None

        headers = {
            "Accept": "application/json",
            "User-Agent": DEFAULT_CLOB_USER_AGENT,
        }
        self.session.headers.update(headers)

    def get_orderbook(
        self,
        token_id: str,
        *,
        fallback_event_ts: datetime | None = None,
    ) -> OrderbookSnapshot | None:
        payload = self._request_json("/book", {"token_id": token_id}, private=False)
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
        next_cursor = "MA=="
        output: list[TradeRecord] = []
        max_pages = 64

        for _ in range(max_pages):
            params = _build_data_trades_params(
                token_id,
                next_cursor=next_cursor,
                since_ts=since_bound,
            )
            payload = self._request_json(
                "/data/trades",
                params,
                private=True,
                sign_with_query=False,
            )
            rows = _extract_rows(payload, preferred_key="trades")
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
                trade_hash = _optional_text(
                    row.get("trade_hash") or row.get("hash") or row.get("id")
                )
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

            if not isinstance(payload, Mapping):
                break
            cursor_raw = payload.get("next_cursor")
            cursor = _optional_text(cursor_raw)
            if cursor is None or cursor.upper() == "LTE=":
                break
            next_cursor = cursor

        return output

    def get_market_trades(
        self,
        condition_id: str,
        *,
        token_id: str,
        since_ts: datetime | None = None,
    ) -> list[TradeRecord]:
        condition = _optional_text(condition_id)
        if condition is None or not condition.startswith("0x"):
            return []

        since_bound = _as_utc_datetime(since_ts) if since_ts is not None else None
        page_size = max(1, self.config.data_api_page_size)
        max_pages = max(1, self.config.data_api_max_pages)
        max_pages_by_offset = (_DATA_API_MAX_HISTORICAL_OFFSET // page_size) + 1
        max_pages = min(max_pages, max_pages_by_offset)
        output: list[TradeRecord] = []
        seen_keys: set[tuple[str, datetime, int, str]] = set()

        for page_index in range(max_pages):
            params = {
                "market": condition,
                "limit": str(page_size),
                "offset": str(page_index * page_size),
            }
            try:
                payload = self._request_json(
                    "/trades",
                    params,
                    private=False,
                    base_url_override=self.config.data_api_base_url,
                )
            except ClobHttpError as exc:
                if (
                    exc.path == "/trades"
                    and exc.status_code == 400
                    and (exc.body_snippet or "").lower().find("offset") >= 0
                ):
                    break
                raise
            rows = _extract_rows(payload, preferred_key="trades")
            if not rows:
                break
            oldest_page_ts: datetime | None = None

            for row in rows:
                page_event_ts = _parse_optional_datetime(
                    row.get("event_ts")
                    or row.get("timestamp")
                    or row.get("ts")
                    or row.get("created_at")
                    or row.get("createdAt")
                )
                if page_event_ts is not None:
                    if oldest_page_ts is None or page_event_ts < oldest_page_ts:
                        oldest_page_ts = page_event_ts

                row_token_id = _optional_text(
                    row.get("asset")
                    or row.get("token_id")
                    or row.get("tokenId")
                    or row.get("asset_id")
                )
                if row_token_id != token_id:
                    continue

                event_ts = page_event_ts
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
                trade_hash = build_trade_hash(
                    token_id=token_id,
                    event_ts=event_ts,
                    price=price,
                    size=size,
                    side=side,
                    extra_fields=_trade_identity_extra_fields(row),
                )
                dedupe_key = (token_id, event_ts, seq or 0, trade_hash)
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)

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

            if len(rows) < page_size:
                break
            if (
                since_bound is not None
                and oldest_page_ts is not None
                and oldest_page_ts < since_bound
            ):
                break

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

        payload = self._request_json("/candles", params, private=False)
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

            if (
                start_ts is None
                or end_ts is None
                or o is None
                or h is None
                or low is None
                or c is None
                or v is None
            ):
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

    def _request_json(
        self,
        path: str,
        params: Mapping[str, str],
        *,
        private: bool,
        sign_with_query: bool = True,
        base_url_override: str | None = None,
    ) -> Any:
        base_url = _optional_text(base_url_override) or self.config.base_url
        url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
        sorted_params = sorted(params.items())
        per_request_headers = (
            self._build_l2_headers(
                path,
                "GET",
                sorted_params,
                include_query=sign_with_query,
            )
            if private
            else {}
        )

        for attempt in range(self.config.max_retries + 1):
            self._wait_for_rate_limit()
            try:
                response = self.session.get(
                    url,
                    params=sorted_params,
                    headers=per_request_headers if per_request_headers else None,
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
                raise ClobHttpError(
                    status_code=response.status_code,
                    path=path,
                    body_snippet=_response_body_snippet(response),
                ) from exc

            try:
                return response.json()
            except ValueError as exc:
                raise ClobClientError(f"CLOB response is not valid JSON for {path}") from exc

        raise ClobClientError(f"CLOB request exhausted retries for {path}")

    def _build_l2_headers(
        self,
        path: str,
        method: str,
        sorted_params: list[tuple[str, str]],
        *,
        include_query: bool,
    ) -> dict[str, str]:
        api_key = _optional_text(self.config.api_key)
        api_secret = _optional_text(self.config.api_secret)
        api_passphrase = _optional_text(self.config.api_passphrase)
        poly_address = _optional_text(self.config.poly_address)
        if api_key is None or api_secret is None or api_passphrase is None or poly_address is None:
            return {}

        timestamp = str(int(self.epoch_seconds_fn()))
        request_path = _build_request_path(path, sorted_params, include_query=include_query)
        signature = _build_l2_hmac_signature(
            secret=api_secret,
            timestamp=timestamp,
            method=method,
            request_path=request_path,
        )
        return {
            "POLY_ADDRESS": poly_address,
            "POLY_API_KEY": api_key,
            "POLY_PASSPHRASE": api_passphrase,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": timestamp,
        }

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
                return float(retry_after)
        except ValueError:
            pass

    exponential = base_backoff_seconds * (2**attempt)
    deterministic_jitter = min(0.05 * (attempt + 1), 0.25)
    return float(exponential + deterministic_jitter)


def _build_data_trades_params(
    token_id: str,
    *,
    next_cursor: str,
    since_ts: datetime | None,
) -> dict[str, str]:
    params: dict[str, str] = {"asset_id": token_id, "next_cursor": next_cursor}
    if since_ts is not None:
        params["after"] = str(int(since_ts.timestamp()))
    return params


def _build_request_path(
    path: str,
    sorted_params: list[tuple[str, str]],
    *,
    include_query: bool,
) -> str:
    normalized_path = f"/{path.lstrip('/')}"
    if not include_query or not sorted_params:
        return normalized_path
    return f"{normalized_path}?{urlencode(sorted_params)}"


def _build_l2_hmac_signature(
    *,
    secret: str,
    timestamp: str,
    method: str,
    request_path: str,
) -> str:
    message = f"{timestamp}{method.upper()}{request_path}"
    digest = hmac.new(
        base64.urlsafe_b64decode(secret),
        message.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8")


def _response_body_snippet(response: requests.Response) -> str | None:
    text = response.text
    if not text:
        return None
    compact = " ".join(text.split())
    if not compact:
        return None
    return compact[:180]
