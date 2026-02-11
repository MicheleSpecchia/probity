from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests

DEFAULT_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_GAMMA_TIMEOUT_SECONDS = 20
DEFAULT_GAMMA_PAGE_SIZE = 200
DEFAULT_GAMMA_USER_AGENT = "pmx-gamma-client/0.1"


class GammaClientError(RuntimeError):
    """Raised when Gamma API requests fail after retry attempts."""


@dataclass(frozen=True, slots=True)
class GammaClientConfig:
    base_url: str = DEFAULT_GAMMA_BASE_URL
    timeout_seconds: int = DEFAULT_GAMMA_TIMEOUT_SECONDS
    page_size: int = DEFAULT_GAMMA_PAGE_SIZE
    max_retries: int = 4
    backoff_seconds: float = 0.5


def extract_market_page(payload: Any) -> tuple[list[dict[str, Any]], dict[str, int | str]]:
    """Extract market rows and next-page parameters from Gamma payload."""
    markets = _extract_markets(payload)
    next_params = _extract_next_params(payload)
    return markets, next_params


class GammaClient:
    def __init__(
        self,
        config: GammaClientConfig,
        *,
        session: requests.Session | None = None,
        sleep_fn: Any = time.sleep,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": DEFAULT_GAMMA_USER_AGENT,
            }
        )
        self.sleep_fn = sleep_fn

    def iter_markets(
        self,
        *,
        since_updated_at: datetime | None = None,
        max_markets: int | None = None,
    ) -> list[dict[str, Any]]:
        base_params: dict[str, int | str] = {"limit": self.config.page_size}
        if since_updated_at is not None:
            base_params["updated_after"] = _as_utc_iso(since_updated_at)

        request_params: dict[str, int | str] = dict(base_params)
        seen_pages: set[tuple[tuple[str, int | str], ...]] = set()
        output: list[dict[str, Any]] = []

        while True:
            page_key = tuple(sorted(request_params.items(), key=lambda item: item[0]))
            if page_key in seen_pages:
                break
            seen_pages.add(page_key)

            payload = self._request_json("/markets", request_params)
            markets, next_params = extract_market_page(payload)

            if not markets:
                break

            for market in markets:
                if since_updated_at is not None and not _updated_after(market, since_updated_at):
                    continue
                output.append(market)
                if max_markets is not None and len(output) >= max_markets:
                    return output

            if (
                not next_params
                and isinstance(payload, list)
                and len(markets) >= self.config.page_size
            ):
                current_offset = int(request_params.get("offset", 0))
                next_params = {"offset": current_offset + self.config.page_size}

            if not next_params:
                break

            request_params = dict(base_params)
            request_params.update(next_params)

        return output

    def _request_json(self, path: str, params: Mapping[str, int | str]) -> Any:
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"

        for attempt in range(self.config.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=dict(params),
                    timeout=self.config.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.config.max_retries:
                    raise GammaClientError(f"Gamma request failed for {path}") from exc
                self.sleep_fn(_retry_delay_seconds(attempt, None, self.config.backoff_seconds))
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt >= self.config.max_retries:
                    raise GammaClientError(
                        f"Gamma request failed with status={response.status_code} for {path}"
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
                raise GammaClientError(
                    f"Gamma request failed with status={response.status_code} for {path}"
                ) from exc

            try:
                return response.json()
            except ValueError as exc:
                raise GammaClientError(f"Gamma response is not valid JSON for {path}") from exc

        raise GammaClientError(f"Gamma request exhausted retries for {path}")


def _extract_markets(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        return []

    candidate_keys = ("markets", "data", "results", "items")
    for key in candidate_keys:
        candidate = payload.get(key)
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
        if isinstance(candidate, dict):
            nested = candidate.get("markets") or candidate.get("items") or candidate.get("results")
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]

    return []


def _extract_next_params(payload: Any) -> dict[str, int | str]:
    if not isinstance(payload, dict):
        return {}

    for key in ("next_cursor", "nextCursor", "cursor"):
        next_cursor = payload.get(key)
        if next_cursor:
            return {"cursor": str(next_cursor)}

    pagination = payload.get("pagination")
    if isinstance(pagination, dict):
        next_cursor = pagination.get("next_cursor") or pagination.get("nextCursor")
        if next_cursor:
            return {"cursor": str(next_cursor)}

        next_offset = pagination.get("next_offset") or pagination.get("nextOffset")
        if isinstance(next_offset, int):
            return {"offset": next_offset}

        next_page = pagination.get("next_page") or pagination.get("nextPage")
        if isinstance(next_page, int):
            return {"page": next_page}

    has_more = payload.get("has_more") or payload.get("hasMore")
    if has_more:
        offset = payload.get("offset")
        limit = payload.get("limit")
        if isinstance(offset, int) and isinstance(limit, int):
            return {"offset": offset + limit}

        page = payload.get("page")
        if isinstance(page, int):
            return {"page": page + 1}

    return {}


def _updated_after(market: Mapping[str, Any], since_updated_at: datetime) -> bool:
    updated_at = _parse_optional_datetime(
        market.get("updatedAt") or market.get("updated_at") or market.get("updated_ts")
    )
    if updated_at is None:
        return True
    return updated_at >= _as_utc_datetime(since_updated_at)


def _as_utc_iso(value: datetime) -> str:
    return _as_utc_datetime(value).isoformat()


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc_datetime(raw)
    if isinstance(raw, int | float):
        return datetime.fromtimestamp(float(raw), tz=UTC)
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
