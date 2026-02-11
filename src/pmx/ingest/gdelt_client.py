from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests

from pmx.news.normalize import canonicalize_json, extract_domain

DEFAULT_GDELT_BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_GDELT_TIMEOUT_SECONDS = 20
DEFAULT_GDELT_MAX_RECORDS = 250
DEFAULT_GDELT_USER_AGENT = "pmx-gdelt-client/0.1"


class GdeltClientError(RuntimeError):
    """Raised when GDELT API requests fail after retry attempts."""


@dataclass(frozen=True, slots=True)
class GdeltClientConfig:
    base_url: str = DEFAULT_GDELT_BASE_URL
    timeout_seconds: int = DEFAULT_GDELT_TIMEOUT_SECONDS
    max_retries: int = 4
    backoff_seconds: float = 0.5
    max_records: int = DEFAULT_GDELT_MAX_RECORDS


@dataclass(frozen=True, slots=True)
class GdeltArticle:
    url: str
    title: str
    published_at: datetime | None
    domain: str
    lang: str | None
    summary: str | None
    raw: dict[str, Any]


class GdeltClient:
    def __init__(
        self,
        config: GdeltClientConfig,
        *,
        session: requests.Session | None = None,
        sleep_fn: Any = time.sleep,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.sleep_fn = sleep_fn
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": DEFAULT_GDELT_USER_AGENT,
            }
        )

    def fetch_articles(
        self,
        *,
        since_published: datetime | None,
        max_articles: int | None,
    ) -> list[GdeltArticle]:
        params = _build_gdelt_params(
            since_published=since_published,
            max_records=max_articles or self.config.max_records,
        )
        payload = self._request_json(params)
        parsed = parse_gdelt_articles(payload)
        parsed.sort(
            key=lambda article: (
                article.published_at or datetime.min.replace(tzinfo=UTC),
                article.url,
            )
        )
        if max_articles is not None:
            return parsed[:max_articles]
        return parsed

    def _request_json(self, params: Mapping[str, str]) -> Any:
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self.session.get(
                    self.config.base_url,
                    params=dict(params),
                    timeout=self.config.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.config.max_retries:
                    raise GdeltClientError("GDELT request failed") from exc
                self.sleep_fn(_retry_delay_seconds(attempt, None, self.config.backoff_seconds))
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt >= self.config.max_retries:
                    raise GdeltClientError(
                        f"GDELT request failed with status={response.status_code}"
                    )
                retry_after = response.headers.get("Retry-After")
                self.sleep_fn(
                    _retry_delay_seconds(attempt, retry_after, self.config.backoff_seconds)
                )
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                raise GdeltClientError(
                    f"GDELT request failed with status={response.status_code}"
                ) from exc

            try:
                return response.json()
            except ValueError as exc:
                raise GdeltClientError("GDELT response is not valid JSON") from exc

        raise GdeltClientError("GDELT request exhausted retries")


def parse_gdelt_articles(payload: Any) -> list[GdeltArticle]:
    rows = _extract_rows(payload)
    output: list[GdeltArticle] = []
    for row in rows:
        url = _as_text(
            row.get("url") or row.get("sourceurl") or row.get("sourceUrl") or row.get("link")
        )
        if url is None:
            continue

        domain = _as_text(row.get("domain")) or extract_domain(url)
        if domain is None:
            continue

        title = _as_text(row.get("title") or row.get("seendate")) or url
        summary = _as_text(row.get("summary") or row.get("snippet") or row.get("socialimage"))
        lang = _as_text(row.get("language") or row.get("lang"))
        published_at = _parse_optional_datetime(
            row.get("published_at")
            or row.get("publishedAt")
            or row.get("seendate")
            or row.get("date")
            or row.get("datetime")
        )

        output.append(
            GdeltArticle(
                url=url,
                title=title,
                published_at=published_at,
                domain=domain,
                lang=lang,
                summary=summary,
                raw=canonicalize_json(dict(row)),
            )
        )
    return output


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("articles", "data", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _build_gdelt_params(
    *,
    since_published: datetime | None,
    max_records: int,
) -> dict[str, str]:
    params: dict[str, str] = {
        "query": "*",
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records),
        "sort": "DateDesc",
    }
    if since_published is not None:
        params["startdatetime"] = _as_utc_datetime(since_published).strftime("%Y%m%d%H%M%S")
    return params


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc_datetime(raw)
    if isinstance(raw, int | float):
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
            return _as_utc_datetime(datetime.fromisoformat(normalized))
        except ValueError:
            pass

        if len(text) == 14 and text.isdigit():
            try:
                return datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
            except ValueError:
                return None

        if (
            len(text) == 16
            and text[8:9] == "T"
            and text.endswith("Z")
            and text[0:8].isdigit()
            and text[9:15].isdigit()
        ):
            try:
                return datetime.strptime(text, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
            except ValueError:
                return None

    return None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _retry_delay_seconds(
    attempt: int,
    retry_after_header: str | None,
    base_backoff_seconds: float,
) -> float:
    if retry_after_header:
        try:
            parsed = float(retry_after_header)
            if parsed >= 0:
                return parsed
        except ValueError:
            pass
    exponential = base_backoff_seconds * (2**attempt)
    deterministic_jitter = min(0.05 * (attempt + 1), 0.25)
    return exponential + deterministic_jitter
