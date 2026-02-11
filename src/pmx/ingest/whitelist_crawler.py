from __future__ import annotations

import html
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

import requests

from pmx.news.normalize import canonicalize_json

_META_TAG_RE = re.compile(
    r"<meta[^>]+(?:property|name)\s*=\s*['\"](?P<key>[^'\"]+)['\"][^>]*"
    r"content\s*=\s*['\"](?P<value>[^'\"]*)['\"][^>]*>",
    flags=re.IGNORECASE,
)
_TITLE_TAG_RE = re.compile(r"<title[^>]*>(?P<value>.*?)</title>", flags=re.IGNORECASE | re.DOTALL)
_H1_TAG_RE = re.compile(r"<h1[^>]*>(?P<value>.*?)</h1>", flags=re.IGNORECASE | re.DOTALL)
_TIME_TAG_RE = re.compile(
    r"<time[^>]+datetime\s*=\s*['\"](?P<value>[^'\"]+)['\"][^>]*>",
    flags=re.IGNORECASE,
)
_ARTICLE_TAG_RE = re.compile(
    r"<article[^>]*>(?P<value>.*?)</article>",
    flags=re.IGNORECASE | re.DOTALL,
)
_PARAGRAPH_TAG_RE = re.compile(
    r"<p[^>]*>(?P<value>.*?)</p>",
    flags=re.IGNORECASE | re.DOTALL,
)
_JSON_LD_RE = re.compile(
    r"<script[^>]+type\s*=\s*['\"]application/ld\+json['\"][^>]*>(?P<value>.*?)</script>",
    flags=re.IGNORECASE | re.DOTALL,
)
_STRIP_TAGS_RE = re.compile(r"<[^>]+>")

_HEADER_KEYS = ("content-type", "date", "etag", "last-modified")


class WhitelistCrawlerError(RuntimeError):
    """Raised when crawler requests fail after retries."""


@dataclass(frozen=True, slots=True)
class WhitelistCrawlerConfig:
    connect_timeout_seconds: int = 5
    read_timeout_seconds: int = 15
    max_retries: int = 3
    backoff_seconds: float = 0.5
    default_rps: float = 0.5


@dataclass(frozen=True, slots=True)
class CrawlResult:
    url: str
    status_code: int
    title: str | None
    published_at: datetime | None
    body: str | None
    raw: dict[str, Any]


class _TokenBucketLimiter:
    def __init__(
        self,
        *,
        sleep_fn: Any,
        clock_fn: Any,
    ) -> None:
        self.sleep_fn = sleep_fn
        self.clock_fn = clock_fn
        self._states: dict[str, tuple[float, float]] = {}

    def wait(self, *, domain: str, rps: float) -> None:
        if rps <= 0:
            return

        now = float(self.clock_fn())
        tokens, last_ts = self._states.get(domain, (1.0, now))
        elapsed = max(now - last_ts, 0.0)
        tokens = min(1.0, tokens + (elapsed * rps))

        if tokens < 1.0:
            wait_seconds = (1.0 - tokens) / rps
            self.sleep_fn(wait_seconds)
            now = float(self.clock_fn())
            tokens = 1.0

        tokens = max(tokens - 1.0, 0.0)
        self._states[domain] = (tokens, now)


class WhitelistCrawler:
    def __init__(
        self,
        config: WhitelistCrawlerConfig,
        *,
        session: requests.Session | None = None,
        sleep_fn: Any = time.sleep,
        clock_fn: Any = time.monotonic,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.sleep_fn = sleep_fn
        self.clock_fn = clock_fn
        self._rate_limiter = _TokenBucketLimiter(sleep_fn=sleep_fn, clock_fn=clock_fn)

    def crawl_article(
        self,
        *,
        url: str,
        domain: str,
        rps: float | None = None,
    ) -> CrawlResult:
        request_rps = rps if rps is not None else self.config.default_rps
        response = self._request_with_retry(url=url, domain=domain, rps=request_rps)

        html_text = response.text if response.status_code < 400 else ""
        extracted = extract_article_fields(html_text)
        raw = {
            "status_code": response.status_code,
            "headers": _select_headers(response.headers),
            "final_url": response.url if response.url else url,
            "extracted": {
                "title": extracted["title"],
                "published_at": (
                    extracted["published_at"].isoformat() if extracted["published_at"] else None
                ),
                "body": extracted["body"],
            },
        }

        return CrawlResult(
            url=url,
            status_code=response.status_code,
            title=extracted["title"],
            published_at=extracted["published_at"],
            body=extracted["body"],
            raw=canonicalize_json(raw),
        )

    def _request_with_retry(
        self,
        *,
        url: str,
        domain: str,
        rps: float,
    ) -> requests.Response:
        for attempt in range(self.config.max_retries + 1):
            self._rate_limiter.wait(domain=domain, rps=rps)
            try:
                response = self.session.get(
                    url,
                    timeout=(
                        self.config.connect_timeout_seconds,
                        self.config.read_timeout_seconds,
                    ),
                )
            except requests.RequestException as exc:
                if attempt >= self.config.max_retries:
                    raise WhitelistCrawlerError(f"Crawler request failed for {url}") from exc
                self.sleep_fn(_retry_delay_seconds(attempt, None, self.config.backoff_seconds))
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt >= self.config.max_retries:
                    raise WhitelistCrawlerError(
                        f"Crawler request failed with status={response.status_code} for {url}"
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
            return response

        raise WhitelistCrawlerError(f"Crawler request exhausted retries for {url}")


def extract_article_fields(html_text: str) -> dict[str, Any]:
    title = _extract_title(html_text)
    published_at = _extract_published_at(html_text)
    body = _extract_body(html_text)
    return {
        "title": title,
        "published_at": published_at,
        "body": body,
    }


def _extract_title(html_text: str) -> str | None:
    meta_value = _find_meta_content(html_text, "og:title")
    if meta_value:
        return meta_value

    title_match = _TITLE_TAG_RE.search(html_text)
    if title_match:
        cleaned = _clean_html_fragment(title_match.group("value"))
        if cleaned:
            return cleaned

    h1_match = _H1_TAG_RE.search(html_text)
    if h1_match:
        cleaned = _clean_html_fragment(h1_match.group("value"))
        if cleaned:
            return cleaned
    return None


def _extract_published_at(html_text: str) -> datetime | None:
    for payload in _extract_json_ld_payloads(html_text):
        raw = _find_first_key(payload, "datePublished")
        parsed = _parse_optional_datetime(raw)
        if parsed is not None:
            return parsed

    meta_value = _find_meta_content(html_text, "article:published_time")
    parsed_meta = _parse_optional_datetime(meta_value)
    if parsed_meta is not None:
        return parsed_meta

    time_match = _TIME_TAG_RE.search(html_text)
    if time_match:
        parsed_time = _parse_optional_datetime(time_match.group("value"))
        if parsed_time is not None:
            return parsed_time

    return None


def _extract_body(html_text: str) -> str | None:
    for payload in _extract_json_ld_payloads(html_text):
        raw = _find_first_key(payload, "articleBody")
        body_text = _as_text(raw)
        if body_text:
            return body_text

    article_match = _ARTICLE_TAG_RE.search(html_text)
    if article_match:
        cleaned = _clean_html_fragment(article_match.group("value"))
        if cleaned:
            return cleaned

    paragraphs = [
        _clean_html_fragment(match.group("value")) for match in _PARAGRAPH_TAG_RE.finditer(html_text)
    ]
    paragraph_text = " ".join(part for part in paragraphs if part)
    if paragraph_text:
        return paragraph_text

    return None


def _extract_json_ld_payloads(html_text: str) -> list[Any]:
    output: list[Any] = []
    for match in _JSON_LD_RE.finditer(html_text):
        raw_payload = html.unescape(match.group("value").strip())
        if not raw_payload:
            continue
        try:
            parsed = json.loads(raw_payload)
        except ValueError:
            continue
        output.append(parsed)
    return output


def _find_first_key(payload: Any, target_key: str) -> Any:
    if isinstance(payload, Mapping):
        for key in sorted(payload.keys(), key=str):
            value = payload[key]
            if str(key) == target_key:
                return value
            nested = _find_first_key(value, target_key)
            if nested is not None:
                return nested
        return None

    if isinstance(payload, list):
        for item in payload:
            nested = _find_first_key(item, target_key)
            if nested is not None:
                return nested
    return None


def _find_meta_content(html_text: str, meta_key: str) -> str | None:
    lowered_target = meta_key.lower()
    for match in _META_TAG_RE.finditer(html_text):
        key = match.group("key").strip().lower()
        if key == lowered_target:
            value = _as_text(match.group("value"))
            if value:
                return value
    return None


def _clean_html_fragment(value: str) -> str | None:
    stripped = _STRIP_TAGS_RE.sub(" ", value)
    unescaped = html.unescape(stripped)
    normalized = re.sub(r"\s+", " ", unescaped).strip()
    return normalized if normalized else None


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
            return None
    return None


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _select_headers(headers: Mapping[str, str]) -> dict[str, str]:
    normalized_headers: dict[str, str] = {}
    for key in _HEADER_KEYS:
        if key in headers:
            normalized_headers[key] = str(headers[key])
    return normalized_headers


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
