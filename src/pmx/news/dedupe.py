from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Sequence

from pmx.news.normalize import sha256_hex

_SOFT_WINDOW = timedelta(hours=24)


@dataclass(frozen=True, slots=True)
class DedupeHashes:
    content_hash: str
    title_hash: str


@dataclass(frozen=True, slots=True)
class SoftDedupeCandidate:
    article_id: int
    source_domain: str
    published_at: datetime
    content_hash: str | None
    title_hash: str | None


def build_dedupe_hashes(
    *,
    title: str | None,
    body: str | None,
    summary: str | None = None,
) -> DedupeHashes:
    content_basis = body if body and body.strip() else " ".join(
        part for part in (title, summary) if part and part.strip()
    )
    content_hash = sha256_hex(content_basis)
    title_hash = sha256_hex(title)
    return DedupeHashes(content_hash=content_hash, title_hash=title_hash)


def select_soft_dedupe_candidate(
    candidates: Sequence[SoftDedupeCandidate],
    *,
    content_hash: str | None,
    title_hash: str | None,
    source_domain: str,
    published_at: datetime,
) -> SoftDedupeCandidate | None:
    published_utc = _as_utc_datetime(published_at)

    if content_hash:
        content_matches = [
            candidate for candidate in candidates if candidate.content_hash == content_hash
        ]
        if content_matches:
            return _choose_nearest_time(content_matches, published_utc)

    if title_hash:
        fallback_matches = [
            candidate
            for candidate in candidates
            if candidate.title_hash == title_hash
            and candidate.source_domain == source_domain
            and abs(_as_utc_datetime(candidate.published_at) - published_utc) <= _SOFT_WINDOW
        ]
        if fallback_matches:
            return _choose_nearest_time(fallback_matches, published_utc)

    return None


def _choose_nearest_time(
    candidates: Sequence[SoftDedupeCandidate],
    reference_ts: datetime,
) -> SoftDedupeCandidate:
    return sorted(
        candidates,
        key=lambda candidate: (
            abs(_as_utc_datetime(candidate.published_at) - reference_ts),
            candidate.article_id,
        ),
    )[0]


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
