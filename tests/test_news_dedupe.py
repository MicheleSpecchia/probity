from __future__ import annotations

from datetime import UTC, datetime

from pmx.news.dedupe import (
    SoftDedupeCandidate,
    build_dedupe_hashes,
    select_soft_dedupe_candidate,
    select_soft_dedupe_match,
)


def test_content_hash_is_deterministic() -> None:
    left = build_dedupe_hashes(
        title="Example title",
        body="Same body text",
        summary="Summary one",
    )
    right = build_dedupe_hashes(
        title="Example title",
        body="Same body text",
        summary="Summary one",
    )

    assert left.content_hash == right.content_hash
    assert left.title_hash == right.title_hash


def test_select_soft_dedupe_prefers_content_hash_match() -> None:
    published_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    candidates = [
        SoftDedupeCandidate(
            article_id=10,
            source_domain="reuters.com",
            published_at=datetime(2026, 1, 1, 11, 30, tzinfo=UTC),
            content_hash="content-a",
            title_hash="title-a",
        ),
        SoftDedupeCandidate(
            article_id=20,
            source_domain="reuters.com",
            published_at=datetime(2026, 1, 1, 11, 40, tzinfo=UTC),
            content_hash="content-b",
            title_hash="title-b",
        ),
    ]

    match = select_soft_dedupe_candidate(
        candidates,
        content_hash="content-b",
        title_hash="title-a",
        source_domain="reuters.com",
        published_at=published_at,
    )

    assert match is not None
    assert match.article_id == 20


def test_select_soft_dedupe_falls_back_to_title_hash_domain_and_window() -> None:
    published_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    candidates = [
        SoftDedupeCandidate(
            article_id=11,
            source_domain="reuters.com",
            published_at=datetime(2026, 1, 1, 11, 0, tzinfo=UTC),
            content_hash=None,
            title_hash="title-x",
        ),
        SoftDedupeCandidate(
            article_id=12,
            source_domain="apnews.com",
            published_at=datetime(2026, 1, 1, 11, 0, tzinfo=UTC),
            content_hash=None,
            title_hash="title-x",
        ),
    ]

    match = select_soft_dedupe_candidate(
        candidates,
        content_hash=None,
        title_hash="title-x",
        source_domain="reuters.com",
        published_at=published_at,
    )

    assert match is not None
    assert match.article_id == 11


def test_select_soft_dedupe_match_exposes_reason() -> None:
    published_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    candidates = [
        SoftDedupeCandidate(
            article_id=21,
            source_domain="reuters.com",
            published_at=datetime(2026, 1, 1, 11, 30, tzinfo=UTC),
            content_hash="content-a",
            title_hash="title-a",
        )
    ]

    match = select_soft_dedupe_match(
        candidates,
        content_hash="content-a",
        title_hash="title-a",
        source_domain="reuters.com",
        published_at=published_at,
    )

    assert match is not None
    assert match.candidate.article_id == 21
    assert match.reason == "content_hash"
