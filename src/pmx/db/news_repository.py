from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pmx.news.dedupe import SoftDedupeCandidate


@dataclass(frozen=True, slots=True)
class ArticleWritePayload:
    url: str
    canonical_url: str
    title: str
    body: str | None
    summary: str | None
    published_at: datetime
    ingested_at: datetime
    source_id: int
    lang: str | None
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ExistingArticleRef:
    article_id: int
    canonical_url: str | None


class NewsRepository:
    def __init__(self, connection: psycopg.Connection) -> None:
        self.connection = connection

    def insert_run(
        self,
        *,
        run_id: UUID,
        run_type: str,
        decision_ts: datetime,
        ingest_epsilon_seconds: int,
        code_version: str,
        config_hash: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO runs (
                    run_id,
                    run_type,
                    decision_ts,
                    ingest_epsilon_seconds,
                    code_version,
                    config_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    run_type,
                    _as_utc_datetime(decision_ts),
                    ingest_epsilon_seconds,
                    code_version,
                    config_hash,
                ),
            )

    def upsert_source(
        self,
        *,
        domain: str,
        name: str,
        is_primary: bool,
        trust_score: int,
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO sources (domain, name, is_primary, trust_score)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (domain) DO UPDATE
                SET name = EXCLUDED.name,
                    is_primary = EXCLUDED.is_primary,
                    trust_score = EXCLUDED.trust_score
                RETURNING source_id
                """,
                (domain, name, is_primary, trust_score),
            )
            row = cursor.fetchone()
        if row is None:
            raise RuntimeError(f"Unable to upsert source for domain={domain}")
        return int(row[0])

    def list_markets_for_linking(self) -> list[dict[str, str | None]]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT market_id, title, slug
                FROM markets
                ORDER BY market_id ASC
                """
            )
            rows = cursor.fetchall()
        return [
            {
                "market_id": str(row["market_id"]),
                "title": _as_text(row["title"]),
                "slug": _as_text(row["slug"]),
            }
            for row in rows
        ]

    def find_article_by_canonical_url(self, canonical_url: str) -> ExistingArticleRef | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT article_id, canonical_url
                FROM articles
                WHERE canonical_url = %s
                ORDER BY article_id ASC
                LIMIT 1
                """,
                (canonical_url,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return ExistingArticleRef(
            article_id=int(row[0]),
            canonical_url=_as_text(row[1]),
        )

    def find_soft_candidates_by_content_hash(
        self,
        *,
        content_hash: str,
    ) -> list[SoftDedupeCandidate]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    a.article_id,
                    s.domain AS source_domain,
                    a.published_at,
                    a.raw->'dedupe'->>'content_hash' AS content_hash,
                    a.raw->'dedupe'->>'title_hash' AS title_hash
                FROM articles AS a
                JOIN sources AS s
                  ON s.source_id = a.source_id
                WHERE a.raw->'dedupe'->>'content_hash' = %s
                ORDER BY a.article_id ASC
                """,
                (content_hash,),
            )
            rows = cursor.fetchall()
        return [_row_to_soft_candidate(row) for row in rows]

    def find_soft_candidates_by_title_hash(
        self,
        *,
        title_hash: str,
        source_domain: str,
        published_at: datetime,
    ) -> list[SoftDedupeCandidate]:
        anchor = _as_utc_datetime(published_at)
        lower_bound = anchor - timedelta(hours=24)
        upper_bound = anchor + timedelta(hours=24)

        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    a.article_id,
                    s.domain AS source_domain,
                    a.published_at,
                    a.raw->'dedupe'->>'content_hash' AS content_hash,
                    a.raw->'dedupe'->>'title_hash' AS title_hash
                FROM articles AS a
                JOIN sources AS s
                  ON s.source_id = a.source_id
                WHERE a.raw->'dedupe'->>'title_hash' = %s
                  AND s.domain = %s
                  AND a.published_at BETWEEN %s AND %s
                ORDER BY a.article_id ASC
                """,
                (title_hash, source_domain, lower_bound, upper_bound),
            )
            rows = cursor.fetchall()
        return [_row_to_soft_candidate(row) for row in rows]

    def insert_article(self, payload: ArticleWritePayload) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO articles (
                    url,
                    canonical_url,
                    title,
                    body,
                    summary,
                    published_at,
                    ingested_at,
                    source_id,
                    lang,
                    raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (url) DO UPDATE
                SET canonical_url = EXCLUDED.canonical_url,
                    title = EXCLUDED.title,
                    body = EXCLUDED.body,
                    summary = EXCLUDED.summary,
                    published_at = EXCLUDED.published_at,
                    ingested_at = EXCLUDED.ingested_at,
                    source_id = EXCLUDED.source_id,
                    lang = EXCLUDED.lang,
                    raw = EXCLUDED.raw
                RETURNING article_id
                """,
                (
                    payload.url,
                    payload.canonical_url,
                    payload.title,
                    payload.body,
                    payload.summary,
                    _as_utc_datetime(payload.published_at),
                    _as_utc_datetime(payload.ingested_at),
                    payload.source_id,
                    payload.lang,
                    Jsonb(payload.raw),
                ),
            )
            row = cursor.fetchone()
        if row is None:
            raise RuntimeError("Unable to insert article")
        return int(row[0])

    def update_article(self, article_id: int, payload: ArticleWritePayload) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE articles
                SET canonical_url = %s,
                    title = %s,
                    body = %s,
                    summary = %s,
                    published_at = %s,
                    ingested_at = %s,
                    source_id = %s,
                    lang = %s,
                    raw = %s::jsonb
                WHERE article_id = %s
                """,
                (
                    payload.canonical_url,
                    payload.title,
                    payload.body,
                    payload.summary,
                    _as_utc_datetime(payload.published_at),
                    _as_utc_datetime(payload.ingested_at),
                    payload.source_id,
                    payload.lang,
                    Jsonb(payload.raw),
                    article_id,
                ),
            )

    def replace_article_markets(self, article_id: int, market_ids: list[str]) -> int:
        sorted_market_ids = sorted({market_id for market_id in market_ids if market_id})
        with self.connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM article_markets WHERE article_id = %s",
                (article_id,),
            )
            if not sorted_market_ids:
                return 0
            for market_id in sorted_market_ids:
                cursor.execute(
                    """
                    INSERT INTO article_markets (article_id, market_id)
                    VALUES (%s, %s)
                    ON CONFLICT (article_id, market_id) DO NOTHING
                    """,
                    (article_id, market_id),
                )
        return len(sorted_market_ids)


def _row_to_soft_candidate(row: dict[str, Any]) -> SoftDedupeCandidate:
    return SoftDedupeCandidate(
        article_id=int(row["article_id"]),
        source_domain=str(row["source_domain"]),
        published_at=_as_utc_datetime(row["published_at"]),
        content_hash=_as_text(row["content_hash"]),
        title_hash=_as_text(row["title_hash"]),
    )


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
