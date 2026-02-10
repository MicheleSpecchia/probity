from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest

from pmx.db.db_helpers import get_database_url
from tests.db_helpers import alembic_upgrade_head, to_psycopg_dsn


def test_articles_asof_filters_records_after_ingest_epsilon() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    run_id = uuid4()
    source_slug = run_id.hex[:12]
    decision_ts = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
    epsilon_seconds = 300
    published_ts = decision_ts - timedelta(minutes=30)

    accepted_ingested_ts = decision_ts + timedelta(seconds=epsilon_seconds)
    rejected_ingested_ts = decision_ts + timedelta(seconds=epsilon_seconds + 1)

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as conn:
        with conn.cursor() as cursor:
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
                (run_id, "asof_contract_test", decision_ts, epsilon_seconds, "test", "test-config"),
            )

            cursor.execute(
                """
                INSERT INTO sources (domain, name, is_primary, trust_score)
                VALUES (%s, %s, %s, %s)
                RETURNING source_id
                """,
                (f"{source_slug}.example", "As-Of Contract Source", True, 80),
            )
            source_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO articles (
                    url,
                    canonical_url,
                    title,
                    published_at,
                    ingested_at,
                    source_id,
                    lang,
                    raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
                RETURNING article_id
                """,
                (
                    f"https://{source_slug}.example/accepted",
                    f"https://{source_slug}.example/accepted",
                    "Accepted article",
                    published_ts,
                    accepted_ingested_ts,
                    source_id,
                    "en",
                ),
            )
            accepted_article_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO articles (
                    url,
                    canonical_url,
                    title,
                    published_at,
                    ingested_at,
                    source_id,
                    lang,
                    raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
                RETURNING article_id
                """,
                (
                    f"https://{source_slug}.example/rejected",
                    f"https://{source_slug}.example/rejected",
                    "Rejected article",
                    published_ts,
                    rejected_ingested_ts,
                    source_id,
                    "en",
                ),
            )
            rejected_article_id = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT a.article_id
                FROM articles AS a
                JOIN runs AS r ON r.run_id = %s
                WHERE a.article_id IN (%s, %s)
                  AND a.published_at <= r.decision_ts
                  AND a.ingested_at <= (
                        r.decision_ts
                        + make_interval(secs => r.ingest_epsilon_seconds)
                  )
                ORDER BY a.article_id
                """,
                (run_id, accepted_article_id, rejected_article_id),
            )
            selected_article_ids = [row[0] for row in cursor.fetchall()]

    assert selected_article_ids == [accepted_article_id]


def test_asof_filter_rejects_published_after_decision() -> None:
    database_url = get_database_url()
    if not database_url:
        pytest.skip("Skipping DB tests: set DATABASE_URL or APP_DATABASE_URL")

    alembic_upgrade_head(database_url)

    run_id = uuid4()
    source_slug = run_id.hex[:12]
    decision_ts = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
    epsilon_seconds = 300
    published_after_decision = decision_ts + timedelta(seconds=1)
    ingested_on_time = decision_ts + timedelta(seconds=epsilon_seconds)

    with psycopg.connect(to_psycopg_dsn(database_url), autocommit=True) as conn:
        with conn.cursor() as cursor:
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
                (run_id, "asof_contract_test", decision_ts, epsilon_seconds, "test", "test-config"),
            )

            cursor.execute(
                """
                INSERT INTO sources (domain, name, is_primary, trust_score)
                VALUES (%s, %s, %s, %s)
                RETURNING source_id
                """,
                (f"{source_slug}.example", "As-Of Contract Source", True, 80),
            )
            source_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO articles (
                    url,
                    canonical_url,
                    title,
                    published_at,
                    ingested_at,
                    source_id,
                    lang,
                    raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
                RETURNING article_id
                """,
                (
                    f"https://{source_slug}.example/late-published",
                    f"https://{source_slug}.example/late-published",
                    "Published too late",
                    published_after_decision,
                    ingested_on_time,
                    source_id,
                    "en",
                ),
            )
            late_article_id = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM articles AS a
                JOIN runs AS r ON r.run_id = %s
                WHERE a.article_id = %s
                  AND a.published_at <= r.decision_ts
                  AND a.ingested_at <= (
                        r.decision_ts
                        + make_interval(secs => r.ingest_epsilon_seconds)
                  )
                """,
                (run_id, late_article_id),
            )
            selected_count = cursor.fetchone()[0]

    assert selected_count == 0
