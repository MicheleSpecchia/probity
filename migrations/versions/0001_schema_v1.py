"""schema_v1

Revision ID: 0001_schema_v1
Revises:
Create Date: 2026-02-10 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_schema_v1"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "runs",
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_type", sa.Text(), nullable=False),
        sa.Column("decision_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("ingest_epsilon_seconds", sa.Integer(), nullable=False),
        sa.Column("code_version", sa.Text(), nullable=False),
        sa.Column("config_hash", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("run_id", name="runs_pkey"),
    )
    op.create_index("runs_decision_ts_idx", "runs", ["decision_ts"], unique=False)

    op.create_table(
        "markets",
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_ts", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("updated_ts", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("resolution_ts", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("rule_text", sa.Text(), nullable=True),
        sa.Column("rule_parse_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "rule_parse_ok",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.PrimaryKeyConstraint("market_id", name="markets_pkey"),
    )
    op.create_index("markets_slug_idx", "markets", ["slug"], unique=False)
    op.create_index("markets_status_idx", "markets", ["status"], unique=False)

    op.create_table(
        "market_tokens",
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("token_id", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["markets.market_id"],
            name="market_tokens_market_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("market_id", "outcome", name="market_tokens_pkey"),
        sa.UniqueConstraint("token_id", name="market_tokens_token_id_uk"),
    )

    op.create_table(
        "market_outcomes",
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("resolved", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("outcome", sa.Text(), nullable=True),
        sa.Column("resolved_ts", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("resolver_source", sa.Text(), nullable=True),
        sa.Column("ingested_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["markets.market_id"],
            name="market_outcomes_market_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("market_id", name="market_outcomes_pkey"),
    )

    op.execute(
        """
        CREATE TABLE trades (
            trade_id BIGINT GENERATED ALWAYS AS IDENTITY,
            token_id TEXT NOT NULL REFERENCES market_tokens (token_id) ON DELETE CASCADE,
            event_ts TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            price NUMERIC(10, 8) NOT NULL,
            size NUMERIC(18, 8) NOT NULL,
            side TEXT NOT NULL,
            trade_hash TEXT NULL,
            seq BIGINT NULL,
            seq_norm BIGINT GENERATED ALWAYS AS (COALESCE(seq, 0)) STORED,
            trade_hash_norm TEXT GENERATED ALWAYS AS (COALESCE(trade_hash, ''::text)) STORED,
            CONSTRAINT trades_pkey PRIMARY KEY (trade_id, event_ts),
            CONSTRAINT trades_idempotency_uk UNIQUE (token_id, event_ts, seq_norm, trade_hash_norm),
            CONSTRAINT trades_side_ck CHECK (side IN ('buy', 'sell', 'unknown'))
        ) PARTITION BY RANGE (event_ts)
        """
    )
    op.execute(
        """
        CREATE TABLE trades_p_default
        PARTITION OF trades
        FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2030-01-01 00:00:00+00')
        """
    )
    op.execute("CREATE INDEX trades_token_event_idx ON trades (token_id, event_ts DESC)")
    op.execute("CREATE INDEX trades_token_ingested_idx ON trades (token_id, ingested_at DESC)")

    op.execute(
        """
        CREATE TABLE orderbook_snapshots (
            snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY,
            token_id TEXT NOT NULL REFERENCES market_tokens (token_id) ON DELETE CASCADE,
            event_ts TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            bids JSONB NOT NULL,
            asks JSONB NOT NULL,
            mid NUMERIC(10, 8) NULL,
            CONSTRAINT orderbook_snapshots_pkey PRIMARY KEY (snapshot_id, event_ts),
            CONSTRAINT orderbook_snapshots_token_event_uk UNIQUE (token_id, event_ts)
        ) PARTITION BY RANGE (event_ts)
        """
    )
    op.execute(
        """
        CREATE TABLE orderbook_snapshots_p_default
        PARTITION OF orderbook_snapshots
        FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2030-01-01 00:00:00+00')
        """
    )
    op.execute(
        "CREATE INDEX orderbook_snapshots_token_event_idx "
        "ON orderbook_snapshots (token_id, event_ts DESC)"
    )
    op.execute(
        "CREATE INDEX orderbook_snapshots_token_ingested_idx "
        "ON orderbook_snapshots (token_id, ingested_at DESC)"
    )

    op.execute(
        """
        CREATE TABLE candles (
            candle_id BIGINT GENERATED ALWAYS AS IDENTITY,
            token_id TEXT NOT NULL REFERENCES market_tokens (token_id) ON DELETE CASCADE,
            interval TEXT NOT NULL,
            start_ts TIMESTAMPTZ NOT NULL,
            end_ts TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            o NUMERIC(10, 8) NOT NULL,
            h NUMERIC(10, 8) NOT NULL,
            l NUMERIC(10, 8) NOT NULL,
            c NUMERIC(10, 8) NOT NULL,
            v NUMERIC(18, 8) NOT NULL,
            CONSTRAINT candles_pkey PRIMARY KEY (candle_id, start_ts),
            CONSTRAINT candles_token_interval_start_uk UNIQUE (token_id, interval, start_ts),
            CONSTRAINT candles_window_ck CHECK (end_ts > start_ts)
        ) PARTITION BY RANGE (start_ts)
        """
    )
    op.execute(
        """
        CREATE TABLE candles_p_default
        PARTITION OF candles
        FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2030-01-01 00:00:00+00')
        """
    )
    op.execute("CREATE INDEX candles_token_event_idx ON candles (token_id, start_ts DESC)")
    op.execute("CREATE INDEX candles_token_ingested_idx ON candles (token_id, ingested_at DESC)")

    op.create_table(
        "sources",
        sa.Column("source_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("domain", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("is_primary", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("trust_score", sa.SmallInteger(), nullable=False, server_default=sa.text("50")),
        sa.CheckConstraint(
            "trust_score >= 0 AND trust_score <= 100",
            name="sources_trust_score_ck",
        ),
        sa.PrimaryKeyConstraint("source_id", name="sources_pkey"),
        sa.UniqueConstraint("domain", name="sources_domain_uk"),
    )

    op.create_table(
        "articles",
        sa.Column("article_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("canonical_url", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("lang", sa.Text(), nullable=True),
        sa.Column(
            "raw",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.source_id"],
            name="articles_source_id_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("article_id", name="articles_pkey"),
        sa.UniqueConstraint("url", name="articles_url_uk"),
    )
    op.execute("CREATE INDEX articles_pub_idx ON articles (published_at DESC)")
    op.execute("CREATE INDEX articles_ingested_idx ON articles (ingested_at DESC)")
    op.execute("CREATE INDEX articles_source_pub_idx ON articles (source_id, published_at DESC)")
    op.create_index(
        "articles_source_external_uk",
        "articles",
        ["source_id", "external_id"],
        unique=True,
        postgresql_where=sa.text("external_id IS NOT NULL"),
    )

    op.create_table(
        "article_markets",
        sa.Column("article_id", sa.BigInteger(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["article_id"],
            ["articles.article_id"],
            name="article_markets_article_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["markets.market_id"],
            name="article_markets_market_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("article_id", "market_id", name="article_markets_pkey"),
    )
    op.create_index("article_markets_market_idx", "article_markets", ["market_id"], unique=False)

    op.create_table(
        "claims",
        sa.Column("claim_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("claim_canonical", sa.Text(), nullable=False),
        sa.Column("claim_hash", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["market_id"], ["markets.market_id"], name="claims_market_id_fkey", ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("claim_id", name="claims_pkey"),
        sa.UniqueConstraint("market_id", "claim_hash", name="claims_market_claim_hash_uk"),
    )
    op.create_index("claims_market_idx", "claims", ["market_id"], unique=False)

    op.create_table(
        "claim_evidence",
        sa.Column("evidence_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("claim_id", sa.BigInteger(), nullable=False),
        sa.Column("article_id", sa.BigInteger(), nullable=True),
        sa.Column("source_id", sa.BigInteger(), nullable=True),
        sa.Column("quote", sa.Text(), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("ingested_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("stance", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Numeric(precision=4, scale=3), nullable=True),
        sa.CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="claim_evidence_confidence_ck",
        ),
        sa.ForeignKeyConstraint(
            ["article_id"],
            ["articles.article_id"],
            name="claim_evidence_article_id_fkey",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["claim_id"],
            ["claims.claim_id"],
            name="claim_evidence_claim_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.source_id"],
            name="claim_evidence_source_id_fkey",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("evidence_id", name="claim_evidence_pkey"),
    )
    op.create_index("claim_evidence_claim_idx", "claim_evidence", ["claim_id"], unique=False)

    op.create_table(
        "claim_edges",
        sa.Column("edge_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("src_claim_id", sa.BigInteger(), nullable=False),
        sa.Column("dst_claim_id", sa.BigInteger(), nullable=False),
        sa.Column("relation", sa.Text(), nullable=False),
        sa.CheckConstraint("src_claim_id <> dst_claim_id", name="claim_edges_self_ref_ck"),
        sa.ForeignKeyConstraint(
            ["dst_claim_id"],
            ["claims.claim_id"],
            name="claim_edges_dst_claim_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["markets.market_id"],
            name="claim_edges_market_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["src_claim_id"],
            ["claims.claim_id"],
            name="claim_edges_src_claim_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("edge_id", name="claim_edges_pkey"),
        sa.UniqueConstraint(
            "market_id",
            "src_claim_id",
            "dst_claim_id",
            "relation",
            name="claim_edges_market_src_dst_rel_uk",
        ),
    )
    op.create_index("claim_edges_market_idx", "claim_edges", ["market_id"], unique=False)

    op.create_table(
        "feature_snapshots",
        sa.Column("feature_snapshot_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("asof_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("feature_set_version", sa.Text(), nullable=False),
        sa.Column("features", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["markets.market_id"],
            name="feature_snapshots_market_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["runs.run_id"],
            name="feature_snapshots_run_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("feature_snapshot_id", name="feature_snapshots_pkey"),
        sa.UniqueConstraint(
            "run_id",
            "market_id",
            "asof_ts",
            "feature_set_version",
            name="feature_snapshots_run_market_asof_version_uk",
        ),
    )
    op.execute(
        "CREATE INDEX feature_snapshots_market_asof_idx "
        "ON feature_snapshots (market_id, asof_ts DESC)"
    )

    op.create_table(
        "selection_runs",
        sa.Column("selection_run_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("universe_size", sa.Integer(), nullable=False),
        sa.Column("selected_size", sa.Integer(), nullable=False),
        sa.Column("selector_version", sa.Text(), nullable=False),
        sa.Column(
            "params",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["runs.run_id"],
            name="selection_runs_run_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("selection_run_id", name="selection_runs_pkey"),
    )

    op.create_table(
        "selection_items",
        sa.Column("selection_run_id", sa.BigInteger(), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("score", sa.Numeric(precision=12, scale=6), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column(
            "reason",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["markets.market_id"],
            name="selection_items_market_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["selection_run_id"],
            ["selection_runs.selection_run_id"],
            name="selection_items_selection_run_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("selection_run_id", "market_id", name="selection_items_pkey"),
    )
    op.create_index(
        "selection_items_run_rank_idx",
        "selection_items",
        ["selection_run_id", "rank"],
        unique=False,
    )

    op.create_table(
        "forecasts",
        sa.Column("forecast_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("market_id", sa.Text(), nullable=False),
        sa.Column("horizon_hours", sa.Integer(), nullable=False),
        sa.Column("p_raw", sa.Numeric(precision=6, scale=5), nullable=False),
        sa.Column("p_cal", sa.Numeric(precision=6, scale=5), nullable=False),
        sa.Column("model_version", sa.Text(), nullable=False),
        sa.Column("calibrator_version", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("p_raw >= 0 AND p_raw <= 1", name="forecasts_p_raw_ck"),
        sa.CheckConstraint("p_cal >= 0 AND p_cal <= 1", name="forecasts_p_cal_ck"),
        sa.ForeignKeyConstraint(
            ["market_id"],
            ["markets.market_id"],
            name="forecasts_market_id_fkey",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["runs.run_id"],
            name="forecasts_run_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("forecast_id", name="forecasts_pkey"),
        sa.UniqueConstraint(
            "run_id",
            "market_id",
            "horizon_hours",
            "model_version",
            name="forecasts_run_market_horizon_model_uk",
        ),
    )
    op.execute(
        "CREATE INDEX forecasts_market_created_idx ON forecasts (market_id, created_at DESC)"
    )

    op.create_table(
        "forecast_intervals",
        sa.Column("forecast_id", sa.BigInteger(), nullable=False),
        sa.Column("level", sa.Integer(), nullable=False),
        sa.Column("lo", sa.Numeric(precision=6, scale=5), nullable=False),
        sa.Column("hi", sa.Numeric(precision=6, scale=5), nullable=False),
        sa.CheckConstraint(
            "level > 0 AND level < 100",
            name="forecast_intervals_level_ck",
        ),
        sa.CheckConstraint("lo >= 0 AND lo <= 1", name="forecast_intervals_lo_ck"),
        sa.CheckConstraint("hi >= 0 AND hi <= 1", name="forecast_intervals_hi_ck"),
        sa.CheckConstraint("lo <= hi", name="forecast_intervals_order_ck"),
        sa.ForeignKeyConstraint(
            ["forecast_id"],
            ["forecasts.forecast_id"],
            name="forecast_intervals_forecast_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("forecast_id", "level", name="forecast_intervals_pkey"),
    )

    op.create_table(
        "forecast_drivers",
        sa.Column("forecast_id", sa.BigInteger(), nullable=False),
        sa.Column("driver_type", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["forecast_id"],
            ["forecasts.forecast_id"],
            name="forecast_drivers_forecast_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("forecast_id", "driver_type", name="forecast_drivers_pkey"),
    )

    op.create_table(
        "no_trade_flags",
        sa.Column("forecast_id", sa.BigInteger(), nullable=False),
        sa.Column("flag", sa.Text(), nullable=False),
        sa.Column("severity", sa.SmallInteger(), nullable=False),
        sa.Column(
            "detail",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint("severity >= 1 AND severity <= 5", name="no_trade_flags_severity_ck"),
        sa.ForeignKeyConstraint(
            ["forecast_id"],
            ["forecasts.forecast_id"],
            name="no_trade_flags_forecast_id_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("forecast_id", "flag", name="no_trade_flags_pkey"),
    )


def downgrade() -> None:
    op.drop_table("no_trade_flags")
    op.drop_table("forecast_drivers")
    op.drop_table("forecast_intervals")
    op.drop_table("forecasts")
    op.drop_table("selection_items")
    op.drop_table("selection_runs")
    op.drop_table("feature_snapshots")
    op.drop_table("claim_edges")
    op.drop_table("claim_evidence")
    op.drop_table("claims")
    op.drop_table("article_markets")
    op.drop_table("articles")
    op.drop_table("sources")
    op.execute("DROP TABLE IF EXISTS candles CASCADE")
    op.execute("DROP TABLE IF EXISTS orderbook_snapshots CASCADE")
    op.execute("DROP TABLE IF EXISTS trades CASCADE")
    op.drop_table("market_outcomes")
    op.drop_table("market_tokens")
    op.drop_table("markets")
    op.drop_table("runs")
