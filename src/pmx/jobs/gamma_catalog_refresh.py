from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import psycopg

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.db.db_helpers import get_database_url, to_psycopg_dsn
from pmx.db.gamma_catalog_repository import GammaCatalogRepository
from pmx.ingest.gamma_catalog import market_sort_key, normalize_market_payload
from pmx.ingest.gamma_client import (
    DEFAULT_GAMMA_BASE_URL,
    DEFAULT_GAMMA_PAGE_SIZE,
    DEFAULT_GAMMA_TIMEOUT_SECONDS,
    GammaClient,
    GammaClientConfig,
)

JOB_NAME = "gamma_catalog_refresh"


@dataclass(frozen=True, slots=True)
class GammaCatalogRefreshConfig:
    gamma_base_url: str
    gamma_timeout_seconds: int
    gamma_page_size: int
    ingest_epsilon_seconds: int

    def as_hash_dict(self) -> dict[str, Any]:
        return {
            "gamma_base_url": self.gamma_base_url,
            "gamma_timeout_seconds": self.gamma_timeout_seconds,
            "gamma_page_size": self.gamma_page_size,
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
        }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    since_updated_at = _parse_optional_datetime_arg(args.since_updated_at)
    if args.max_markets is not None and args.max_markets <= 0:
        raise ValueError("--max-markets must be > 0")

    config = load_gamma_catalog_refresh_config()
    stats = run_gamma_catalog_refresh(
        config=config,
        since_updated_at=since_updated_at,
        max_markets=args.max_markets,
    )
    return 0 if stats["errors"] == 0 else 1


def run_gamma_catalog_refresh(
    *,
    config: GammaCatalogRefreshConfig,
    since_updated_at: datetime | None,
    max_markets: int | None,
) -> dict[str, int]:
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("Missing DB URL: set DATABASE_URL or APP_DATABASE_URL")

    started_at = datetime.now(tz=UTC)
    run_context = build_run_context(
        JOB_NAME,
        {
            **config.as_hash_dict(),
            "since_updated_at": since_updated_at.isoformat() if since_updated_at else None,
            "max_markets": max_markets,
        },
        started_at=started_at,
    )
    run_uuid = UUID(run_context.run_id)

    logger = get_logger(f"pmx.jobs.{JOB_NAME}")
    _log(
        logger,
        logging.INFO,
        "gamma_catalog_refresh_started",
        run_context,
        since_updated_at=since_updated_at.isoformat() if since_updated_at else None,
        max_markets=max_markets,
    )

    client = GammaClient(
        GammaClientConfig(
            base_url=config.gamma_base_url,
            timeout_seconds=config.gamma_timeout_seconds,
            page_size=config.gamma_page_size,
        )
    )
    fetched_markets = client.iter_markets(
        since_updated_at=since_updated_at,
        max_markets=max_markets,
    )
    sorted_markets = sorted(fetched_markets, key=market_sort_key)

    stats = {
        "errors": 0,
        "markets_upserted": 0,
        "markets_seen": len(sorted_markets),
        "markets_skipped": 0,
        "token_conflicts": 0,
        "tokens_upserted": 0,
    }

    with psycopg.connect(to_psycopg_dsn(database_url)) as connection:
        repository = GammaCatalogRepository(connection)
        repository.insert_run(
            run_id=run_uuid,
            run_type="catalog_refresh",
            decision_ts=started_at,
            ingest_epsilon_seconds=config.ingest_epsilon_seconds,
            code_version=run_context.code_version,
            config_hash=run_context.config_hash,
        )

        for payload in sorted_markets:
            market_record, token_records = normalize_market_payload(
                payload,
                ingested_at=started_at,
            )
            if market_record is None:
                stats["markets_skipped"] += 1
                _log(
                    logger,
                    logging.WARNING,
                    "gamma_market_skipped_missing_id",
                    run_context,
                    market_payload=payload,
                )
                continue

            repository.upsert_market(market_record)
            stats["markets_upserted"] += 1

            for token_record in token_records:
                conflict = repository.upsert_market_token(token_record)
                if conflict is None:
                    stats["tokens_upserted"] += 1
                    continue

                stats["token_conflicts"] += 1
                stats["errors"] += 1
                _log(
                    logger,
                    logging.ERROR,
                    "token_conflict",
                    run_context,
                    event="token_conflict",
                    data_quality_issue=True,
                    token_id=conflict.token_id,
                    new_market_id=token_record.market_id,
                    new_outcome=token_record.outcome,
                    existing_market_id=conflict.existing_market_id,
                    existing_outcome=conflict.existing_outcome,
                    action=conflict.action,
                )

    elapsed_seconds = int((datetime.now(tz=UTC) - started_at).total_seconds())
    _log(
        logger,
        logging.INFO,
        "gamma_catalog_refresh_completed",
        run_context,
        elapsed_seconds=elapsed_seconds,
        **stats,
    )
    return stats


def load_gamma_catalog_refresh_config() -> GammaCatalogRefreshConfig:
    gamma_base_url = os.getenv("GAMMA_BASE_URL", DEFAULT_GAMMA_BASE_URL)
    gamma_timeout_seconds = _load_positive_int(
        "GAMMA_TIMEOUT_SECONDS",
        DEFAULT_GAMMA_TIMEOUT_SECONDS,
    )
    gamma_page_size = _load_positive_int("GAMMA_PAGE_SIZE", DEFAULT_GAMMA_PAGE_SIZE)
    ingest_epsilon_seconds = _load_positive_int("INGEST_EPSILON_SECONDS", 300)

    return GammaCatalogRefreshConfig(
        gamma_base_url=gamma_base_url,
        gamma_timeout_seconds=gamma_timeout_seconds,
        gamma_page_size=gamma_page_size,
        ingest_epsilon_seconds=ingest_epsilon_seconds,
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh Polymarket Gamma market catalog.")
    parser.add_argument(
        "--since-updated-at",
        dest="since_updated_at",
        help="Optional ISO datetime for incremental updates.",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Optional max number of markets to process.",
    )
    return parser.parse_args(argv)


def _parse_optional_datetime_arg(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid --since-updated-at value: {raw!r}") from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _load_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default

    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc

    if parsed <= 0:
        raise ValueError(f"{name} must be > 0, got {parsed}")
    return parsed


def _log(
    logger: logging.Logger,
    level: int,
    message: str,
    run_context: RunContext,
    **extra_fields: Any,
) -> None:
    payload = run_context.as_log_context()
    payload["extra_fields"] = extra_fields
    logger.log(level, message, extra=payload)


if __name__ == "__main__":
    raise SystemExit(main())
