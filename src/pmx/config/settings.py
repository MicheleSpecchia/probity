from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


class SettingsError(ValueError):
    """Raised when environment configuration is invalid."""


_REQUIRED_ENV_VARS = (
    "APP_DATABASE_URL",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
)


@dataclass(frozen=True, slots=True)
class Settings:
    app_database_url: str
    postgres_user: str
    postgres_password: str
    postgres_db: str
    decision_cadence_hours: int
    screening_budget_per_cycle: int
    deep_dive_budget_per_cycle: int
    deep_dive_news_limit: int
    canonical_claim_limit: int
    primary_source_limit: int
    ingest_epsilon_seconds: int

    def as_audit_dict(self) -> dict[str, int | str]:
        return {
            "app_database_url": self.app_database_url,
            "postgres_user": self.postgres_user,
            "postgres_password": self.postgres_password,
            "postgres_db": self.postgres_db,
            "decision_cadence_hours": self.decision_cadence_hours,
            "screening_budget_per_cycle": self.screening_budget_per_cycle,
            "deep_dive_budget_per_cycle": self.deep_dive_budget_per_cycle,
            "deep_dive_news_limit": self.deep_dive_news_limit,
            "canonical_claim_limit": self.canonical_claim_limit,
            "primary_source_limit": self.primary_source_limit,
            "ingest_epsilon_seconds": self.ingest_epsilon_seconds,
        }


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    source: dict[str, str] = dict(os.environ) if environ is None else dict(environ)
    missing = [key for key in _REQUIRED_ENV_VARS if not source.get(key)]
    if missing:
        missing_csv = ", ".join(sorted(missing))
        raise SettingsError(f"Missing required environment variables: {missing_csv}")

    return Settings(
        app_database_url=source["APP_DATABASE_URL"],
        postgres_user=source["POSTGRES_USER"],
        postgres_password=source["POSTGRES_PASSWORD"],
        postgres_db=source["POSTGRES_DB"],
        decision_cadence_hours=_get_positive_int(source, "DECISION_CADENCE_HOURS", 4),
        screening_budget_per_cycle=_get_positive_int(source, "SCREENING_BUDGET_PER_CYCLE", 1500),
        deep_dive_budget_per_cycle=_get_positive_int(source, "DEEP_DIVE_BUDGET_PER_CYCLE", 200),
        deep_dive_news_limit=_get_positive_int(source, "DEEP_DIVE_NEWS_LIMIT", 80),
        canonical_claim_limit=_get_positive_int(source, "CANONICAL_CLAIM_LIMIT", 25),
        primary_source_limit=_get_positive_int(source, "PRIMARY_SOURCE_LIMIT", 10),
        ingest_epsilon_seconds=_get_positive_int(source, "INGEST_EPSILON_SECONDS", 300),
    )


def _get_positive_int(source: Mapping[str, str], key: str, default: int) -> int:
    raw = source.get(key)
    if raw is None or raw == "":
        return default

    try:
        parsed = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{key} must be an integer, got {raw!r}") from exc

    if parsed <= 0:
        raise SettingsError(f"{key} must be > 0, got {parsed}")

    return parsed
