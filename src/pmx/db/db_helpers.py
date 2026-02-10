from __future__ import annotations

import os


def get_database_url() -> str | None:
    """Resolve DB URL with explicit fallback order for local and CI tests."""
    return os.getenv("DATABASE_URL") or os.getenv("APP_DATABASE_URL")


def to_psycopg_dsn(database_url: str) -> str:
    """Convert SQLAlchemy-style DSNs into psycopg DSNs."""
    if database_url.startswith("postgresql+psycopg://"):
        return database_url.replace("postgresql+psycopg://", "postgresql://", 1)
    return database_url
