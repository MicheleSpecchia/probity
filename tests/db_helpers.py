from __future__ import annotations

import os
import subprocess
import sys
from urllib.parse import quote, unquote, urlsplit


def to_psycopg_dsn(database_url: str) -> str:
    raw = database_url.strip()
    if not raw:
        return raw

    parsed = urlsplit(raw)
    scheme = parsed.scheme.lower()
    if scheme in {"postgresql", "postgres"} and "+" not in scheme:
        if scheme == "postgres":
            return raw.replace("postgres://", "postgresql://", 1)
        return raw

    if scheme.startswith("postgresql+") or scheme.startswith("postgres+"):
        username = ""
        if parsed.username is not None:
            username = quote(unquote(parsed.username), safe="")
        password = ""
        if parsed.password is not None:
            password = quote(unquote(parsed.password), safe="")

        auth = ""
        if username:
            auth = username
            if password:
                auth = f"{auth}:{password}"
            auth = f"{auth}@"

        host = parsed.hostname or ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = f":{parsed.port}" if parsed.port is not None else ""
        query = f"?{parsed.query}" if parsed.query else ""
        return f"postgresql://{auth}{host}{port}{parsed.path}{query}"

    return raw


def alembic_upgrade_head(database_url: str) -> None:
    env = dict(os.environ)
    env["APP_DATABASE_URL"] = database_url
    env["DATABASE_URL"] = database_url

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        env=env,
    )
