from __future__ import annotations

import os
import subprocess
import sys


def alembic_upgrade_head(database_url: str) -> None:
    env = dict(os.environ)
    env["APP_DATABASE_URL"] = database_url
    env["DATABASE_URL"] = database_url

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        env=env,
    )
