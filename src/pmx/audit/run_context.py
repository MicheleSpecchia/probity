from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping
from uuid import NAMESPACE_URL, uuid4, uuid5


@dataclass(frozen=True, slots=True)
class RunContext:
    run_id: str
    job_name: str
    code_version: str
    config_hash: str
    started_at: str

    def as_log_context(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "job_name": self.job_name,
            "code_version": self.code_version,
            "config_hash": self.config_hash,
        }


def build_run_context(
    job_name: str,
    config: Mapping[str, Any],
    *,
    started_at: datetime | None = None,
    nonce: str | None = None,
    code_version: str | None = None,
) -> RunContext:
    if not job_name:
        raise ValueError("job_name must be non-empty")

    timestamp = started_at if started_at is not None else datetime.now(UTC)
    if timestamp.tzinfo is None:
        raise ValueError("started_at must be timezone-aware")
    timestamp = timestamp.astimezone(UTC)

    computed_hash = compute_config_hash(config)
    entropy = nonce if nonce is not None else uuid4().hex
    seed = f"{job_name}|{computed_hash}|{timestamp.isoformat()}|{entropy}"
    run_id = uuid5(NAMESPACE_URL, seed).hex

    return RunContext(
        run_id=run_id,
        job_name=job_name,
        code_version=code_version or resolve_code_version(),
        config_hash=computed_hash,
        started_at=timestamp.isoformat(),
    )


def compute_config_hash(config: Mapping[str, Any]) -> str:
    canonical = _normalize(config)
    payload = json.dumps(canonical, separators=(",", ":"), sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def resolve_code_version() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            capture_output=True,
            check=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"

    code_version = result.stdout.strip()
    return code_version if code_version else "unknown"


def _normalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda item: str(item[0]))
        return {str(key): _normalize(item_value) for key, item_value in items}

    if isinstance(value, (tuple, list)):
        return [_normalize(item_value) for item_value in value]

    if isinstance(value, set):
        return [_normalize(item_value) for item_value in sorted(value, key=str)]

    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("Datetime values in config must be timezone-aware")
        return value.astimezone(UTC).isoformat()

    return value
