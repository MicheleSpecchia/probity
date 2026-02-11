from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any, Final

from pmx.news.normalize import canonicalize_json

CLAIM_EXTRACT_SCHEMA_VERSION: Final[str] = "claim_extract.v1"
EVIDENCE_CHECKLIST_SCHEMA_VERSION: Final[str] = "evidence_checklist.v1"
CLAIM_EXTRACT_SCHEMA_FILENAME: Final[str] = "claim_extract.v1.json"
EVIDENCE_CHECKLIST_SCHEMA_FILENAME: Final[str] = "evidence_checklist.v1.json"
SCHEMAS_DIR: Final[Path] = Path(__file__).resolve().parents[3] / "schemas"


def schema_path(filename: str) -> Path:
    return SCHEMAS_DIR / filename


@cache
def load_claim_extract_schema() -> dict[str, Any]:
    return _load_schema(CLAIM_EXTRACT_SCHEMA_FILENAME)


@cache
def load_evidence_checklist_schema() -> dict[str, Any]:
    return _load_schema(EVIDENCE_CHECKLIST_SCHEMA_FILENAME)


def _load_schema(filename: str) -> dict[str, Any]:
    path = schema_path(filename)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"Schema root must be a JSON object: {path}")
    schema = canonicalize_json(raw)
    if not isinstance(schema, dict):
        raise ValueError(f"Schema canonicalization failed for: {path}")
    return schema
