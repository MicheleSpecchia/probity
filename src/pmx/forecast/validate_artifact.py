from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from pmx.news.normalize import canonicalize_json

FORECAST_ARTIFACT_SCHEMA_VERSION = "forecast_artifact.v1"
FORECAST_ARTIFACT_SCHEMA_FILENAME = "forecast_artifact.v1.json"
SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas"


def validate_forecast_artifact(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return [
            {
                "code": "payload_type",
                "path": "$",
                "reason": "Payload root must be an object",
            }
        ]

    canonical = canonicalize_json(payload)
    if not isinstance(canonical, dict):
        return [
            {
                "code": "payload_type",
                "path": "$",
                "reason": "Canonicalized payload root must be an object",
            }
        ]

    validator = _artifact_validator()
    errors = sorted(
        validator.iter_errors(canonical),
        key=lambda error: (_json_path(error.path), error.validator, error.message),
    )
    issues: list[dict[str, Any]] = []
    for error in errors:
        issues.append(
            {
                "code": f"schema:{error.validator}",
                "path": _json_path(error.path),
                "reason": error.message,
            }
        )
    return issues


@cache
def _artifact_validator() -> Draft202012Validator:
    return Draft202012Validator(_load_artifact_schema())


def _load_artifact_schema() -> dict[str, Any]:
    path = SCHEMAS_DIR / FORECAST_ARTIFACT_SCHEMA_FILENAME
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


def _json_path(path: Iterable[Any]) -> str:
    result = "$"
    for token in path:
        if isinstance(token, int):
            result += f"[{token}]"
        else:
            result += f".{token}"
    return result
