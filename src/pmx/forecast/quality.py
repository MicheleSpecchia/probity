from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def merge_quality_flags(*groups: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({flag for group in groups for flag in group if flag}))


def merge_quality_warnings(
    *groups: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, str], ...]:
    deduped: dict[tuple[str, str], dict[str, str]] = {}
    for group in groups:
        for item in group:
            normalized = normalize_quality_warning(item)
            key = (normalized["code"], normalized.get("message", ""))
            deduped[key] = normalized
    keys = sorted(deduped.keys(), key=lambda key: (key[0], key[1]))
    return tuple(deduped[key] for key in keys)


def normalize_quality_warning(raw: Mapping[str, Any]) -> dict[str, str]:
    code = str(raw.get("code", "")).strip()
    message_raw = raw.get("message")
    if message_raw is None:
        message_raw = raw.get("detail")
    message = str(message_raw).strip() if message_raw is not None else ""
    if not code:
        code = "unknown_warning"
    payload: dict[str, str] = {"code": code}
    if message:
        payload["message"] = message
    return payload
