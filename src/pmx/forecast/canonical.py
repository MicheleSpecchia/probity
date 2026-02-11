from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


def canonicalize_for_hash(payload: Any, *, float_decimals: int = 6) -> Any:
    if isinstance(payload, Mapping):
        items = sorted(payload.items(), key=lambda item: str(item[0]))
        return {
            str(key): canonicalize_for_hash(value, float_decimals=float_decimals)
            for key, value in items
        }

    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return [canonicalize_for_hash(item, float_decimals=float_decimals) for item in payload]

    if isinstance(payload, float):
        rounded = round(payload, float_decimals)
        # Normalize negative zero for stable serialization.
        return 0.0 if rounded == 0.0 else rounded

    if isinstance(payload, (str, int, bool)) or payload is None:
        return payload

    return str(payload)


def canonical_json_dumps(payload: Any, *, float_decimals: int = 6) -> str:
    normalized = canonicalize_for_hash(payload, float_decimals=float_decimals)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_hash(payload: Any, *, float_decimals: int = 6) -> str:
    serialized = canonical_json_dumps(payload, float_decimals=float_decimals)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
