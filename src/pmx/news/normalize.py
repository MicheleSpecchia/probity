from __future__ import annotations

import hashlib
import re
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "gbraid",
    "mc_cid",
    "mc_eid",
    "mkt_tok",
    "wbraid",
}
_WHITESPACE_RE = re.compile(r"\s+")


def canonicalize_url(raw_url: str | None) -> str | None:
    if raw_url is None:
        return None
    url_text = raw_url.strip()
    if not url_text:
        return None

    split = urlsplit(url_text)
    if not split.scheme or not split.netloc:
        return None

    hostname = split.hostname.lower() if split.hostname else ""
    if hostname.startswith("www."):
        hostname = hostname[4:]
    if not hostname:
        return None

    netloc = hostname
    if split.port is not None:
        netloc = f"{hostname}:{split.port}"

    filtered_pairs: list[tuple[str, str]] = []
    for key, value in parse_qsl(split.query, keep_blank_values=True):
        normalized_key = key.strip()
        lowered_key = normalized_key.lower()
        if lowered_key.startswith("utm_"):
            continue
        if lowered_key in _TRACKING_PARAMS:
            continue
        filtered_pairs.append((normalized_key, value))

    filtered_pairs.sort(key=lambda item: (item[0], item[1]))
    canonical_query = urlencode(filtered_pairs, doseq=True)
    canonical_path = split.path or "/"

    return urlunsplit(
        (
            split.scheme.lower(),
            netloc,
            canonical_path,
            canonical_query,
            "",
        )
    )


def extract_domain(raw_url: str | None) -> str | None:
    if raw_url is None:
        return None
    split = urlsplit(raw_url.strip())
    hostname = split.hostname.lower() if split.hostname else ""
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname or None


def normalize_text(raw_text: str | None) -> str:
    if raw_text is None:
        return ""
    normalized = _WHITESPACE_RE.sub(" ", raw_text.strip().lower())
    return normalized


def sha256_hex(raw_text: str | None) -> str:
    normalized = normalize_text(raw_text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def canonicalize_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda item: str(item[0]))
        return {str(key): canonicalize_json(item_value) for key, item_value in items}
    if isinstance(value, list):
        return [canonicalize_json(item) for item in value]
    if isinstance(value, tuple):
        return [canonicalize_json(item) for item in value]
    return value
