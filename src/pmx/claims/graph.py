from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pmx.news.normalize import canonicalize_url, extract_domain

DEFAULT_SIMILARITY_THRESHOLD = 0.5

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_PUNCT_RE = re.compile(r"[^a-z0-9\s]")
_WHITESPACE_RE = re.compile(r"\s+")
_MAX_DT = datetime.max.replace(tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class SourceRecord:
    url: str
    domain: str
    published_at: datetime | None
    source_type: str

    def as_dict(self) -> dict[str, str | None]:
        return {
            "url": self.url,
            "domain": self.domain,
            "published_at": self.published_at.isoformat()
            if self.published_at is not None
            else None,
            "source_type": self.source_type,
        }


@dataclass(frozen=True, slots=True)
class ClaimNode:
    claim_id: int
    claim_text: str
    normalized_text: str
    tokens: frozenset[str]
    fingerprint: str
    sources: tuple[SourceRecord, ...]
    published_at_min: datetime | None
    domain_min: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "claim_text": self.claim_text,
            "normalized_text": self.normalized_text,
            "fingerprint": self.fingerprint,
            "published_at_min": (
                self.published_at_min.isoformat() if self.published_at_min is not None else None
            ),
            "domain_min": self.domain_min,
            "sources": [source.as_dict() for source in self.sources],
        }


@dataclass(frozen=True, slots=True)
class ClaimCluster:
    cluster_id: int
    claim_ids: tuple[int, ...]
    representative_claim_id: int
    representative_text: str
    source_records: tuple[SourceRecord, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "claim_ids": list(self.claim_ids),
            "representative_claim_id": self.representative_claim_id,
            "representative_text": self.representative_text,
            "source_records": [source.as_dict() for source in self.source_records],
        }


@dataclass(frozen=True, slots=True)
class GraphResult:
    claims: tuple[ClaimNode, ...]
    clusters: tuple[ClaimCluster, ...]
    claim_to_cluster: dict[int, int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "claims": [claim.as_dict() for claim in self.claims],
            "clusters": [cluster.as_dict() for cluster in self.clusters],
            "claim_to_cluster": {
                str(claim_id): cluster_id
                for claim_id, cluster_id in sorted(self.claim_to_cluster.items())
            },
        }


def normalize_claim_text(text: str) -> str:
    lowered = text.lower().strip()
    without_punct = _PUNCT_RE.sub(" ", lowered)
    collapsed = _WHITESPACE_RE.sub(" ", without_punct).strip()
    return collapsed


def claim_fingerprint(text: str) -> str:
    normalized = normalize_claim_text(text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def similarity(left: str, right: str) -> float:
    left_tokens = _tokenize(normalize_claim_text(left))
    right_tokens = _tokenize(normalize_claim_text(right))
    return _token_jaccard(left_tokens, right_tokens)


def build_claim_graph(
    claims_raw: Sequence[Mapping[str, Any]],
    *,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> GraphResult:
    if similarity_threshold < 0 or similarity_threshold > 1:
        raise ValueError("similarity_threshold must be between 0 and 1")

    nodes = _build_claim_nodes(claims_raw)
    sorted_nodes = sorted(nodes, key=_claim_sort_key)

    mutable_clusters: list[_MutableCluster] = []
    claim_to_cluster: dict[int, int] = {}

    for node in sorted_nodes:
        matched_cluster: _MutableCluster | None = None
        for cluster in mutable_clusters:
            if _cluster_similarity(node, cluster) >= similarity_threshold:
                matched_cluster = cluster
                break

        if matched_cluster is None:
            cluster_id = len(mutable_clusters) + 1
            new_cluster = _MutableCluster(
                cluster_id=cluster_id,
                claim_ids=[node.claim_id],
                representative_claim_id=node.claim_id,
                representative_text=node.claim_text,
                representative_tokens=node.tokens,
                source_records=list(node.sources),
            )
            mutable_clusters.append(new_cluster)
            claim_to_cluster[node.claim_id] = cluster_id
            continue

        matched_cluster.claim_ids.append(node.claim_id)
        claim_to_cluster[node.claim_id] = matched_cluster.cluster_id
        matched_cluster.source_records.extend(node.sources)
        if _is_better_representative(node, matched_cluster):
            matched_cluster.representative_claim_id = node.claim_id
            matched_cluster.representative_text = node.claim_text
            matched_cluster.representative_tokens = node.tokens

    frozen_clusters = tuple(_freeze_cluster(cluster) for cluster in mutable_clusters)
    frozen_claims = tuple(sorted(nodes, key=lambda node: node.claim_id))
    return GraphResult(
        claims=frozen_claims,
        clusters=frozen_clusters,
        claim_to_cluster=claim_to_cluster,
    )


@dataclass(slots=True)
class _MutableCluster:
    cluster_id: int
    claim_ids: list[int]
    representative_claim_id: int
    representative_text: str
    representative_tokens: frozenset[str]
    source_records: list[SourceRecord]


def _build_claim_nodes(claims_raw: Sequence[Mapping[str, Any]]) -> list[ClaimNode]:
    seen_ids: set[int] = set()
    nodes: list[ClaimNode] = []
    for index, claim_raw in enumerate(claims_raw, start=1):
        claim_id = _resolve_claim_id(claim_raw, index=index, seen_ids=seen_ids)
        claim_text = _as_text(claim_raw.get("claim_text")) or ""
        normalized_text = normalize_claim_text(claim_text)
        tokens = _tokenize(normalized_text)
        sources = _parse_sources(claim_raw.get("sources"))
        published_at_min = _published_at_min(sources)
        domain_min = _domain_min(sources)
        nodes.append(
            ClaimNode(
                claim_id=claim_id,
                claim_text=claim_text,
                normalized_text=normalized_text,
                tokens=tokens,
                fingerprint=claim_fingerprint(claim_text),
                sources=sources,
                published_at_min=published_at_min,
                domain_min=domain_min,
            )
        )
        seen_ids.add(claim_id)
    return nodes


def _parse_sources(raw_sources: Any) -> tuple[SourceRecord, ...]:
    if not isinstance(raw_sources, list):
        return ()

    parsed: list[SourceRecord] = []
    for source_raw in raw_sources:
        if not isinstance(source_raw, Mapping):
            continue
        raw_url = _as_text(source_raw.get("url")) or ""
        canonical_url = canonicalize_url(raw_url) or raw_url
        domain = (
            _as_text(source_raw.get("domain")) or extract_domain(canonical_url) or "unknown.local"
        )
        source_type = _normalize_source_type(source_raw)
        published_at = _parse_optional_datetime(source_raw.get("published_at"))
        parsed.append(
            SourceRecord(
                url=canonical_url,
                domain=domain,
                published_at=published_at,
                source_type=source_type,
            )
        )

    parsed.sort(
        key=lambda source: (
            source.domain,
            source.url,
            source.published_at.isoformat() if source.published_at is not None else "",
            source.source_type,
        )
    )
    return tuple(parsed)


def _normalize_source_type(source_raw: Mapping[str, Any]) -> str:
    raw = _as_text(source_raw.get("source_type"))
    if raw in {"primary", "secondary", "unknown"}:
        return raw
    if bool(source_raw.get("is_primary")):
        return "primary"
    return "unknown"


def _resolve_claim_id(
    claim_raw: Mapping[str, Any],
    *,
    index: int,
    seen_ids: set[int],
) -> int:
    raw_claim_id = claim_raw.get("claim_id")
    parsed = _optional_int(raw_claim_id)
    if parsed is None or parsed in seen_ids:
        candidate = index
        while candidate in seen_ids:
            candidate += 1
        return candidate
    return parsed


def _claim_sort_key(node: ClaimNode) -> tuple[datetime, str, int]:
    return (
        node.published_at_min if node.published_at_min is not None else _MAX_DT,
        node.domain_min,
        node.claim_id,
    )


def _cluster_similarity(node: ClaimNode, cluster: _MutableCluster) -> float:
    max_similarity = _token_jaccard(node.tokens, cluster.representative_tokens)
    return max_similarity


def _is_better_representative(node: ClaimNode, cluster: _MutableCluster) -> bool:
    current = (len(cluster.representative_text), cluster.representative_claim_id)
    candidate = (len(node.claim_text), node.claim_id)
    return candidate < current


def _freeze_cluster(cluster: _MutableCluster) -> ClaimCluster:
    sorted_claim_ids = tuple(sorted(cluster.claim_ids))
    source_records = tuple(
        sorted(
            cluster.source_records,
            key=lambda source: (
                source.domain,
                source.url,
                source.published_at.isoformat() if source.published_at is not None else "",
                source.source_type,
            ),
        )
    )
    return ClaimCluster(
        cluster_id=cluster.cluster_id,
        claim_ids=sorted_claim_ids,
        representative_claim_id=cluster.representative_claim_id,
        representative_text=cluster.representative_text,
        source_records=source_records,
    )


def _tokenize(text: str) -> frozenset[str]:
    if not text:
        return frozenset()
    return frozenset(_TOKEN_RE.findall(text))


def _token_jaccard(left: frozenset[str], right: frozenset[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    intersection = len(left & right)
    union = len(left | right)
    if union == 0:
        return 0.0
    return intersection / union


def _published_at_min(sources: Sequence[SourceRecord]) -> datetime | None:
    published = [source.published_at for source in sources if source.published_at is not None]
    if not published:
        return None
    return min(published)


def _domain_min(sources: Sequence[SourceRecord]) -> str:
    domains = [source.domain for source in sources if source.domain]
    if not domains:
        return "unknown.local"
    return min(domains)


def _optional_int(raw: Any) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _parse_optional_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc_datetime(raw)
    text = _as_text(raw)
    if text is None:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return _as_utc_datetime(parsed)


def _as_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
