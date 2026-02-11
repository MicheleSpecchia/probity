from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pmx.claims.echo import EchoMetrics, compute_echo_metrics
from pmx.claims.graph import (
    DEFAULT_SIMILARITY_THRESHOLD,
    ClaimCluster,
    GraphResult,
    SourceRecord,
    build_claim_graph,
    claim_fingerprint,
)
from pmx.news.normalize import canonicalize_url

DEFAULT_MAX_CANONICAL_CLAIMS = 25
MAX_PRIMARY_SOURCES_PER_CLAIM = 10


@dataclass(frozen=True, slots=True)
class CanonicalClaim:
    canonical_claim_id: int
    claim_canonical: str
    claim_hash: str
    representative_claim_id: int
    claim_ids: tuple[int, ...]
    source_urls: tuple[str, ...]
    sources: tuple[dict[str, str | None], ...]
    metrics: EchoMetrics

    def as_dict(self) -> dict[str, Any]:
        return {
            "canonical_claim_id": self.canonical_claim_id,
            "claim_canonical": self.claim_canonical,
            "claim_hash": self.claim_hash,
            "representative_claim_id": self.representative_claim_id,
            "claim_ids": list(self.claim_ids),
            "source_urls": list(self.source_urls),
            "sources": list(self.sources),
            "metrics": self.metrics.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class CanonicalizationResult:
    canonical_claims: tuple[CanonicalClaim, ...]
    claim_to_canonical: dict[int, int]
    dropped_claim_ids: tuple[int, ...]
    graph: GraphResult

    def as_dict(self) -> dict[str, Any]:
        return {
            "canonical_claims": [claim.as_dict() for claim in self.canonical_claims],
            "claim_to_canonical": {
                str(claim_id): canonical_id
                for claim_id, canonical_id in sorted(self.claim_to_canonical.items())
            },
            "dropped_claim_ids": list(self.dropped_claim_ids),
            "graph": self.graph.as_dict(),
        }


def canonicalize_claims(
    claims_raw: Sequence[Mapping[str, Any]],
    *,
    max_canonical: int = DEFAULT_MAX_CANONICAL_CLAIMS,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> CanonicalizationResult:
    if max_canonical <= 0:
        raise ValueError("max_canonical must be > 0")

    graph = build_claim_graph(
        claims_raw,
        similarity_threshold=similarity_threshold,
    )

    canonical_claims: list[CanonicalClaim] = []
    claim_to_canonical: dict[int, int] = {}

    for cluster in graph.clusters:
        canonical_claim_id = len(canonical_claims) + 1
        canonical = _canonical_from_cluster(
            canonical_claim_id=canonical_claim_id,
            cluster=cluster,
        )
        canonical_claims.append(canonical)
        for claim_id in cluster.claim_ids:
            claim_to_canonical[claim_id] = canonical_claim_id

    dropped_claim_ids: list[int] = []
    if len(canonical_claims) > max_canonical:
        kept_ids = {claim.canonical_claim_id for claim in canonical_claims[:max_canonical]}
        for claim in canonical_claims[max_canonical:]:
            dropped_claim_ids.extend(claim.claim_ids)
        claim_to_canonical = {
            claim_id: canonical_id
            for claim_id, canonical_id in claim_to_canonical.items()
            if canonical_id in kept_ids
        }
        canonical_claims = canonical_claims[:max_canonical]

    return CanonicalizationResult(
        canonical_claims=tuple(canonical_claims),
        claim_to_canonical=claim_to_canonical,
        dropped_claim_ids=tuple(sorted(dropped_claim_ids)),
        graph=graph,
    )


def _canonical_from_cluster(
    *,
    canonical_claim_id: int,
    cluster: ClaimCluster,
) -> CanonicalClaim:
    unique_sources = _dedupe_sources(cluster.source_records)
    capped_sources = _cap_primary_sources(unique_sources, max_primary=MAX_PRIMARY_SOURCES_PER_CLAIM)
    source_urls = tuple(source.url for source in capped_sources if source.url)
    metrics = compute_echo_metrics(capped_sources)
    return CanonicalClaim(
        canonical_claim_id=canonical_claim_id,
        claim_canonical=cluster.representative_text,
        claim_hash=claim_fingerprint(cluster.representative_text),
        representative_claim_id=cluster.representative_claim_id,
        claim_ids=cluster.claim_ids,
        source_urls=source_urls,
        sources=tuple(source.as_dict() for source in capped_sources),
        metrics=metrics,
    )


def _dedupe_sources(sources: Sequence[SourceRecord]) -> list[SourceRecord]:
    unique: dict[tuple[str, str, str], SourceRecord] = {}
    for source in sources:
        canonical_url = canonicalize_url(source.url) or source.url
        published = source.published_at.isoformat() if source.published_at is not None else ""
        key = (source.domain, canonical_url, source.source_type)
        previous = unique.get(key)
        if previous is None:
            unique[key] = SourceRecord(
                url=canonical_url,
                domain=source.domain,
                published_at=source.published_at,
                source_type=source.source_type,
            )
            continue
        prev_published = (
            previous.published_at.isoformat() if previous.published_at is not None else ""
        )
        if published and (not prev_published or published < prev_published):
            unique[key] = SourceRecord(
                url=canonical_url,
                domain=source.domain,
                published_at=source.published_at,
                source_type=source.source_type,
            )

    deduped = list(unique.values())
    deduped.sort(
        key=lambda source: (
            source.domain,
            source.url,
            source.published_at.isoformat() if source.published_at is not None else "",
            source.source_type,
        )
    )
    return deduped


def _cap_primary_sources(
    sources: Sequence[SourceRecord],
    *,
    max_primary: int,
) -> list[SourceRecord]:
    primary: list[SourceRecord] = []
    non_primary: list[SourceRecord] = []
    for source in sources:
        if source.source_type == "primary":
            primary.append(source)
        else:
            non_primary.append(source)

    return primary[:max_primary] + non_primary
