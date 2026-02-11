"""Claim extraction schema loading and validation helpers."""

from pmx.claims.audit import build_audit_bundle, compute_prompt_hash, write_audit_bundle
from pmx.claims.canonicalize import (
    CanonicalClaim,
    CanonicalizationResult,
    canonicalize_claims,
)
from pmx.claims.echo import EchoMetrics, compute_echo_metrics, echo_penalty, source_diversity_score
from pmx.claims.extractor import (
    ExtractionOutcome,
    build_prompt,
    normalize_articles_for_prompt,
    run_extract_stub,
    validate_and_normalize,
)
from pmx.claims.graph import (
    DEFAULT_SIMILARITY_THRESHOLD,
    ClaimCluster,
    ClaimNode,
    GraphResult,
    SourceRecord,
    build_claim_graph,
    claim_fingerprint,
    normalize_claim_text,
    similarity,
)
from pmx.claims.schemas import (
    CLAIM_EXTRACT_SCHEMA_FILENAME,
    CLAIM_EXTRACT_SCHEMA_VERSION,
    EVIDENCE_CHECKLIST_SCHEMA_FILENAME,
    EVIDENCE_CHECKLIST_SCHEMA_VERSION,
    load_claim_extract_schema,
    load_evidence_checklist_schema,
)
from pmx.claims.validate import (
    PayloadValidationError,
    ValidatedChecklist,
    ValidatedClaims,
    ValidationIssue,
    validate_claim_extract,
    validate_evidence_checklist,
)

__all__ = [
    "CLAIM_EXTRACT_SCHEMA_FILENAME",
    "CLAIM_EXTRACT_SCHEMA_VERSION",
    "DEFAULT_SIMILARITY_THRESHOLD",
    "EVIDENCE_CHECKLIST_SCHEMA_FILENAME",
    "EVIDENCE_CHECKLIST_SCHEMA_VERSION",
    "CanonicalClaim",
    "CanonicalizationResult",
    "ClaimCluster",
    "ClaimNode",
    "EchoMetrics",
    "ExtractionOutcome",
    "GraphResult",
    "PayloadValidationError",
    "SourceRecord",
    "ValidatedChecklist",
    "ValidatedClaims",
    "ValidationIssue",
    "build_audit_bundle",
    "build_claim_graph",
    "build_prompt",
    "canonicalize_claims",
    "claim_fingerprint",
    "compute_echo_metrics",
    "compute_prompt_hash",
    "echo_penalty",
    "load_claim_extract_schema",
    "load_evidence_checklist_schema",
    "normalize_articles_for_prompt",
    "normalize_claim_text",
    "run_extract_stub",
    "similarity",
    "source_diversity_score",
    "validate_and_normalize",
    "validate_claim_extract",
    "validate_evidence_checklist",
    "write_audit_bundle",
]
