from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from typing import Any, Final, cast

from jsonschema import Draft202012Validator, FormatChecker

from pmx.claims.schemas import (
    CLAIM_EXTRACT_SCHEMA_VERSION,
    EVIDENCE_CHECKLIST_SCHEMA_VERSION,
    load_claim_extract_schema,
    load_evidence_checklist_schema,
)
from pmx.news.normalize import canonicalize_json, canonicalize_url

MAX_RAW_CLAIMS: Final[int] = 80
MAX_CANONICAL_CLAIMS: Final[int] = 25
MAX_SOURCES_PER_CLAIM: Final[int] = 10


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    code: str
    path: str
    reason: str


@dataclass(frozen=True, slots=True)
class ValidatedClaims:
    payload: dict[str, Any]
    schema_version: str
    raw_claim_count: int
    canonical_claim_count: int


@dataclass(frozen=True, slots=True)
class ValidatedChecklist:
    payload: dict[str, Any]
    schema_version: str
    item_count: int


class PayloadValidationError(ValueError):
    def __init__(self, payload_type: str, issues: Sequence[ValidationIssue]) -> None:
        self.payload_type = payload_type
        self.issues = tuple(_sort_issues(issues))
        super().__init__(
            f"{payload_type} validation failed with {len(self.issues)} error(s): "
            f"{'; '.join(_format_issue(issue) for issue in self.issues)}"
        )


def validate_claim_extract(payload: Any) -> ValidatedClaims:
    canonical_payload = _to_canonical_object(payload)
    issues = _collect_schema_issues(canonical_payload, _claim_extract_validator())
    issues.extend(_collect_claim_extract_custom_issues(canonical_payload))
    sorted_issues = _sort_issues(issues)
    if sorted_issues:
        raise PayloadValidationError("claim_extract", sorted_issues)

    claims = canonical_payload.get("claims")
    canonical_claims = canonical_payload.get("canonical_claims")
    raw_claim_count = len(claims) if isinstance(claims, list) else 0
    canonical_claim_count = len(canonical_claims) if isinstance(canonical_claims, list) else 0
    return ValidatedClaims(
        payload=canonical_payload,
        schema_version=CLAIM_EXTRACT_SCHEMA_VERSION,
        raw_claim_count=raw_claim_count,
        canonical_claim_count=canonical_claim_count,
    )


def validate_evidence_checklist(payload: Any) -> ValidatedChecklist:
    canonical_payload = _to_canonical_object(payload)
    issues = _collect_schema_issues(canonical_payload, _evidence_checklist_validator())
    issues.extend(_collect_checklist_custom_issues(canonical_payload))
    sorted_issues = _sort_issues(issues)
    if sorted_issues:
        raise PayloadValidationError("evidence_checklist", sorted_issues)

    items = canonical_payload.get("items")
    item_count = len(items) if isinstance(items, list) else 0
    return ValidatedChecklist(
        payload=canonical_payload,
        schema_version=EVIDENCE_CHECKLIST_SCHEMA_VERSION,
        item_count=item_count,
    )


@cache
def _claim_extract_validator() -> Draft202012Validator:
    return Draft202012Validator(load_claim_extract_schema(), format_checker=FormatChecker())


@cache
def _evidence_checklist_validator() -> Draft202012Validator:
    return Draft202012Validator(load_evidence_checklist_schema(), format_checker=FormatChecker())


def _to_canonical_object(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        issue = ValidationIssue(
            code="payload_type",
            path="$",
            reason="Payload root must be an object",
        )
        raise PayloadValidationError("payload", [issue])
    canonical = canonicalize_json(payload)
    if not isinstance(canonical, dict):
        issue = ValidationIssue(
            code="payload_type",
            path="$",
            reason="Canonicalized payload root must be an object",
        )
        raise PayloadValidationError("payload", [issue])
    return cast(dict[str, Any], canonical)


def _collect_schema_issues(
    payload: Mapping[str, Any],
    validator: Draft202012Validator,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    errors = sorted(
        validator.iter_errors(payload),
        key=lambda error: (_json_path(error.path), error.validator, error.message),
    )
    for error in errors:
        issues.append(
            ValidationIssue(
                code=f"schema:{error.validator}",
                path=_json_path(error.path),
                reason=error.message,
            )
        )
    return issues


def _collect_claim_extract_custom_issues(payload: Mapping[str, Any]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    claims = payload.get("claims")
    if isinstance(claims, list):
        if len(claims) > MAX_RAW_CLAIMS:
            issues.append(
                ValidationIssue(
                    code="max_raw_claims_exceeded",
                    path="$.claims",
                    reason=f"raw claim count {len(claims)} exceeds {MAX_RAW_CLAIMS}",
                )
            )
        issues.extend(_collect_claim_source_issues(claims, "$.claims"))

    canonical_claims = payload.get("canonical_claims")
    if isinstance(canonical_claims, list):
        if len(canonical_claims) > MAX_CANONICAL_CLAIMS:
            issues.append(
                ValidationIssue(
                    code="max_canonical_claims_exceeded",
                    path="$.canonical_claims",
                    reason=(
                        f"canonical claim count {len(canonical_claims)} exceeds "
                        f"{MAX_CANONICAL_CLAIMS}"
                    ),
                )
            )
        issues.extend(_collect_claim_source_issues(canonical_claims, "$.canonical_claims"))
    return issues


def _collect_checklist_custom_issues(payload: Mapping[str, Any]) -> list[ValidationIssue]:
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    return _collect_claim_source_issues(items, "$.items")


def _collect_claim_source_issues(rows: Sequence[Any], root_path: str) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for claim_idx, claim_payload in enumerate(rows):
        if not isinstance(claim_payload, Mapping):
            continue

        sources = _extract_sources(claim_payload)
        if sources is None:
            continue

        source_path = f"{root_path}[{claim_idx}].sources"
        if len(sources) > MAX_SOURCES_PER_CLAIM:
            issues.append(
                ValidationIssue(
                    code="max_sources_per_claim_exceeded",
                    path=source_path,
                    reason=f"source count {len(sources)} exceeds {MAX_SOURCES_PER_CLAIM}",
                )
            )

        seen_urls: dict[str, int] = {}
        for source_idx, source_payload in enumerate(sources):
            if not isinstance(source_payload, Mapping):
                continue
            normalized_url = _normalize_source_url(source_payload.get("url"))
            if normalized_url is None:
                continue
            if normalized_url in seen_urls:
                first_idx = seen_urls[normalized_url]
                issues.append(
                    ValidationIssue(
                        code="duplicate_source_url",
                        path=f"{source_path}[{source_idx}].url",
                        reason=(
                            f"duplicate source URL in claim at index {first_idx}: {normalized_url}"
                        ),
                    )
                )
            else:
                seen_urls[normalized_url] = source_idx
    return issues


def _extract_sources(claim_payload: Mapping[str, Any]) -> list[Any] | None:
    sources = claim_payload.get("sources")
    if isinstance(sources, list):
        return sources
    source_urls = claim_payload.get("source_urls")
    if isinstance(source_urls, list):
        return [{"url": value} for value in source_urls]
    return None


def _normalize_source_url(raw_url: Any) -> str | None:
    if raw_url is None:
        return None
    text = str(raw_url).strip()
    if not text:
        return None
    canonical = canonicalize_url(text)
    if canonical is not None:
        return canonical
    return text


def _json_path(path: Iterable[Any]) -> str:
    result = "$"
    for token in path:
        if isinstance(token, int):
            result += f"[{token}]"
        else:
            result += f".{token}"
    return result


def _sort_issues(issues: Sequence[ValidationIssue]) -> list[ValidationIssue]:
    return sorted(
        issues,
        key=lambda issue: (issue.path, issue.code, issue.reason),
    )


def _format_issue(issue: ValidationIssue) -> str:
    return f"{issue.code}@{issue.path}: {issue.reason}"
