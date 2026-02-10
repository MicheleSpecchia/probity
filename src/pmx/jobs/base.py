from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from pmx.audit.logging import get_logger
from pmx.audit.run_context import RunContext, build_run_context
from pmx.config.settings import Settings


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Small placeholder for retry behavior shared by future jobs."""

    max_attempts: int = 3
    backoff_seconds: float = 2.0


class BaseJob(ABC):
    """Shared skeleton for deterministic, auditable jobs."""

    def __init__(self, settings: Settings, retry_policy: RetryPolicy | None = None) -> None:
        self.settings = settings
        self.retry_policy = retry_policy or RetryPolicy()
        self.logger = get_logger(f"pmx.jobs.{self.job_name}")

    @property
    @abstractmethod
    def job_name(self) -> str:
        raise NotImplementedError

    def build_run_context(self, *, nonce: str | None = None) -> RunContext:
        return build_run_context(
            job_name=self.job_name,
            config=self.settings.as_audit_dict(),
            nonce=nonce,
        )

    def idempotency_key(self, run_context: RunContext) -> str:
        # Placeholder boundary; can later include as-of timestamp and market shard.
        return f"{self.job_name}:{run_context.config_hash}"

    def log(
        self,
        level: int,
        message: str,
        run_context: RunContext,
        **extra_fields: Any,
    ) -> None:
        payload: dict[str, Any] = run_context.as_log_context()
        payload["extra_fields"] = extra_fields
        self.logger.log(level, message, extra=payload)

    def execute(self) -> None:
        run_context = self.build_run_context()
        self.log(
            logging.INFO,
            "job_started",
            run_context,
            idempotency_key=self.idempotency_key(run_context),
            retry_max_attempts=self.retry_policy.max_attempts,
            retry_backoff_seconds=self.retry_policy.backoff_seconds,
        )
        self.run(run_context)
        self.log(logging.INFO, "job_completed", run_context)

    @abstractmethod
    def run(self, run_context: RunContext) -> None:
        """Implement job behavior in subclasses."""
        raise NotImplementedError
