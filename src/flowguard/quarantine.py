"""Deterministic gate failure classification and fast-fail quarantine manager."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class GateFailureClassification(str, Enum):
    PIN_MISMATCH = "pin_mismatch"
    PROVIDER_RATE_LIMIT = "provider_rate_limit"
    ENVIRONMENT_MISSING = "environment_missing"
    TRANSIENT_TIMEOUT = "transient_timeout"
    GENERAL_FAILURE = "general_failure"


@dataclass(frozen=True)
class QuarantineRecord:
    task_id: str
    code: GateFailureClassification
    raw_error: str
    quarantined_at: float
    retryable: bool


class QuarantineManager:
    """Detects deterministic failure patterns and segregates them from the active pipeline."""

    DETERMINISTIC_PATTERNS = [
        (re.compile(r".*_DEPENDENCY_PIN_MISMATCH.*"), GateFailureClassification.PIN_MISMATCH, False),
        (re.compile(r".*SUBLLM-PROVIDER-RATE-LIMIT.*|.*429 Too Many Requests.*"), GateFailureClassification.PROVIDER_RATE_LIMIT, True),
        (re.compile(r".*No such file or directory:.*python.*|.*ENVIRONMENT_UNAVAILABLE.*"), GateFailureClassification.ENVIRONMENT_MISSING, False),
    ]

    def __init__(self) -> None:
        self._records: list[QuarantineRecord] = []

    def classify_and_quarantine(self, task_id: str, error_message: str, timestamp: float) -> tuple[GateFailureClassification, bool]:
        """Classify error message and record in quarantine ledger."""
        for pattern, classification, retryable in self.DETERMINISTIC_PATTERNS:
            if pattern.search(error_message):
                record = QuarantineRecord(
                    task_id=task_id,
                    code=classification,
                    raw_error=error_message,
                    quarantined_at=timestamp,
                    retryable=retryable,
                )
                self._records.append(record)
                return classification, retryable

        # Default fallback
        record = QuarantineRecord(
            task_id=task_id,
            code=GateFailureClassification.GENERAL_FAILURE,
            raw_error=error_message,
            quarantined_at=timestamp,
            retryable=False,
        )
        self._records.append(record)
        return GateFailureClassification.GENERAL_FAILURE, False

    @property
    def records(self) -> list[QuarantineRecord]:
        return list(self._records)
