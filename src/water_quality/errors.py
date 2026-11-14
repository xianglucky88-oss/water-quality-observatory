from __future__ import annotations

from typing import Any


def describe_error(exc: BaseException) -> str:
    """Stable failure text that keeps the class name for later triage."""
    return f"{type(exc).__name__}: {exc}"


class IngestionError(RuntimeError):
    """Base class for errors that carry a machine-readable recovery report."""

    def __init__(self, message: str, report: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.report = report


class InjectedFault(IngestionError):
    """Raised by a controlled fault injector at a durable checkpoint."""


class CrashSignal(BaseException):
    """Optional hard-stop signal used to emulate an abrupt process death."""


class LeaseLostError(IngestionError):
    """Raised when another worker owns the batch lease."""


class SettlementError(IngestionError):
    """Raised when the durable evidence cannot be settled consistently."""


class BatchInProgressError(IngestionError):
    """Raised when another live worker still owns the batch."""


class UnrecoverableBatchError(IngestionError):
    """Raised when automatic recovery is not safe."""


class AmbiguousBatchError(IngestionError):
    """Raised when a digest maps to more than one source batch."""


class EvidenceError(IngestionError):
    """Raised when raw evidence contradicts its registered digest."""
