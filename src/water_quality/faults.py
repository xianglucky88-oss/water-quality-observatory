from __future__ import annotations

import threading

from .errors import CrashSignal, InjectedFault

CHECKPOINTS = {
    "after_evidence": "raw evidence, input snapshot and running batch are durable",
    "after_first_write": "the first accepted structured row and its ledger entry are durable",
    "after_rejection": "the first rejection row and its ledger entry are durable",
    "before_finalize": "all items are durable but the batch has not been settled",
    "after_finalize": "the batch is settled and the result is ready to return",
}

_MODE_RAISE = "raise"
_MODE_CRASH = "crash"


class FaultInjector:
    """One-shot checkpoint interrupter, parsed from ``--fail-at`` style values."""

    def __init__(self, spec: str | None = None) -> None:
        self._faults: dict[str, str] = {}
        self._lock = threading.Lock()
        self.hits: list[str] = []
        if spec:
            for part in spec.split(","):
                token = part.strip()
                if not token:
                    continue
                pieces = token.split(":")
                checkpoint = pieces[0]
                mode = pieces[1] if len(pieces) > 1 else _MODE_RAISE
                if checkpoint not in CHECKPOINTS:
                    raise ValueError(f"unknown fault checkpoint: {checkpoint}")
                if mode not in {_MODE_RAISE, _MODE_CRASH}:
                    raise ValueError(f"unknown fault mode: {mode}")
                if checkpoint in self._faults:
                    raise ValueError(f"duplicate fault checkpoint: {checkpoint}")
                self._faults[checkpoint] = mode

    def armed(self) -> bool:
        return bool(self._faults)

    def trigger(self, checkpoint: str, detail: dict[str, object] | None = None) -> None:
        with self._lock:
            mode = self._faults.pop(checkpoint, None)
            if mode is None:
                return
            self.hits.append(checkpoint)
        payload = {"checkpoint": checkpoint, **(detail or {})}
        if mode == _MODE_CRASH:
            raise CrashSignal(f"process interrupted at {checkpoint}")
        raise InjectedFault(f"injected fault at {checkpoint}", payload)
