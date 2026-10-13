"""Small application-owned contracts for managed sandbox operations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class EffectState(StrEnum):
    """What the caller can establish about one remote operation."""

    APPLIED = "applied"
    NOT_APPLIED = "not_applied"
    UNKNOWN = "unknown"


class SandboxAction(StrEnum):
    """Named operations whose outcomes can need reconciliation."""

    CREATE = "create"
    COMMAND = "command"
    FILE_WRITE = "file_write"
    NETWORK_POLICY = "network_policy"
    STOP = "stop"
    DESTROY = "destroy"
    OBSERVE = "observe"


class SandboxEffectError(RuntimeError):
    """A sanitized operation result that never carries SDK response text."""

    def __init__(self, action: SandboxAction, effect: EffectState, reason: str) -> None:
        self.action = action
        self.effect = effect
        self.reason = reason
        super().__init__(action, effect, reason)

    def __str__(self) -> str:
        return f"sandbox {self.action.value} effect is {self.effect.value}: {self.reason}"


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Bounded result from one completed remote process."""

    exit_code: int | None
    stdout: str
    stderr: str
    effect: EffectState = EffectState.APPLIED


@dataclass(frozen=True, slots=True)
class SandboxObservation:
    """Read-only state returned for an allocation name."""

    name: str
    sandbox_status: str | None
    session_id: str | None
    session_status: str | None


@dataclass(frozen=True, slots=True)
class ProcessObservation:
    """Read-only status for one provider process identifier."""

    process_id: str
    status: str | None
    exit_code: int | None
