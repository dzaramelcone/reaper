"""Closed outcomes for every nondeterministic runtime call."""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from reaper.runtime import RuntimeOperation


class OutcomeKind(StrEnum):
    SUCCESS = "success"
    DELAYED = "delayed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"
    SHORT = "short"
    EOF = "eof"
    PERMANENT_ERROR = "permanent_error"
    RESOURCE_LIMIT = "resource_limit"
    NOT_FOUND = "not_found"
    PERMISSION = "permission"
    ALREADY_CLOSED = "already_closed"
    PROCESS_EXIT = "process_exit"
    CONNECTION_LOST = "connection_lost"
    TIMEOUT = "timeout"
    LOCK_TIMEOUT = "lock_timeout"
    STATEMENT_TIMEOUT = "statement_timeout"
    AUTH_FAILED = "auth_failed"
    CONFLICT = "conflict"
    DEADLOCK = "deadlock"
    SERIALIZATION = "serialization"
    PROTOCOL_ERROR = "protocol_error"
    LOST = "lost"
    DUPLICATE = "duplicate"
    REORDERED = "reordered"
    CLOCK_JUMP = "clock_jump"


class FaultPhase(StrEnum):
    """Place an observable fault before or after the underlying side effect."""

    BEFORE = "before"
    AFTER = "after"


CALL_OUTCOMES: dict[RuntimeOperation, frozenset[OutcomeKind]] = {
    RuntimeOperation.CLOCK: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CLOCK_JUMP,
        }
    ),
    RuntimeOperation.SLEEP: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.INTERRUPTED,
        }
    ),
    RuntimeOperation.SOCKET_PAIR: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.PERMANENT_ERROR,
            OutcomeKind.RESOURCE_LIMIT,
        }
    ),
    RuntimeOperation.PIPE: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.PERMANENT_ERROR,
            OutcomeKind.RESOURCE_LIMIT,
        }
    ),
    RuntimeOperation.DUP_FD: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.INTERRUPTED,
            OutcomeKind.PERMANENT_ERROR,
            OutcomeKind.RESOURCE_LIMIT,
            OutcomeKind.ALREADY_CLOSED,
        }
    ),
    RuntimeOperation.SPAWN_PROCESS: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.INTERRUPTED,
            OutcomeKind.PERMANENT_ERROR,
            OutcomeKind.RESOURCE_LIMIT,
            OutcomeKind.NOT_FOUND,
            OutcomeKind.PERMISSION,
        }
    ),
    RuntimeOperation.SPAWN_THREAD: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.PERMANENT_ERROR,
            OutcomeKind.RESOURCE_LIMIT,
        }
    ),
    RuntimeOperation.CLOSE_FD: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.INTERRUPTED,
            OutcomeKind.ALREADY_CLOSED,
            OutcomeKind.PERMANENT_ERROR,
        }
    ),
    RuntimeOperation.CONFIG_WRITE: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.INTERRUPTED,
            OutcomeKind.SHORT,
            OutcomeKind.EOF,
            OutcomeKind.PERMANENT_ERROR,
        }
    ),
    RuntimeOperation.CONTROL_SEND: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.EOF,
            OutcomeKind.PERMANENT_ERROR,
        }
    ),
    RuntimeOperation.CONTROL_RECEIVE: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.INTERRUPTED,
            OutcomeKind.SHORT,
            OutcomeKind.EOF,
            OutcomeKind.PERMANENT_ERROR,
        }
    ),
    RuntimeOperation.KILL: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.NOT_FOUND,
            OutcomeKind.PERMISSION,
            OutcomeKind.INTERRUPTED,
        }
    ),
    RuntimeOperation.WAIT_PROCESS: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.INTERRUPTED,
            OutcomeKind.NOT_FOUND,
            OutcomeKind.PROCESS_EXIT,
        }
    ),
    RuntimeOperation.HEARTBEAT_OPEN: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.INTERRUPTED,
            OutcomeKind.NOT_FOUND,
            OutcomeKind.PERMISSION,
            OutcomeKind.PERMANENT_ERROR,
            OutcomeKind.RESOURCE_LIMIT,
        }
    ),
    RuntimeOperation.HEARTBEAT_READ: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.INTERRUPTED,
            OutcomeKind.SHORT,
            OutcomeKind.EOF,
            OutcomeKind.PERMANENT_ERROR,
            OutcomeKind.ALREADY_CLOSED,
        }
    ),
    RuntimeOperation.HEARTBEAT_WRITE: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.INTERRUPTED,
            OutcomeKind.SHORT,
            OutcomeKind.PERMANENT_ERROR,
            OutcomeKind.RESOURCE_LIMIT,
            OutcomeKind.ALREADY_CLOSED,
        }
    ),
    RuntimeOperation.DB_CONNECT: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.CONNECTION_LOST,
            OutcomeKind.TIMEOUT,
            OutcomeKind.AUTH_FAILED,
            OutcomeKind.RESOURCE_LIMIT,
            OutcomeKind.PROTOCOL_ERROR,
        }
    ),
    RuntimeOperation.DB_ACQUIRE: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.CONNECTION_LOST,
            OutcomeKind.TIMEOUT,
            OutcomeKind.RESOURCE_LIMIT,
            OutcomeKind.PROTOCOL_ERROR,
        }
    ),
    RuntimeOperation.DB_BEGIN: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.CONNECTION_LOST,
            OutcomeKind.TIMEOUT,
            OutcomeKind.CONFLICT,
            OutcomeKind.DEADLOCK,
            OutcomeKind.SERIALIZATION,
            OutcomeKind.PROTOCOL_ERROR,
        }
    ),
    RuntimeOperation.DB_QUERY: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.CONNECTION_LOST,
            OutcomeKind.TIMEOUT,
            OutcomeKind.LOCK_TIMEOUT,
            OutcomeKind.STATEMENT_TIMEOUT,
            OutcomeKind.CONFLICT,
            OutcomeKind.DEADLOCK,
            OutcomeKind.SERIALIZATION,
            OutcomeKind.PROTOCOL_ERROR,
        }
    ),
    RuntimeOperation.DB_COMMIT: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.CONNECTION_LOST,
            OutcomeKind.TIMEOUT,
            OutcomeKind.LOCK_TIMEOUT,
            OutcomeKind.STATEMENT_TIMEOUT,
            OutcomeKind.CONFLICT,
            OutcomeKind.DEADLOCK,
            OutcomeKind.SERIALIZATION,
            OutcomeKind.PROTOCOL_ERROR,
        }
    ),
    RuntimeOperation.DB_ROLLBACK: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.CONNECTION_LOST,
            OutcomeKind.TIMEOUT,
            OutcomeKind.PROTOCOL_ERROR,
        }
    ),
    RuntimeOperation.DB_RELEASE: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.CONNECTION_LOST,
            OutcomeKind.TIMEOUT,
            OutcomeKind.PROTOCOL_ERROR,
        }
    ),
    RuntimeOperation.DB_NOTIFY: frozenset(
        {
            OutcomeKind.SUCCESS,
            OutcomeKind.DELAYED,
            OutcomeKind.CANCELLED,
            OutcomeKind.LOST,
            OutcomeKind.DUPLICATE,
            OutcomeKind.REORDERED,
            OutcomeKind.CONNECTION_LOST,
            OutcomeKind.TIMEOUT,
            OutcomeKind.PROTOCOL_ERROR,
        }
    ),
}

AFTER_CALLS = frozenset(
    {
        RuntimeOperation.SPAWN_PROCESS,
        RuntimeOperation.WAIT_PROCESS,
        RuntimeOperation.CONTROL_SEND,
        RuntimeOperation.CONTROL_RECEIVE,
        RuntimeOperation.DB_CONNECT,
        RuntimeOperation.DB_ACQUIRE,
        RuntimeOperation.DB_BEGIN,
        RuntimeOperation.DB_QUERY,
        RuntimeOperation.DB_COMMIT,
        RuntimeOperation.DB_ROLLBACK,
        RuntimeOperation.DB_RELEASE,
        RuntimeOperation.DB_NOTIFY,
    }
)


class FaultStep(BaseModel):
    """Declare one call response and side effect."""

    model_config = ConfigDict(frozen=True, strict=True)

    call: RuntimeOperation
    outcome: OutcomeKind
    phase: FaultPhase = FaultPhase.BEFORE
    site: str = ""
    delay: Annotated[float, Field(ge=0)] = 0.0
    amount: Annotated[int, Field(ge=0)] = 0
    errno: Annotated[int, Field(ge=0, le=4_095)] = 0

    @model_validator(mode="after")
    def check_outcome(self) -> FaultStep:
        assert self.call in CALL_OUTCOMES
        if self.outcome not in CALL_OUTCOMES[self.call]:
            raise ValueError(f"{self.outcome} is not modeled for {self.call}")
        if self.phase is FaultPhase.AFTER and self.call not in AFTER_CALLS:
            raise ValueError(f"after-side-effect faults are not observable for {self.call}")
        assert self.delay >= 0
        assert self.amount >= 0
        assert 0 <= self.errno <= 4_095
        return self


FAULT_STEPS = tuple(
    FaultStep(call=call, outcome=outcome)
    for call, outcomes in CALL_OUTCOMES.items()
    for outcome in outcomes
)
