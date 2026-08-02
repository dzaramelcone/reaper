"""Execute scripted nondeterministic outcomes in deterministic tests."""

import asyncio
import errno
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from reaper.runtime import RuntimeCheckpoint, RuntimeOperation
from tests.faults import FaultPhase, FaultStep, OutcomeKind
from tests.scheduler import DeterministicScheduler

DATABASE_CALLS = frozenset(
    {
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
MODEL_CALLS = frozenset({RuntimeOperation.CLOCK, RuntimeOperation.SLEEP})


def database_exception(step: FaultStep) -> BaseException | None:
    """Create the concrete asyncpg error represented by one database step."""

    if step.call not in DATABASE_CALLS:
        return None
    match step.outcome:
        case OutcomeKind.CONNECTION_LOST:
            return asyncpg.ConnectionDoesNotExistError("scripted connection loss")
        case OutcomeKind.AUTH_FAILED:
            return asyncpg.InvalidPasswordError("scripted authentication failure")
        case OutcomeKind.RESOURCE_LIMIT:
            return asyncpg.TooManyConnectionsError("scripted connection exhaustion")
        case OutcomeKind.CONFLICT:
            return asyncpg.UniqueViolationError("scripted transaction conflict")
        case OutcomeKind.DEADLOCK:
            return asyncpg.DeadlockDetectedError("scripted deadlock")
        case OutcomeKind.SERIALIZATION:
            return asyncpg.SerializationError("scripted serialization failure")
        case OutcomeKind.LOCK_TIMEOUT:
            return asyncpg.LockNotAvailableError("scripted lock timeout")
        case OutcomeKind.STATEMENT_TIMEOUT:
            return asyncpg.QueryCanceledError("scripted statement timeout")
        case OutcomeKind.PROTOCOL_ERROR:
            return asyncpg.ProtocolViolationError("scripted protocol error")
        case _:
            return None


def operating_system_exception(step: FaultStep) -> OSError | None:
    """Create the concrete syscall error represented by one OS step."""

    if step.call is RuntimeOperation.SPAWN_PROCESS:
        if step.outcome is OutcomeKind.NOT_FOUND:
            return FileNotFoundError(errno.ENOENT, "scripted executable not found")
        if step.outcome is OutcomeKind.PERMISSION:
            return PermissionError(errno.EACCES, "scripted executable permission failure")
    if step.call is RuntimeOperation.KILL:
        if step.outcome is OutcomeKind.NOT_FOUND:
            return ProcessLookupError(errno.ESRCH, "scripted process not found")
        if step.outcome is OutcomeKind.PERMISSION:
            return PermissionError(errno.EPERM, "scripted signal permission failure")
    if step.call is RuntimeOperation.WAIT_PROCESS and step.outcome is OutcomeKind.NOT_FOUND:
        return ChildProcessError(errno.ECHILD, "scripted child not found")
    if step.call is RuntimeOperation.HEARTBEAT_OPEN:
        if step.outcome is OutcomeKind.NOT_FOUND:
            return FileNotFoundError(errno.ENOENT, "scripted heartbeat path not found")
        if step.outcome is OutcomeKind.PERMISSION:
            return PermissionError(errno.EACCES, "scripted heartbeat path permission failure")
    if (
        step.call in {RuntimeOperation.CLOSE_FD, RuntimeOperation.DUP_FD}
        and step.outcome is OutcomeKind.ALREADY_CLOSED
    ):
        return OSError(errno.EBADF, "scripted descriptor already closed")
    if (
        step.call
        in {
            RuntimeOperation.HEARTBEAT_READ,
            RuntimeOperation.HEARTBEAT_WRITE,
        }
        and step.outcome is OutcomeKind.ALREADY_CLOSED
    ):
        return OSError(errno.EBADF, "scripted heartbeat descriptor already closed")
    if step.call is RuntimeOperation.HEARTBEAT_WRITE and step.outcome is OutcomeKind.RESOURCE_LIMIT:
        return OSError(errno.ENOSPC, "scripted heartbeat storage exhaustion")
    return None


class ScriptedClock:
    """Give models a manually advanced monotonic clock."""

    def __init__(self, start: int = 0) -> None:
        if start < 0:
            raise ValueError("clock cannot start below zero")
        self.value = start

    def now(self) -> int:
        """Return the current logical time."""

        return self.value

    def advance(self, amount: int) -> int:
        """Move logical time forward."""

        if amount < 0:
            raise ValueError("clock cannot move backward")
        self.value += amount
        return self.value


class FaultResult(BaseModel):
    """Describe the observable result of one injected call."""

    model_config = ConfigDict(frozen=True, strict=True)

    call: RuntimeOperation
    outcome: OutcomeKind
    amount: Annotated[int, Field(ge=0)] = 0
    value: int | bool | None = None
    data: bytes = b""
    deliveries: Annotated[int, Field(ge=0)] = 0


class FaultRuntime:
    """Consume `FaultStep` values at real asynchronous call boundaries."""

    def __init__(
        self,
        steps: Sequence[FaultStep],
        scheduler: DeterministicScheduler,
    ) -> None:
        self.steps = deque(steps)
        self.scheduler = scheduler

    def take(
        self,
        call: RuntimeOperation,
        site: str = "",
        phase: FaultPhase | None = None,
    ) -> FaultStep:
        """Consume a matching scripted outcome or return success."""

        if (
            self.steps
            and self.steps[0].call is call
            and (not self.steps[0].site or self.steps[0].site == site)
            and (phase is None or self.steps[0].phase is phase)
        ):
            return self.steps.popleft()
        return FaultStep(call=call, outcome=OutcomeKind.SUCCESS)

    async def execute(
        self,
        actor: str,
        call: RuntimeOperation,
        *,
        data: bytes = b"",
        clock: ScriptedClock | None = None,
        site: str = "",
    ) -> FaultResult:
        """Execute every catalog outcome through one typed adapter boundary."""

        step = self.take(call, site)
        return await self.apply(step, actor, data=data, clock=clock)

    async def around[ValueT](
        self,
        actor: str,
        call: RuntimeOperation,
        operation: Callable[[], Awaitable[ValueT]],
        *,
        site: str = "",
    ) -> ValueT:
        """Inject before or after a real asynchronous side effect."""

        step = self.take(call, site)
        if step.phase is FaultPhase.BEFORE:
            await self.apply(step, actor)
        result = await operation()
        if step.phase is FaultPhase.AFTER:
            await self.apply(step, actor)
        return result

    async def apply(
        self,
        step: FaultStep,
        actor: str,
        *,
        data: bytes = b"",
        clock: ScriptedClock | None = None,
    ) -> FaultResult:
        """Apply one already-selected fault step."""

        call = step.call
        if step.outcome is OutcomeKind.DELAYED:
            await self.scheduler.pause(actor, call.value, "delayed")
            return self.success_result(call, step.outcome, data, clock)

        database_failure = database_exception(step)
        if database_failure is not None:
            raise database_failure
        operating_system_failure = operating_system_exception(step)
        if operating_system_failure is not None:
            raise operating_system_failure

        match step.outcome:
            case OutcomeKind.SUCCESS:
                return self.success_result(call, step.outcome, data, clock)
            case OutcomeKind.CLOCK_JUMP:
                if clock is None:
                    raise ValueError("clock_jump needs a scripted clock")
                return FaultResult(
                    call=call,
                    outcome=step.outcome,
                    value=clock.advance(step.amount),
                )
            case OutcomeKind.SHORT:
                amount = min(len(data), max(1, step.amount)) if data else 0
                return FaultResult(
                    call=call,
                    outcome=step.outcome,
                    amount=amount,
                    data=(
                        data[:amount]
                        if call
                        in {RuntimeOperation.CONTROL_RECEIVE, RuntimeOperation.HEARTBEAT_READ}
                        else b""
                    ),
                )
            case OutcomeKind.EOF if call in {
                RuntimeOperation.CONTROL_RECEIVE,
                RuntimeOperation.HEARTBEAT_READ,
            }:
                return FaultResult(call=call, outcome=step.outcome)
            case OutcomeKind.EOF:
                raise BrokenPipeError(errno.EPIPE, "scripted end of stream")
            case OutcomeKind.NOT_FOUND | OutcomeKind.PERMISSION:
                return FaultResult(call=call, outcome=step.outcome, value=False)
            case OutcomeKind.ALREADY_CLOSED:
                return FaultResult(call=call, outcome=step.outcome, value=False)
            case OutcomeKind.PROCESS_EXIT:
                return FaultResult(
                    call=call,
                    outcome=step.outcome,
                    value=step.amount,
                )
            case OutcomeKind.LOST:
                return FaultResult(call=call, outcome=step.outcome, deliveries=0)
            case OutcomeKind.DUPLICATE:
                return FaultResult(call=call, outcome=step.outcome, deliveries=2)
            case OutcomeKind.REORDERED:
                return FaultResult(call=call, outcome=step.outcome, deliveries=1)
            case OutcomeKind.CANCELLED:
                raise asyncio.CancelledError
            case OutcomeKind.INTERRUPTED:
                raise InterruptedError(step.errno or errno.EINTR, "scripted interruption")
            case OutcomeKind.PERMANENT_ERROR:
                raise OSError(step.errno or errno.EIO, "scripted permanent failure")
            case OutcomeKind.RESOURCE_LIMIT:
                raise OSError(step.errno or errno.EMFILE, "scripted resource limit")
            case OutcomeKind.CONNECTION_LOST:
                raise ConnectionError("scripted connection loss")
            case OutcomeKind.TIMEOUT:
                raise TimeoutError("scripted timeout")
            case OutcomeKind.AUTH_FAILED:
                raise PermissionError("scripted authentication failure")
            case _:
                raise RuntimeError(f"unsupported {call.value} fault {step.outcome.value}")

    def success_result(
        self,
        call: RuntimeOperation,
        outcome: OutcomeKind,
        data: bytes,
        clock: ScriptedClock | None,
    ) -> FaultResult:
        """Construct a call-shaped successful result."""

        if call is RuntimeOperation.CLOCK:
            if clock is None:
                raise ValueError("clock call needs a scripted clock")
            return FaultResult(call=call, outcome=outcome, value=clock.now())
        if call in {
            RuntimeOperation.CONFIG_WRITE,
            RuntimeOperation.CONTROL_SEND,
            RuntimeOperation.CONTROL_RECEIVE,
        }:
            return FaultResult(
                call=call,
                outcome=outcome,
                amount=len(data),
                data=data if call is RuntimeOperation.CONTROL_RECEIVE else b"",
            )
        if call in {RuntimeOperation.HEARTBEAT_READ, RuntimeOperation.HEARTBEAT_WRITE}:
            return FaultResult(call=call, outcome=outcome, value=True)
        if call is RuntimeOperation.DB_NOTIFY:
            return FaultResult(call=call, outcome=outcome, deliveries=1)
        return FaultResult(call=call, outcome=outcome, value=True)

    async def send(self, actor: str, data: bytes) -> int:
        """Apply one scripted transport outcome and return bytes written."""
        return (await self.execute(actor, RuntimeOperation.CONTROL_SEND, data=data)).amount

    async def sleep(self, actor: str) -> None:
        """Apply one scripted scheduling outcome."""

        await self.execute(actor, RuntimeOperation.SLEEP)

    async def clock(self, actor: str, clock: ScriptedClock) -> int:
        """Read or jump a scripted logical clock."""

        result = await self.execute(actor, RuntimeOperation.CLOCK, clock=clock)
        assert isinstance(result.value, int)
        return result.value

    async def database(
        self, actor: str, call: RuntimeOperation = RuntimeOperation.DB_QUERY
    ) -> None:
        """Execute one database fault at a schedulable query boundary."""

        if call not in {
            RuntimeOperation.DB_CONNECT,
            RuntimeOperation.DB_ACQUIRE,
            RuntimeOperation.DB_BEGIN,
            RuntimeOperation.DB_QUERY,
            RuntimeOperation.DB_COMMIT,
            RuntimeOperation.DB_ROLLBACK,
            RuntimeOperation.DB_RELEASE,
        }:
            raise ValueError("database adapter needs a database call kind")
        await self.execute(actor, call)

    async def heartbeat(self, actor: str) -> bool:
        """Execute one schedulable heartbeat outcome."""

        result = await self.execute(actor, RuntimeOperation.HEARTBEAT_READ, data=bytes(8))
        if result.outcome in {OutcomeKind.SHORT, OutcomeKind.EOF}:
            return len(result.data) == 8
        return bool(result.value)


class FaultHooks:
    """Inject catalog outcomes at named production runtime checkpoints."""

    def __init__(self, runtime: FaultRuntime) -> None:
        self.runtime = runtime

    async def checkpoint(
        self,
        operation: RuntimeCheckpoint,
        **details: object,
    ) -> object | None:
        """Execute a fault when this checkpoint represents a catalog call."""

        if not isinstance(operation, RuntimeOperation):
            return None
        call = operation
        data = details.get("data", b"")
        if not isinstance(data, bytes):
            raise TypeError("fault checkpoint data must be bytes")
        phase = FaultPhase(str(details.get("phase", FaultPhase.BEFORE)))
        step = self.runtime.take(
            call,
            str(details.get("purpose", "")),
            phase,
        )
        result = await self.runtime.apply(
            step,
            str(details.get("actor", "runtime")),
            data=data,
        )
        if operation is RuntimeOperation.HEARTBEAT_READ and result.outcome in {
            OutcomeKind.SHORT,
            OutcomeKind.EOF,
        }:
            return result.data
        if operation is RuntimeOperation.HEARTBEAT_WRITE and result.outcome is OutcomeKind.SHORT:
            return result.amount
        if operation is RuntimeOperation.CONFIG_WRITE and result.outcome is OutcomeKind.SHORT:
            return result.amount
        if operation is RuntimeOperation.CONTROL_RECEIVE and result.outcome is OutcomeKind.SHORT:
            return result.amount
        if operation is RuntimeOperation.CONTROL_RECEIVE and result.outcome is OutcomeKind.EOF:
            return 0
        if (
            operation is RuntimeOperation.WAIT_PROCESS
            and result.outcome is OutcomeKind.PROCESS_EXIT
        ):
            return result.value
        if operation == "db_notify" and result.outcome in {
            OutcomeKind.LOST,
            OutcomeKind.DUPLICATE,
            OutcomeKind.REORDERED,
        }:
            return result.deliveries
        return None
