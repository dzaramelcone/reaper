"""Check every nondeterministic call outcome is executable."""

import asyncio
import errno

import asyncpg
import pytest
from hypothesis import given, strategies

from reaper.runtime import RuntimeOperation
from tests.fault_runtime import (
    FaultResult,
    FaultRuntime,
    ScriptedClock,
)
from tests.faults import (
    CALL_OUTCOMES,
    FAULT_STEPS,
    FaultPhase,
    FaultStep,
    OutcomeKind,
)
from tests.scheduler import DeterministicScheduler


@given(step=strategies.sampled_from(FAULT_STEPS))
def test_declared_fault_steps_validate(step: FaultStep) -> None:
    assert step.outcome in CALL_OUTCOMES[step.call]


def test_every_call_has_success_and_failure_paths() -> None:
    assert set(CALL_OUTCOMES) == set(RuntimeOperation)
    assert all(OutcomeKind.SUCCESS in outcomes for outcomes in CALL_OUTCOMES.values())
    assert all(len(outcomes) > 1 for outcomes in CALL_OUTCOMES.values())
    assert {step.call for step in FAULT_STEPS} == set(RuntimeOperation)
    assert {step.outcome for step in FAULT_STEPS} == set(OutcomeKind)


def test_fault_phase_rejects_impossible_post_side_effect_injection() -> None:
    with pytest.raises(ValueError, match="after-side-effect faults are not observable"):
        FaultStep(
            call=RuntimeOperation.PIPE,
            outcome=OutcomeKind.PERMANENT_ERROR,
            phase=FaultPhase.AFTER,
        )


@pytest.mark.parametrize("step", FAULT_STEPS, ids=lambda step: f"{step.call}-{step.outcome}")
def test_every_declared_fault_is_executable(step: FaultStep) -> None:
    async def check() -> None:
        scheduler = DeterministicScheduler()
        runtime = FaultRuntime([step], scheduler)
        executing = asyncio.create_task(
            runtime.execute(
                "actor",
                step.call,
                data=b"payload",
                clock=ScriptedClock(start=10),
            )
        )
        if step.outcome is OutcomeKind.DELAYED:
            await scheduler.wait_for(operation=step.call.value, phase="delayed")
            await scheduler.release()
        outcome = (await asyncio.gather(executing, return_exceptions=True))[0]
        assert not runtime.steps
        assert isinstance(outcome, FaultResult | BaseException)
        assert not (isinstance(outcome, RuntimeError) and str(outcome).startswith("unsupported"))

    asyncio.run(check())


@pytest.mark.parametrize(
    ("outcome", "error_type", "sqlstate"),
    (
        (OutcomeKind.CONFLICT, asyncpg.UniqueViolationError, "23505"),
        (OutcomeKind.DEADLOCK, asyncpg.DeadlockDetectedError, "40P01"),
        (OutcomeKind.SERIALIZATION, asyncpg.SerializationError, "40001"),
        (OutcomeKind.LOCK_TIMEOUT, asyncpg.LockNotAvailableError, "55P03"),
        (OutcomeKind.STATEMENT_TIMEOUT, asyncpg.QueryCanceledError, "57014"),
        (OutcomeKind.PROTOCOL_ERROR, asyncpg.ProtocolViolationError, "08P01"),
    ),
)
def test_database_faults_have_real_asyncpg_types(
    outcome: OutcomeKind,
    error_type: type[asyncpg.PostgresError],
    sqlstate: str,
) -> None:
    """DST must exercise the same exception classifiers as the live driver."""

    async def check() -> None:
        runtime = FaultRuntime(
            [FaultStep(call=RuntimeOperation.DB_QUERY, outcome=outcome)],
            DeterministicScheduler(),
        )
        result = (
            await asyncio.gather(
                runtime.database("database"),
                return_exceptions=True,
            )
        )[0]
        assert isinstance(result, error_type)
        assert result.sqlstate == sqlstate

    asyncio.run(check())


@pytest.mark.parametrize(
    ("call", "outcome", "error_type", "error_number"),
    (
        (RuntimeOperation.KILL, OutcomeKind.NOT_FOUND, ProcessLookupError, errno.ESRCH),
        (RuntimeOperation.KILL, OutcomeKind.PERMISSION, PermissionError, errno.EPERM),
        (
            RuntimeOperation.WAIT_PROCESS,
            OutcomeKind.NOT_FOUND,
            ChildProcessError,
            errno.ECHILD,
        ),
        (
            RuntimeOperation.CLOSE_FD,
            OutcomeKind.ALREADY_CLOSED,
            OSError,
            errno.EBADF,
        ),
        (
            RuntimeOperation.DUP_FD,
            OutcomeKind.ALREADY_CLOSED,
            OSError,
            errno.EBADF,
        ),
        (
            RuntimeOperation.SPAWN_PROCESS,
            OutcomeKind.NOT_FOUND,
            FileNotFoundError,
            errno.ENOENT,
        ),
        (
            RuntimeOperation.SPAWN_PROCESS,
            OutcomeKind.PERMISSION,
            PermissionError,
            errno.EACCES,
        ),
    ),
)
def test_operating_system_faults_have_real_exception_shapes(
    call: RuntimeOperation,
    outcome: OutcomeKind,
    error_type: type[OSError],
    error_number: int,
) -> None:
    """DST must enter the same exception handlers as the actual syscall."""

    async def check() -> None:
        runtime = FaultRuntime(
            [FaultStep(call=call, outcome=outcome)],
            DeterministicScheduler(),
        )
        result = (
            await asyncio.gather(
                runtime.execute("operating-system", call),
                return_exceptions=True,
            )
        )[0]
        assert isinstance(result, error_type)
        assert result.errno == error_number

    asyncio.run(check())
