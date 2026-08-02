"""Force broad skeleton behavior in every generated campaign."""

import asyncio
import os
from enum import StrEnum

from hypothesis import given, settings, strategies

from reaper.control import ControlEvent, EventKind, SkeletonID, SkeletonState
from reaper.pool import RemoteWorkerError, SkeletonPool
from tests.dst import ReaperModel
from tests.test_hypothesis_dst import settle_fairly
from tests.workers import add_one, fail_async, fail_sync, twice

EXAMPLES = int(os.environ.get("REAPER_BEHAVIOR_EXAMPLES", "100"))
RUNTIME_EXAMPLES = int(os.environ.get("REAPER_RUNTIME_EXAMPLES", "10"))


class SkeletonBehavior(StrEnum):
    READY = "ready"
    SUCCESS = "success"
    TASK_ERROR = "task_error"
    CRASH_STARTING = "crash_starting"
    CRASH_IDLE = "crash_idle"
    CRASH_RUNNING = "crash_running"
    SPAWN_ERROR = "spawn_error"
    SEND_ERROR = "send_error"
    STALE_EOF = "stale_eof"
    STALE_RESULT = "stale_result"
    WRONG_RESULT = "wrong_result"
    DUP_RESULT = "dup_result"
    HANG = "hang"


class WorkerOutcome(StrEnum):
    SYNC_OK = "sync_ok"
    SYNC_ERROR = "sync_error"
    ASYNC_OK = "async_ok"
    ASYNC_ERROR = "async_error"
    BASH_OK = "bash_ok"
    BASH_ERROR = "bash_error"


def ready_all(system: ReaperModel) -> None:
    """Deliver every pending ready event."""

    for slot in tuple(system.slots.values()):
        if slot.state is SkeletonState.STARTING:
            system.apply(ControlEvent(kind=EventKind.READY, identity=slot.identity))


def start_job(system: ReaperModel, job: str) -> tuple[SkeletonID, str]:
    """Start one named job on a live skeleton."""

    ready_all(system)
    system.apply(ControlEvent(kind=EventKind.SUBMIT, job=job))
    running = [slot for slot in system.slots.values() if slot.state is SkeletonState.RUNNING]
    assert len(running) == 1
    return running[0].identity, job


def apply_behavior(system: ReaperModel, behavior: SkeletonBehavior, index: int) -> None:
    """Apply one state-aware skeleton behavior."""

    job = f"behavior-{index}"
    match behavior:
        case SkeletonBehavior.READY:
            ready_all(system)
        case SkeletonBehavior.SUCCESS | SkeletonBehavior.TASK_ERROR:
            identity, running_job = start_job(system, job)
            system.apply(
                ControlEvent(
                    kind=EventKind.RESULT,
                    identity=identity,
                    job=running_job,
                    ok=behavior is SkeletonBehavior.SUCCESS,
                )
            )
        case SkeletonBehavior.CRASH_STARTING:
            ready_all(system)
            idle = next(iter(system.slots.values()))
            system.apply(ControlEvent(kind=EventKind.EOF, identity=idle.identity))
            starting = next(
                slot for slot in system.slots.values() if slot.state is SkeletonState.STARTING
            )
            system.apply(ControlEvent(kind=EventKind.EOF, identity=starting.identity))
        case SkeletonBehavior.CRASH_IDLE:
            ready_all(system)
            idle = next(iter(system.slots.values()))
            system.apply(ControlEvent(kind=EventKind.EOF, identity=idle.identity))
        case SkeletonBehavior.CRASH_RUNNING | SkeletonBehavior.SEND_ERROR:
            identity, _ = start_job(system, job)
            kind = (
                EventKind.EOF
                if behavior is SkeletonBehavior.CRASH_RUNNING
                else EventKind.SEND_FAILED
            )
            system.apply(ControlEvent(kind=kind, identity=identity))
        case SkeletonBehavior.SPAWN_ERROR:
            ready_all(system)
            idle = next(iter(system.slots.values()))
            system.fail_next_spawn()
            system.apply(ControlEvent(kind=EventKind.EOF, identity=idle.identity))
        case SkeletonBehavior.STALE_EOF:
            ready_all(system)
            idle = next(iter(system.slots.values()))
            system.apply(ControlEvent(kind=EventKind.EOF, identity=idle.identity))
            before = system.view()
            system.apply(ControlEvent(kind=EventKind.EOF, identity=idle.identity))
            assert system.view() == before
        case SkeletonBehavior.STALE_RESULT:
            identity, running_job = start_job(system, job)
            system.apply(ControlEvent(kind=EventKind.EOF, identity=identity))
            before = system.view()
            system.apply(
                ControlEvent(
                    kind=EventKind.RESULT,
                    identity=identity,
                    job=running_job,
                )
            )
            assert system.view() == before
        case SkeletonBehavior.WRONG_RESULT:
            identity, _ = start_job(system, job)
            before = system.view()
            system.apply(
                ControlEvent(
                    kind=EventKind.RESULT,
                    identity=identity,
                    job="wrong-job",
                )
            )
            assert system.view() == before
        case SkeletonBehavior.DUP_RESULT:
            identity, running_job = start_job(system, job)
            event = ControlEvent(
                kind=EventKind.RESULT,
                identity=identity,
                job=running_job,
            )
            system.apply(event)
            before = system.view()
            system.apply(event)
            assert system.view() == before
        case SkeletonBehavior.HANG:
            start_job(system, job)


@settings(max_examples=EXAMPLES, deadline=None, print_blob=True)
@given(
    target=strategies.integers(min_value=1, max_value=12),
    order=strategies.permutations(tuple(SkeletonBehavior)),
)
def test_every_skeleton_behavior_recovers_fairly(
    target: int,
    order: list[SkeletonBehavior],
) -> None:
    system = ReaperModel(target)
    covered: set[SkeletonBehavior] = set()
    for index, behavior in enumerate(order):
        apply_behavior(system, behavior, index)
        covered.add(behavior)
        settle_fairly(system)
    assert covered == set(SkeletonBehavior)
    system.apply(ControlEvent(kind=EventKind.CLOSE))
    system.apply(ControlEvent(kind=EventKind.DEADLINE))
    settle_fairly(system)
    assert not system.slots


@settings(max_examples=RUNTIME_EXAMPLES, deadline=None, print_blob=True)
@given(order=strategies.permutations(tuple(WorkerOutcome)))
def test_every_worker_kind_covers_success_and_error(
    order: list[WorkerOutcome],
) -> None:
    async def check() -> None:
        async with SkeletonPool(2, beat_rate=0.02) as reaper:
            covered: set[WorkerOutcome] = set()
            for outcome in order:
                match outcome:
                    case WorkerOutcome.SYNC_OK:
                        assert await reaper.run_sync(twice, 4) == 8
                    case WorkerOutcome.SYNC_ERROR:
                        sync_result = await asyncio.gather(
                            reaper.run_sync(fail_sync),
                            return_exceptions=True,
                        )
                        assert isinstance(sync_result[0], RemoteWorkerError)
                    case WorkerOutcome.ASYNC_OK:
                        assert await reaper.run_async(add_one, 4) == 5
                    case WorkerOutcome.ASYNC_ERROR:
                        async_result = await asyncio.gather(
                            reaper.run_async(fail_async),
                            return_exceptions=True,
                        )
                        assert isinstance(async_result[0], RemoteWorkerError)
                    case WorkerOutcome.BASH_OK:
                        assert await reaper.run_bash("printf ready") == "ready"
                    case WorkerOutcome.BASH_ERROR:
                        bash_result = await asyncio.gather(
                            reaper.run_bash("exit 7"),
                            return_exceptions=True,
                        )
                        assert isinstance(bash_result[0], RemoteWorkerError)
                covered.add(outcome)
            assert covered == set(WorkerOutcome)
            assert len(reaper.status()) == reaper.target
            assert all(row[1] is SkeletonState.IDLE for row in reaper.status())

    asyncio.run(check())
