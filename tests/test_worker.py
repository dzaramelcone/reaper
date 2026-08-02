"""Check durable task dispatch decisions."""

import asyncio
from typing import cast

import pytest

from reaper.database import TransactionExecutor
from reaper.models import DEFAULT_TOPIC, ResultState
from reaper.postgres import ListenerActivity
from reaper.promise import Error, ReaperClient, Result, durable
from reaper.promises.models import PromiseRecord
from reaper.skeleton import LifecycleEvent, LifecycleKind, TaskReleaseReason
from reaper.tasks import TaskExecution
from reaper.tasks.models import ClaimedTask, RetryResult, SubmitCall
from reaper.worker import (
    TaskUnavailableError,
    load_task,
    report_listener_activity,
    run_claimed_task,
    settle_execution,
)


@durable(execution_timeout=1.0, version=2)
async def versioned_task() -> None:
    return None


def test_loader_accepts_only_available_durable_functions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert load_task(versioned_task.name) is versioned_task
    for name in (
        "unqualified",
        "tests.missing_worker.task",
        "tests.test_worker.missing",
        "tests.test_worker.asyncio",
    ):
        with pytest.raises(TaskUnavailableError):
            load_task(name)

    def missing_dependency(name: str) -> object:
        raise ModuleNotFoundError("dependency failed", name="worker_dependency")

    monkeypatch.setattr("reaper.worker.importlib.import_module", missing_dependency)
    with pytest.raises(ModuleNotFoundError, match="dependency failed"):
        load_task("tests.worker.task")


def test_listener_activity_emits_only_maintenance_events() -> None:
    async def check() -> None:
        events: list[LifecycleEvent] = []

        async def report(event: LifecycleEvent) -> None:
            events.append(event)

        for activity in ListenerActivity:
            await report_listener_activity(activity, report)
        assert [event.kind for event in events] == [
            LifecycleKind.LISTENER_PROBED,
            LifecycleKind.LISTENER_RECYCLED,
        ]

    asyncio.run(check())


def test_worker_maps_every_task_result_to_one_transaction_outcome() -> None:
    class RecordingExecution:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object]] = []
            self.retry_rejected = False

        async def complete(self, value: object) -> None:
            self.calls.append(("complete", value))

        async def reject(self, value: object) -> None:
            self.calls.append(("reject", value))

        async def suspend(self, value: object) -> None:
            self.calls.append(("suspend", value))

        async def retry(self, value: object) -> RetryResult:
            self.calls.append(("retry", value))
            return RetryResult(rejected=self.retry_rejected, failures=1)

    async def check() -> None:
        execution = RecordingExecution()
        typed = cast(TaskExecution, execution)
        fault = Error(type="tests.Fault", text="failed")
        assert (
            await settle_execution(typed, Result(state=ResultState.RESOLVED, value=7))
            is ResultState.RESOLVED
        )
        assert (
            await settle_execution(typed, Result(state=ResultState.REJECTED, error=fault))
            is ResultState.REJECTED
        )
        assert (
            await settle_execution(
                typed,
                Result(state=ResultState.SUSPENDED, awaited=("child",)),
            )
            is ResultState.SUSPENDED
        )
        assert (
            await settle_execution(typed, Result(state=ResultState.RETRY, error=fault))
            is ResultState.RETRY
        )
        execution.retry_rejected = True
        assert (
            await settle_execution(typed, Result(state=ResultState.RETRY, error=fault))
            is ResultState.REJECTED
        )
        assert [name for name, _value in execution.calls] == [
            "complete",
            "reject",
            "suspend",
            "retry",
            "retry",
        ]

    asyncio.run(check())


def test_version_mismatch_releases_without_consuming_an_attempt() -> None:
    async def check() -> None:
        execution = TaskExecution(
            ClaimedTask(
                id="versioned",
                root_id=None,
                function=versioned_task.name,
                version=1,
                topic=DEFAULT_TOPIC,
                input={},
                depth=0,
                max_failures=3,
                execution_timeout_ms=1_000,
                waits=(),
            ),
            cast(TransactionExecutor, object()),
        )
        events: list[LifecycleEvent] = []

        async def report(event: LifecycleEvent) -> None:
            events.append(event)

        outcome = await run_claimed_task(
            cast(ReaperClient, object()),
            execution,
            report,
        )
        assert not execution.finished
        assert [event.kind for event in events] == [
            LifecycleKind.TASK_CLAIMED,
            LifecycleKind.TASK_UNAVAILABLE,
        ]
        assert events[-1].release_reason is TaskReleaseReason.FUNCTION_UNAVAILABLE
        assert outcome is None

    asyncio.run(check())


def test_concurrent_child_submissions_coalesce_into_one_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExpectedBatch(Exception):
        pass

    async def check() -> None:
        execution = TaskExecution(
            ClaimedTask(
                id="root",
                root_id=None,
                function=versioned_task.name,
                version=versioned_task.version,
                topic=DEFAULT_TOPIC,
                input={},
                depth=0,
                max_failures=3,
                execution_timeout_ms=1_000,
                waits=(),
            ),
            cast(TransactionExecutor, object()),
        )
        batches: list[tuple[SubmitCall, ...]] = []

        async def record_batch(
            _executor: TransactionExecutor,
            params: tuple[SubmitCall, ...],
        ) -> tuple[PromiseRecord, ...]:
            batches.append(params)
            raise ExpectedBatch

        monkeypatch.setattr("reaper.tasks.submit_calls", record_batch)
        params = tuple(
            SubmitCall(function=versioned_task.name, input={"index": index}) for index in range(8)
        )
        results = await asyncio.gather(
            *(execution.submit(item) for item in params),
            return_exceptions=True,
        )
        assert batches == [params]
        assert all(isinstance(result, ExpectedBatch) for result in results)

    asyncio.run(check())
