"""Run row-locked durable tasks inside skeletons."""

import asyncio
import importlib
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import cast

from pydantic import JsonValue, PostgresDsn

from reaper.log import write
from reaper.models import ResultState
from reaper.postgres import ListenerActivity, ListenerWake
from reaper.promise import (
    Context,
    DurableFunction,
    Reaper,
    Result,
)
from reaper.settings import (
    DEFAULT_GC_RATE,
    DEFAULT_LISTENER_PROBE_RATE,
    DEFAULT_LISTENER_RECYCLE_RATE,
    DEFAULT_RETENTION_MS,
    ReaperSettings,
)
from reaper.skeleton import (
    LifecycleEvent,
    LifecycleKind,
    TaskReleaseReason,
)
from reaper.tasks import TaskExecution
from reaper.tasks.models import FunctionVersion

log = logging.getLogger(__name__)

LifecycleReporter = Callable[[LifecycleEvent], Awaitable[None]]


class TaskUnavailableError(RuntimeError):
    """Report durable work that this worker deployment cannot load."""

    def __init__(self, function: str) -> None:
        self.function = function
        super().__init__(f"durable function {function!r} is unavailable")


async def ignore_lifecycle(event: LifecycleEvent) -> None:
    """Discard lifecycle telemetry when a caller does not provide a driver."""

    del event


async def report_listener_activity(
    activity: ListenerActivity,
    lifecycle: LifecycleReporter,
) -> None:
    """Translate connection maintenance into a lifecycle transition."""

    match activity:
        case ListenerActivity.PROBED:
            await lifecycle(LifecycleEvent(kind=LifecycleKind.LISTENER_PROBED))
        case ListenerActivity.RECYCLED:
            await lifecycle(LifecycleEvent(kind=LifecycleKind.LISTENER_RECYCLED))
        case ListenerActivity.NONE:
            return


def load_task(name: str) -> DurableFunction[..., object]:
    """Load one module-level durable task."""

    module_name, separator, task_name = name.rpartition(".")
    if not separator:
        raise TaskUnavailableError(name)
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as error:
        if error.name is not None and (
            module_name == error.name or module_name.startswith(f"{error.name}.")
        ):
            raise TaskUnavailableError(name) from error
        raise
    try:
        value = getattr(module, task_name)
    except AttributeError as error:
        raise TaskUnavailableError(name) from error
    if not isinstance(value, DurableFunction):
        raise TaskUnavailableError(name)
    return value


async def settle_execution(
    execution: TaskExecution,
    result: Result,
) -> ResultState:
    """Apply one task outcome inside its claim transaction."""

    error = result.error
    error_data = cast(
        JsonValue,
        error.model_dump(mode="json") if error is not None else {},
    )
    match result.state:
        case ResultState.RESOLVED:
            await execution.complete(result.value)
            return ResultState.RESOLVED
        case ResultState.REJECTED:
            await execution.reject(error_data)
            return ResultState.REJECTED
        case ResultState.SUSPENDED:
            await execution.suspend(result.awaited)
            return ResultState.SUSPENDED
        case ResultState.RETRY:
            retried = await execution.retry(error_data)
            return ResultState.REJECTED if retried.rejected else ResultState.RETRY
        case _:
            raise RuntimeError(f"worker gave task state {result.state}")


async def run_claimed_task(
    client: Reaper,
    execution: TaskExecution,
    lifecycle: LifecycleReporter = ignore_lifecycle,
) -> ResultState | None:
    """Execute and settle one row-locked task."""

    await lifecycle(
        LifecycleEvent(
            kind=LifecycleKind.TASK_CLAIMED,
            task_id=execution.task.id,
            version=execution.task.version,
        )
    )
    try:
        task = load_task(execution.task.function)
    except TaskUnavailableError:
        task = None
    if task is None or task.version != execution.task.version:
        write(
            log,
            logging.INFO,
            "task version mismatch",
            id=execution.task.id,
            queued=execution.task.version,
            loaded=task.version if task is not None else None,
        )
        await lifecycle(
            LifecycleEvent(
                kind=LifecycleKind.TASK_UNAVAILABLE,
                task_id=execution.task.id,
                version=execution.task.version,
                release_reason=TaskReleaseReason.FUNCTION_UNAVAILABLE,
            )
        )
        return None
    context = Context(
        store=client,
        execution=execution,
        task_id=execution.task.id,
        preload=execution.task.waits,
        depth=execution.task.depth,
    )
    if not isinstance(execution.task.input, Mapping):
        raise TypeError("durable task input must be a JSON object")
    result = await task.execute(execution.task.input, context, wire=True)
    await lifecycle(
        LifecycleEvent(
            kind=LifecycleKind.TASK_OUTCOME,
            task_id=execution.task.id,
            version=execution.task.version,
            outcome=result.state,
        )
    )
    return await settle_execution(execution, result)


async def poll_tasks(
    postgres_dsn: str,
    topic: str,
    poll_rate: float,
    ready: Callable[[], None] = lambda: None,
    listener_probe_rate: float = DEFAULT_LISTENER_PROBE_RATE,
    listener_recycle_rate: float = DEFAULT_LISTENER_RECYCLE_RATE,
    retention_ms: int = DEFAULT_RETENTION_MS,
    lifecycle: LifecycleReporter = ignore_lifecycle,
) -> None:
    """Listen, poll, and run tasks for one topic."""

    if not topic:
        raise ValueError("task polling requires a topic")
    async with Reaper(
        ReaperSettings(
            postgres_dsn=PostgresDsn(postgres_dsn),
            retention_ms=retention_ms,
        )
    ) as client:
        await lifecycle(LifecycleEvent(kind=LifecycleKind.LINK_OPENED))
        listener = await client.listen(
            topic,
            recycle_rate=listener_recycle_rate,
            probe_rate=listener_probe_rate,
        )
        await lifecycle(LifecycleEvent(kind=LifecycleKind.LISTENER_OPENED))
        await lifecycle(LifecycleEvent(kind=LifecycleKind.READY))
        excluded: set[tuple[str, int]] = set()
        try:
            while True:
                await report_listener_activity(await listener.arm(), lifecycle)
                await lifecycle(LifecycleEvent(kind=LifecycleKind.POLL))
                capabilities = tuple(
                    FunctionVersion(function=function, version=version)
                    for function, version in sorted(excluded)
                )
                async with client.get_store().tasks.claim(
                    topic,
                    capabilities,
                ) as execution:
                    ready()
                    if execution is not None:
                        task_id = execution.task.id
                        version = execution.task.version
                        outcome = await run_claimed_task(client, execution, lifecycle)
                        committed = execution.finished
                        if not committed:
                            excluded.add((execution.task.function, execution.task.version))
                if execution is not None:
                    await lifecycle(
                        LifecycleEvent(
                            kind=(
                                LifecycleKind.TASK_COMMITTED
                                if committed
                                else LifecycleKind.TASK_RELEASED
                            ),
                            task_id=task_id,
                            version=version,
                            outcome=outcome,
                            release_reason=(
                                None if committed else TaskReleaseReason.FUNCTION_UNAVAILABLE
                            ),
                        )
                    )
                    continue
                wake, activity = await listener.wait(poll_rate)
                await lifecycle(
                    LifecycleEvent(
                        kind=(
                            LifecycleKind.LISTENER_NOTIFIED
                            if wake is ListenerWake.NOTIFIED
                            else LifecycleKind.FALLBACK_POLL
                        )
                    )
                )
                await report_listener_activity(activity, lifecycle)
        finally:
            await listener.close()
            await lifecycle(LifecycleEvent(kind=LifecycleKind.LISTENER_CLOSED))


async def poll_maintenance(
    postgres_dsn: str,
    maintenance_rate: float,
    ready: Callable[[], None] = lambda: None,
    gc_rate: float = DEFAULT_GC_RATE,
    retention_ms: int = DEFAULT_RETENTION_MS,
    lifecycle: LifecycleReporter = ignore_lifecycle,
) -> None:
    """Advance deadlines and collect expired roots on one cadence."""

    async with Reaper(
        ReaperSettings(
            postgres_dsn=PostgresDsn(postgres_dsn),
            retention_ms=retention_ms,
        )
    ) as client:
        await lifecycle(LifecycleEvent(kind=LifecycleKind.LINK_OPENED))
        await lifecycle(LifecycleEvent(kind=LifecycleKind.READY))
        next_gc = time.monotonic()
        while True:
            await client.process_due()
            await lifecycle(LifecycleEvent(kind=LifecycleKind.MAINTENANCE_POLL))
            now = time.monotonic()
            if now >= next_gc:
                removed = 0
                for _ in range(10):
                    batch = await client.gc()
                    removed += batch
                    if batch < 10_000:
                        break
                if removed:
                    await lifecycle(
                        LifecycleEvent(
                            kind=LifecycleKind.GC_FINISHED,
                            count=removed,
                        )
                    )
                next_gc = now + gc_rate
            ready()
            await asyncio.sleep(maintenance_rate)
