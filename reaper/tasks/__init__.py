"""Submit, claim, and settle executable durable tasks."""

import asyncio
import json
from types import TracebackType
from typing import Any, cast

from pydantic import JsonValue

from reaper.database import (
    ConnectionPool,
    DatabaseExecutor,
    IdempotencyConflictError,
    PromiseNotFoundError,
    TaskNotFoundError,
    TransactionExecutor,
    connection,
    decode_json,
    encode_json,
    finish_transaction,
    note_cleanup_failure,
    release_connection,
    require_transaction,
    shield_cleanup,
)
from reaper.models import DEFAULT_TOPIC, PromiseState
from reaper.promises import get_promises, promise_from_row, submit_timer
from reaper.promises.models import PromiseRecord, SubmitTimer
from reaper.tasks.models import (
    ClaimedTask,
    ClaimTask,
    CompleteTask,
    FunctionVersion,
    RejectTask,
    RetryResult,
    RetryTask,
    SubmitCall,
)
from reaper.tasks.queries import (
    CLAIM,
    LOCK_PROMISE,
    RETRY,
    SETTLE,
    SUBMIT_CALLS,
)
from reaper.waits import suspend_task
from reaper.waits.models import SuspendTask, WaitedPromise


async def submit_calls(
    executor: DatabaseExecutor,
    params: tuple[SubmitCall, ...],
) -> tuple[PromiseRecord, ...]:
    if not params:
        return ()
    ids = tuple(item.id for item in params)
    if len(set(ids)) != len(ids):
        duplicate = next(item for index, item in enumerate(ids) if item in ids[:index])
        raise IdempotencyConflictError(duplicate)
    fingerprints = tuple(item.fingerprint() for item in params)
    requested = [
        {
            **item.model_dump(mode="json"),
            "idempotency_key": fingerprint,
        }
        for item, fingerprint in zip(params, fingerprints, strict=True)
    ]
    payload = json.dumps(requested, ensure_ascii=False, separators=(",", ":"))
    rows = await executor.fetch(SUBMIT_CALLS, payload)
    promises = {promise.id: promise for promise in map(promise_from_row, rows)}
    missing_ids = tuple(promise_id for promise_id in ids if promise_id not in promises)
    if missing_ids:
        promises.update(
            (promise.id, promise) for promise in await get_promises(executor, missing_ids)
        )
    still_missing = tuple(promise_id for promise_id in ids if promise_id not in promises)
    if still_missing:
        raise PromiseNotFoundError(still_missing[0])
    ordered = tuple(promises[promise_id] for promise_id in ids)
    for item, fingerprint, promise in zip(params, fingerprints, ordered, strict=True):
        if fingerprint != promise.idempotency_key:
            raise IdempotencyConflictError(item.id)
    return ordered


async def submit_call(executor: DatabaseExecutor, params: SubmitCall) -> PromiseRecord:
    return (await submit_calls(executor, (params,)))[0]


async def claim_task(executor: TransactionExecutor, params: ClaimTask) -> ClaimedTask | None:
    require_transaction(executor)
    row = await executor.fetchrow(
        CLAIM,
        params.topic,
        [item.function for item in params.excluded],
        [item.version for item in params.excluded],
    )
    if row is None:
        return None
    values = dict(row)
    waits_json = decode_json(values["waits_json"])
    if not isinstance(waits_json, list):
        raise TypeError("claimed task waits must be a JSON array")
    waits: list[WaitedPromise] = []
    for item in waits_json:
        if not isinstance(item, dict):
            raise TypeError("claimed task wait must be a JSON object")
        item["state"] = PromiseState(str(item["state"]))
        waits.append(WaitedPromise.model_validate(item))
    return ClaimedTask.model_validate(
        {
            "id": values["id"],
            "root_id": values["root_id"],
            "function": values["function"],
            "version": values["version"],
            "topic": values["topic"],
            "input": decode_json(values["input_json"]),
            "depth": values["depth"],
            "max_failures": values["max_failures"],
            "execution_timeout_ms": values["execution_timeout_ms"],
            "waits": tuple(waits),
        }
    )


async def settle_task(
    executor: TransactionExecutor,
    promise_id: str,
    state: PromiseState,
    result: JsonValue,
    error: JsonValue,
) -> PromiseRecord:
    require_transaction(executor)
    # This separate statement establishes the promise lock before SETTLE takes
    # its READ COMMITTED snapshot.  Folding it into SETTLE permits a concurrent
    # waiter registration to retain a pre-settlement snapshot and lose its wake.
    locked = await executor.fetchval(LOCK_PROMISE, promise_id)
    if locked is None:
        raise TaskNotFoundError(promise_id)
    row = await executor.fetchrow(
        SETTLE,
        promise_id,
        state.value,
        None if result is None else encode_json(result),
        None if error is None else encode_json(error),
    )
    if row is None:
        raise TaskNotFoundError(promise_id)
    return promise_from_row(row)


async def complete_task(executor: TransactionExecutor, params: CompleteTask) -> PromiseRecord:
    return await settle_task(executor, params.id, PromiseState.RESOLVED, params.result, None)


async def reject_task(executor: TransactionExecutor, params: RejectTask) -> PromiseRecord:
    return await settle_task(executor, params.id, PromiseState.REJECTED, None, params.error)


async def retry_task(executor: TransactionExecutor, params: RetryTask) -> RetryResult:
    require_transaction(executor)
    row = await executor.fetchrow(RETRY, params.id, encode_json(params.error), params.delay_ms)
    if row is None:
        raise TaskNotFoundError(params.id)
    return RetryResult(
        rejected=cast(bool, row["rejected"]),
        failures=cast(int, row["failures"]),
    )


class TaskExecution:
    """A claimed task and the transaction that owns its row lock."""

    def __init__(self, task: ClaimedTask, connection: TransactionExecutor) -> None:
        self.task = task
        self.connection = connection
        self.finished = False
        self.operation_lock = asyncio.Lock()
        self.pending_submissions: list[tuple[SubmitCall, asyncio.Future[PromiseRecord]]] = []
        self.submission_task: asyncio.Task[None] | None = None

    def ensure_open(self) -> None:
        if self.finished:
            raise RuntimeError(f"task {self.task.id!r} already has an outcome")

    def finish(self) -> None:
        self.finished = True

    async def submit(self, params: SubmitCall) -> PromiseRecord:
        """Coalesce concurrent child work into one transaction statement."""

        self.ensure_open()
        future: asyncio.Future[PromiseRecord] = asyncio.get_running_loop().create_future()
        self.pending_submissions.append((params, future))
        if self.submission_task is None:
            self.submission_task = asyncio.create_task(self.flush_submissions())
        return await future

    async def submit_many(self, params: tuple[SubmitCall, ...]) -> tuple[PromiseRecord, ...]:
        """Create a validated child batch in one transaction statement."""

        async with self.operation_lock:
            self.ensure_open()
            return await submit_calls(self.connection, params)

    async def flush_submissions(self) -> None:
        """Collect submissions started in the same event-loop turn."""

        await asyncio.sleep(0)
        pending = tuple(self.pending_submissions)
        self.pending_submissions.clear()
        self.submission_task = None
        try:
            calls = await self.submit_many(tuple(params for params, _future in pending))
        except BaseException as error:
            for _params, future in pending:
                if not future.done():
                    future.set_exception(error)
            return
        for call, (_params, future) in zip(calls, pending, strict=True):
            if not future.done():
                future.set_result(call)

    async def cancel_submissions(self) -> None:
        """Stop a pending batch before its transaction is released."""

        task = self.submission_task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.submission_task = None
        for _params, future in self.pending_submissions:
            future.cancel()
        self.pending_submissions.clear()

    async def timer(self, params: SubmitTimer) -> PromiseRecord:
        """Create a child timer in the task's existing transaction."""

        async with self.operation_lock:
            self.ensure_open()
            return await submit_timer(self.connection, params)

    async def complete(self, result: JsonValue) -> PromiseRecord:
        async with self.operation_lock:
            self.ensure_open()
            promise = await complete_task(
                self.connection,
                CompleteTask(id=self.task.id, result=result),
            )
            self.finish()
            return promise

    async def reject(self, error: JsonValue) -> PromiseRecord:
        async with self.operation_lock:
            self.ensure_open()
            promise = await reject_task(
                self.connection,
                RejectTask(id=self.task.id, error=error),
            )
            self.finish()
            return promise

    async def retry(self, error: JsonValue, *, delay_ms: int = 0) -> RetryResult:
        async with self.operation_lock:
            self.ensure_open()
            result = await retry_task(
                self.connection,
                RetryTask(id=self.task.id, error=error, delay_ms=delay_ms),
            )
            self.finish()
            return result

    async def suspend(self, awaited_ids: tuple[str, ...]) -> None:
        async with self.operation_lock:
            self.ensure_open()
            await suspend_task(
                self.connection,
                SuspendTask(id=self.task.id, awaited_ids=awaited_ids),
            )
            self.finish()


class TaskClaim:
    """Acquire, retain, and release one task execution transaction."""

    def __init__(self, pool: ConnectionPool, params: ClaimTask) -> None:
        self.pool = pool
        self.params = params
        self.connection: TransactionExecutor | None = None
        self.transaction: Any = None
        self.execution: TaskExecution | None = None

    async def __aenter__(self) -> TaskExecution | None:
        connection = cast(TransactionExecutor, await self.pool.acquire())
        try:
            transaction = connection.transaction()
        except BaseException as error:
            try:
                await release_connection(self.pool, connection)
            except BaseException as cleanup:
                note_cleanup_failure(error, cleanup, "task transaction construction")
            raise
        try:
            await transaction.start()
            self.connection = connection
            self.transaction = transaction
            task = await claim_task(connection, self.params)
        except BaseException as error:
            try:
                await self.close_connection(connection, transaction, commit=False)
            except BaseException as cleanup:
                note_cleanup_failure(error, cleanup, "task claim")
            self.connection = None
            self.transaction = None
            raise
        if task is None:
            await self.close_connection(connection, transaction, commit=False)
            self.connection = None
            self.transaction = None
            return None
        self.execution = TaskExecution(task, connection)
        return self.execution

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        value: BaseException | None,
        trace: TracebackType | None,
    ) -> None:
        del trace
        connection = self.connection
        transaction = self.transaction
        if connection is None or transaction is None:
            return
        commit = kind is None and self.execution is not None and self.execution.finished
        try:
            if self.execution is not None:
                await shield_cleanup(self.execution.cancel_submissions())
            await self.close_connection(connection, transaction, commit=commit)
        except BaseException as cleanup:
            if value is None:
                raise
            note_cleanup_failure(value, cleanup, "task claim")
        finally:
            self.connection = None
            self.transaction = None

    async def close_connection(
        self,
        connection: TransactionExecutor,
        transaction: Any,
        *,
        commit: bool,
    ) -> None:
        """Finish and release a claim even while its caller is being cancelled."""

        async def finish() -> None:
            await finish_transaction(
                self.pool,
                connection,
                transaction,
                commit=commit,
            )

        await shield_cleanup(finish())


class Tasks:
    """High-level durable task API bound to a connection pool."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    async def submit(self, params: SubmitCall) -> PromiseRecord:
        async with connection(self.pool) as executor:
            return await submit_call(executor, params)

    async def submit_many(self, params: tuple[SubmitCall, ...]) -> tuple[PromiseRecord, ...]:
        async with connection(self.pool) as executor:
            return await submit_calls(executor, params)

    def claim(
        self,
        topic: str = DEFAULT_TOPIC,
        excluded: tuple[FunctionVersion, ...] = (),
    ) -> TaskClaim:
        return TaskClaim(self.pool, ClaimTask(topic=topic, excluded=excluded))
