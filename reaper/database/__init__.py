"""Shared executor, validation, JSON, and notification primitives."""

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from typing import Any, Protocol, cast, runtime_checkable

from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter

JSON_ADAPTER: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class DatabaseExecutor(Protocol):
    """Execute parameterized asyncpg-shaped queries."""

    def execute(self, query: str, *args: object) -> Awaitable[Any]: ...
    def fetch(self, query: str, *args: object) -> Awaitable[Any]: ...
    def fetchrow(self, query: str, *args: object) -> Awaitable[Any]: ...
    def fetchval(self, query: str, *args: object) -> Awaitable[Any]: ...


class TransactionExecutor(DatabaseExecutor, Protocol):
    """A checked-out connection with an explicit transaction."""

    def is_in_transaction(self) -> bool: ...
    def transaction(self) -> Any: ...


class ConnectionPool(Protocol):
    """Acquire and release asyncpg-shaped connections."""

    def acquire(self) -> Awaitable[Any]: ...
    def release(self, connection: Any) -> Awaitable[Any]: ...


@runtime_checkable
class ClosableConnection(Protocol):
    """Connection lifecycle used to finalize a transport-level close."""

    def is_closed(self) -> bool: ...
    def terminate(self) -> None: ...


class QueryModel(BaseModel):
    """Reject unknown or implicitly coerced query data."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ReaperSQLError(RuntimeError):
    """Base error for the Reaper SQL boundary."""


class TransactionRequiredError(ReaperSQLError):
    def __init__(self) -> None:
        super().__init__("this operation requires an explicit transaction connection")


class IdempotencyConflictError(ReaperSQLError):
    def __init__(self, promise_id: str) -> None:
        self.promise_id = promise_id
        super().__init__(f"promise id {promise_id!r} was reused for different work")


class PromiseNotFoundError(ReaperSQLError):
    def __init__(self, promise_id: str) -> None:
        self.promise_id = promise_id
        super().__init__(f"promise {promise_id!r} does not exist")


class TaskNotFoundError(ReaperSQLError):
    def __init__(self, promise_id: str) -> None:
        self.promise_id = promise_id
        super().__init__(f"active task {promise_id!r} does not exist")


class CrossGraphWaitError(ReaperSQLError):
    def __init__(self, waiter_id: str, awaited_ids: tuple[str, ...]) -> None:
        self.waiter_id = waiter_id
        self.awaited_ids = awaited_ids
        super().__init__(
            f"task {waiter_id!r} cannot await promises outside its root graph: {awaited_ids!r}"
        )


def encode_json(value: JsonValue) -> str:
    return JSON_ADAPTER.dump_json(value).decode()


def decode_json(value: object) -> JsonValue:
    if not isinstance(value, str):
        raise TypeError("PostgreSQL JSON projection must be text")
    return JSON_ADAPTER.validate_json(value, strict=True)


def decode_optional_json(value: object) -> JsonValue | None:
    return None if value is None else decode_json(value)


def model_fingerprint(model: BaseModel, *, exclude: frozenset[str] = frozenset()) -> str:
    """Hash one canonical validated request for persistent idempotency checks."""

    document = {
        "model": f"{type(model).__module__}.{type(model).__qualname__}",
        "value": model.model_dump(mode="json", exclude=set(exclude)),
    }
    encoded = json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def task_channel(topic: str) -> str:
    digest = hashlib.md5(topic.encode(), usedforsecurity=False).hexdigest()
    return f"reaper_task_{digest}"


def require_transaction(executor: TransactionExecutor) -> None:
    if not executor.is_in_transaction():
        raise TransactionRequiredError()


def note_cleanup_failure(
    failure: BaseException,
    cleanup: BaseException,
    operation: str,
) -> None:
    """Retain a cleanup fault without replacing the operation's primary failure."""

    kind = f"{type(cleanup).__module__}.{type(cleanup).__qualname__}"
    failure.add_note(f"{operation} cleanup also failed with {kind}: {cleanup}")


async def shield_cleanup(operation: Awaitable[Any]) -> Any:
    """Finish cleanup under cancellation while preserving cancellation as primary."""

    cleanup = asyncio.ensure_future(operation)
    try:
        return await asyncio.shield(cleanup)
    except asyncio.CancelledError as cancellation:
        try:
            await cleanup
        except BaseException as failure:
            note_cleanup_failure(cancellation, failure, "cancelled operation")
        raise


async def release_connection(pool: ConnectionPool, executor: object) -> None:
    """Return a connection even when its caller is concurrently cancelled."""

    try:
        if isinstance(executor, ClosableConnection) and executor.is_closed():
            # asyncpg's release fast-path assumes its close callback has already
            # returned the holder.  A transport loss can make is_closed() true
            # just before that callback runs; terminate() completes the callback
            # synchronously and is idempotent for an already-closed connection.
            executor.terminate()
    except BaseException as failure:
        try:
            await shield_cleanup(pool.release(executor))
        except BaseException as cleanup:
            note_cleanup_failure(failure, cleanup, "connection release")
        raise
    await shield_cleanup(pool.release(executor))


async def finish_transaction(
    pool: ConnectionPool,
    executor: TransactionExecutor,
    active: Any,
    *,
    commit: bool,
) -> None:
    """Finish a transaction while retaining its failure across release cleanup."""

    try:
        if commit:
            await active.commit()
        else:
            try:
                in_transaction = executor.is_in_transaction()
            except Exception:
                in_transaction = False
            if in_transaction:
                await active.rollback()
    except BaseException as failure:
        try:
            await release_connection(pool, executor)
        except BaseException as cleanup:
            note_cleanup_failure(failure, cleanup, "transaction release")
        raise
    await release_connection(pool, executor)


@asynccontextmanager
async def connection(pool: ConnectionPool) -> AsyncIterator[DatabaseExecutor]:
    executor = cast(DatabaseExecutor, await pool.acquire())
    failure: BaseException | None = None
    try:
        yield executor
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            await release_connection(pool, executor)
        except BaseException as cleanup:
            if failure is None:
                raise
            note_cleanup_failure(failure, cleanup, "connection release")
