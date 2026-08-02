"""Inject deterministic failures around real reaper.sql database calls."""

from collections.abc import Awaitable, Callable
from typing import Any

from reaper.runtime import RuntimeOperation
from tests.fault_runtime import FaultRuntime
from tests.faults import FaultPhase


class FaultTransaction:
    """Wrap one real transaction with phase-aware fault boundaries."""

    def __init__(self, transaction: Any, runtime: FaultRuntime, actor: str) -> None:
        self.transaction = transaction
        self.runtime = runtime
        self.actor = actor

    async def start(self) -> None:
        await self.runtime.around(
            self.actor,
            RuntimeOperation.DB_BEGIN,
            self.transaction.start,
            site="transaction",
        )

    async def commit(self) -> None:
        await self.runtime.around(
            self.actor,
            RuntimeOperation.DB_COMMIT,
            self.transaction.commit,
            site="transaction",
        )

    async def rollback(self) -> None:
        await self.runtime.around(
            self.actor,
            RuntimeOperation.DB_ROLLBACK,
            self.transaction.rollback,
            site="transaction",
        )


class FaultConnection:
    """Wrap asyncpg-shaped query methods without changing the store API."""

    def __init__(self, connection: Any, runtime: FaultRuntime, actor: str) -> None:
        self.connection = connection
        self.runtime = runtime
        self.actor = actor

    def transaction(self) -> FaultTransaction:
        return FaultTransaction(self.connection.transaction(), self.runtime, self.actor)

    def is_in_transaction(self) -> bool:
        return bool(self.connection.is_in_transaction())

    def is_closed(self) -> bool:
        return bool(self.connection.is_closed())

    def terminate(self) -> None:
        self.connection.terminate()

    async def call[ValueT](
        self,
        operation: Callable[[], Awaitable[ValueT]],
        query: str,
    ) -> ValueT:
        return await self.runtime.around(
            self.actor,
            RuntimeOperation.DB_QUERY,
            operation,
            site=query_site(query),
        )

    async def execute(self, query: str, *args: object) -> Any:
        return await self.call(lambda: self.connection.execute(query, *args), query)

    async def fetch(self, query: str, *args: object) -> Any:
        return await self.call(lambda: self.connection.fetch(query, *args), query)

    async def fetchrow(self, query: str, *args: object) -> Any:
        return await self.call(lambda: self.connection.fetchrow(query, *args), query)

    async def fetchval(self, query: str, *args: object) -> Any:
        return await self.call(lambda: self.connection.fetchval(query, *args), query)


class FaultPool:
    """Wrap a real pool and inject acquire/release failures."""

    def __init__(self, pool: Any, runtime: FaultRuntime, actor: str = "database") -> None:
        self.pool = pool
        self.runtime = runtime
        self.actor = actor

    async def acquire(self) -> FaultConnection:
        step = self.runtime.take(RuntimeOperation.DB_ACQUIRE, "pool")
        if step.phase is FaultPhase.BEFORE:
            await self.runtime.apply(step, self.actor)
        connection = await self.pool.acquire()
        if step.phase is FaultPhase.AFTER:
            try:
                await self.runtime.apply(step, self.actor)
            except BaseException as error:
                try:
                    await self.pool.release(connection)
                except BaseException as cleanup:
                    error.add_note(
                        "post-acquire cleanup also failed with "
                        f"{type(cleanup).__qualname__}: {cleanup}"
                    )
                raise
        return FaultConnection(connection, self.runtime, self.actor)

    async def release(self, connection: Any) -> None:
        if not isinstance(connection, FaultConnection):
            raise TypeError("fault pool can only release its own connections")
        step = self.runtime.take(RuntimeOperation.DB_RELEASE, "pool")
        if step.phase is FaultPhase.BEFORE:
            try:
                await self.runtime.apply(step, self.actor)
            except BaseException as error:
                # asyncpg terminates or returns its holder when reset fails and
                # shields that cleanup from cancellation. Preserve that
                # contract for every injected release failure.
                try:
                    await self.pool.release(connection.connection)
                except BaseException as cleanup:
                    error.add_note(
                        "faulted release cleanup also failed with "
                        f"{type(cleanup).__qualname__}: {cleanup}"
                    )
                raise
        await self.pool.release(connection.connection)
        if step.phase is FaultPhase.AFTER:
            await self.runtime.apply(step, self.actor)


def query_site(query: str) -> str:
    """Give stable names to SQL files used by phase-aware scripts."""

    normalized = query.lstrip().upper()
    if normalized.startswith("WITH SETTLED AS"):
        return "settle"
    if normalized.startswith("SELECT P.ID\nFROM REAPER.PROMISES P\nJOIN REAPER.TASKS"):
        return "lock_promise"
    if "FOR UPDATE OF T SKIP LOCKED" in normalized:
        return "claim"
    if normalized.startswith("WITH PROMISE AS"):
        return "submit"
    if normalized.startswith("WITH REQUESTED AS"):
        return "suspend"
    if normalized.startswith("WITH RETRIED AS"):
        return "retry"
    return "query"
