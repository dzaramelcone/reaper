"""Check deterministic failures at the high-level database boundary."""

import asyncio
from typing import cast

import pytest

from reaper.database import (
    TransactionExecutor,
    TransactionRequiredError,
    decode_json,
    release_connection,
    require_transaction,
)
from reaper.tasks import TaskClaim, Tasks


class BrokenTransactionConnection:
    """Fail before a transaction object can be constructed."""

    def transaction(self) -> object:
        raise RuntimeError("transaction factory failed")


class RecordingPool:
    """Record whether an acquired connection was returned."""

    def __init__(self) -> None:
        self.connection = BrokenTransactionConnection()
        self.acquired = 0
        self.released = 0

    async def acquire(self) -> object:
        self.acquired += 1
        return self.connection

    async def release(self, connection: object) -> None:
        assert connection is self.connection
        self.released += 1


class DelayedClosedConnection:
    """Model a lost asyncpg connection whose close callback has not run yet."""

    def __init__(self) -> None:
        self.finalized = False

    def is_closed(self) -> bool:
        return True

    def terminate(self) -> None:
        self.finalized = True


class DelayedClosePool:
    """Match asyncpg's release fast-path for an already-closed connection."""

    def __init__(self) -> None:
        self.holder_released = False

    async def acquire(self) -> object:
        raise AssertionError("release regression must not acquire")

    async def release(self, connection: object) -> None:
        assert isinstance(connection, DelayedClosedConnection)
        if connection.finalized:
            self.holder_released = True


class CommitAndReleaseFailureConnection:
    """Expose one commit failure followed by a cleanup failure."""

    def is_closed(self) -> bool:
        return False

    def is_in_transaction(self) -> bool:
        return True

    def terminate(self) -> None:
        raise AssertionError("an open test connection must not be terminated")


class CommitFailureTransaction:
    async def commit(self) -> None:
        raise RuntimeError("commit failed")

    async def rollback(self) -> None:
        raise AssertionError("commit path must not roll back")


class ReleaseFailurePool:
    async def acquire(self) -> object:
        raise AssertionError("close regression must not acquire")

    async def release(self, connection: object) -> None:
        assert isinstance(connection, CommitAndReleaseFailureConnection)
        raise RuntimeError("release failed")


def test_database_boundary_rejects_wrong_json_and_unowned_transactions() -> None:
    class NoTransaction:
        def is_in_transaction(self) -> bool:
            return False

    with pytest.raises(TypeError, match="projection must be text"):
        decode_json({})
    executor = cast(TransactionExecutor, NoTransaction())
    with pytest.raises(TransactionRequiredError, match="explicit transaction"):
        require_transaction(executor)


def test_claim_transaction_factory_failure_releases_the_acquired_connection() -> None:
    async def check() -> None:
        pool = RecordingPool()
        with pytest.raises(RuntimeError, match="transaction factory failed"):
            async with Tasks(pool).claim():
                raise AssertionError("claim body must not run")
        assert pool.acquired == 1
        assert pool.released == 1

    asyncio.run(check())


def test_release_finalizes_a_closed_connection_before_returning_it_to_pool() -> None:
    async def check() -> None:
        pool = DelayedClosePool()
        connection = DelayedClosedConnection()

        await release_connection(pool, connection)

        assert connection.finalized
        assert pool.holder_released

    asyncio.run(check())


def test_task_claim_preserves_commit_failure_when_release_also_fails() -> None:
    """An ambiguous COMMIT remains primary when connection cleanup also faults."""

    async def check() -> None:
        claim = TaskClaim(ReleaseFailurePool(), params=Tasks(ReleaseFailurePool()).claim().params)
        with pytest.raises(RuntimeError, match="commit failed") as raised:
            await claim.close_connection(
                cast(TransactionExecutor, CommitAndReleaseFailureConnection()),
                CommitFailureTransaction(),
                commit=True,
            )
        assert any("release failed" in note for note in (raised.value.__notes__ or ()))

    asyncio.run(check())
