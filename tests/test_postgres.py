"""Verify bounded PostgreSQL connection lifecycle behavior."""

import asyncio
from collections.abc import Callable
from typing import Any, cast

import asyncpg
import pytest
from pydantic import PostgresDsn

from reaper.database.queries import PROBE
from reaper.postgres import ListenerActivity, ListenerWake, PostgresListener, PostgresPool
from reaper.runtime import RuntimeOperation
from tests.fault_runtime import FaultHooks, FaultRuntime
from tests.faults import FaultPhase, FaultStep, OutcomeKind
from tests.scheduler import DeterministicScheduler


class FakeConnection:
    """Record listener operations."""

    def __init__(self) -> None:
        self.closed = False
        self.probes = 0
        self.probe_error: BaseException | None = None
        self.close_error: BaseException | None = None
        self.listener_error: BaseException | None = None
        self.hang_close = False
        self.hang_probe = False
        self.terminated = False
        self.callback: Callable[[object, int, str, object], None] | None = None

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[object, int, str, object], None],
    ) -> None:
        assert channel == "channel"
        if self.listener_error is not None:
            raise self.listener_error
        self.callback = callback

    async def fetchval(self, query: str) -> int:
        assert query == PROBE
        self.probes += 1
        if self.hang_probe:
            await asyncio.Event().wait()
        if self.probe_error is not None:
            raise self.probe_error
        return 1

    def is_closed(self) -> bool:
        return self.closed

    async def close(self) -> None:
        self.closed = True
        if self.hang_close:
            await asyncio.Event().wait()
        if self.close_error is not None:
            raise self.close_error

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True


class HangingPool:
    """Model asyncpg waiting forever for a damaged pool to close."""

    def __init__(self) -> None:
        self.terminated = False

    async def close(self) -> None:
        await asyncio.Event().wait()

    def terminate(self) -> None:
        self.terminated = True


def test_executor_terminates_a_pool_that_cannot_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def check() -> None:
        monkeypatch.setattr("reaper.postgres.POOL_CLOSE_TIMEOUT", 0.01)
        pool = HangingPool()
        executor = PostgresPool(PostgresDsn("postgresql://worker:secret@db/reaper"))
        executor.pool = cast(Any, pool)
        await asyncio.wait_for(executor.close(), timeout=0.1)
        assert pool.terminated
        assert executor.pool is None

    asyncio.run(check())


def test_listener_terminates_an_old_connection_that_cannot_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def check() -> None:
        monkeypatch.setattr("reaper.postgres.POOL_CLOSE_TIMEOUT", 0.01)
        connections: list[FakeConnection] = []

        async def connect(dsn: str) -> FakeConnection:
            assert dsn == "postgresql://worker:secret@db/reaper"
            connection = FakeConnection()
            connections.append(connection)
            return connection

        monkeypatch.setattr("reaper.postgres.asyncpg.connect", connect)
        listener = PostgresListener(
            PostgresDsn("postgresql://worker:secret@db/reaper"),
            "channel",
            recycle_rate=100.0,
            probe_rate=10.0,
        )
        await listener.connect()
        connections[0].hang_close = True
        await asyncio.wait_for(listener.connect(), timeout=0.1)
        assert connections[0].terminated
        assert cast(object, listener.connection) is connections[1]
        await listener.close()

    asyncio.run(check())


def test_listener_setup_failure_terminates_a_replacement_that_cannot_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A LISTEN setup fault must not hang while discarding its new connection."""

    async def check() -> None:
        monkeypatch.setattr("reaper.postgres.POOL_CLOSE_TIMEOUT", 0.01)
        connection = FakeConnection()
        connection.listener_error = asyncpg.InterfaceError("LISTEN failed")
        connection.hang_close = True

        async def connect(dsn: str) -> FakeConnection:
            assert dsn == "postgresql://worker:secret@db/reaper"
            return connection

        monkeypatch.setattr("reaper.postgres.asyncpg.connect", connect)
        listener = PostgresListener(
            PostgresDsn("postgresql://worker:secret@db/reaper"),
            "channel",
            recycle_rate=100.0,
            probe_rate=10.0,
        )
        with pytest.raises(asyncpg.InterfaceError, match="LISTEN failed"):
            await asyncio.wait_for(listener.connect(), timeout=0.1)
        assert connection.terminated
        assert listener.connection is None

    asyncio.run(check())


def test_post_connect_fault_closes_the_unpublished_listener_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def check() -> None:
        connection = FakeConnection()

        async def connect(dsn: str) -> FakeConnection:
            assert dsn == "postgresql://worker:secret@db/reaper"
            return connection

        monkeypatch.setattr("reaper.postgres.asyncpg.connect", connect)
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.DB_CONNECT,
                    outcome=OutcomeKind.CONNECTION_LOST,
                    phase=FaultPhase.AFTER,
                )
            ],
            DeterministicScheduler(),
        )
        listener = PostgresListener(
            PostgresDsn("postgresql://worker:secret@db/reaper"),
            "channel",
            recycle_rate=100.0,
            probe_rate=10.0,
            hooks=FaultHooks(runtime),
        )
        with pytest.raises(asyncpg.ConnectionDoesNotExistError):
            await listener.connect()
        assert connection.closed
        assert listener.connection is None
        assert not runtime.steps

    asyncio.run(check())


def test_lost_notification_fault_immediately_uses_fallback_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def check() -> None:
        connection = FakeConnection()

        async def connect(dsn: str) -> FakeConnection:
            assert dsn == "postgresql://worker:secret@db/reaper"
            return connection

        monkeypatch.setattr("reaper.postgres.asyncpg.connect", connect)
        runtime = FaultRuntime([], DeterministicScheduler())
        listener = PostgresListener(
            PostgresDsn("postgresql://worker:secret@db/reaper"),
            "channel",
            recycle_rate=100.0,
            probe_rate=10.0,
            hooks=FaultHooks(runtime),
        )
        await listener.connect()
        await listener.arm()
        runtime.steps.append(
            FaultStep(
                call=RuntimeOperation.DB_NOTIFY,
                outcome=OutcomeKind.LOST,
                site="wait",
            )
        )
        wake, _ = await asyncio.wait_for(listener.wait(10.0), timeout=0.1)
        assert wake is ListenerWake.FALLBACK
        assert not runtime.steps
        await listener.close()

    asyncio.run(check())


@pytest.mark.parametrize("outcome", (OutcomeKind.DUPLICATE, OutcomeKind.REORDERED))
def test_delivered_notification_faults_coalesce_into_an_immediate_poll(
    monkeypatch: pytest.MonkeyPatch,
    outcome: OutcomeKind,
) -> None:
    """Duplicate and reordered hints must wake; the subsequent poll is authoritative."""

    async def check() -> None:
        connection = FakeConnection()

        async def connect(dsn: str) -> FakeConnection:
            assert dsn == "postgresql://worker:secret@db/reaper"
            return connection

        monkeypatch.setattr("reaper.postgres.asyncpg.connect", connect)
        runtime = FaultRuntime([], DeterministicScheduler())
        listener = PostgresListener(
            PostgresDsn("postgresql://worker:secret@db/reaper"),
            "channel",
            recycle_rate=100.0,
            probe_rate=10.0,
            hooks=FaultHooks(runtime),
        )
        await listener.connect()
        await listener.arm()
        runtime.steps.append(
            FaultStep(
                call=RuntimeOperation.DB_NOTIFY,
                outcome=outcome,
                site="wait",
            )
        )
        wake, _ = await asyncio.wait_for(listener.wait(10.0), timeout=0.1)
        assert wake is ListenerWake.NOTIFIED
        assert not runtime.steps
        await listener.close()

    asyncio.run(check())


def test_listener_probes_and_recycles_on_independent_timers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def check() -> None:
        clock = [0.0]
        connections: list[FakeConnection] = []

        async def connect(dsn: str) -> FakeConnection:
            assert dsn == "postgresql://worker:secret@db/reaper"
            connection = FakeConnection()
            connections.append(connection)
            return connection

        monkeypatch.setattr("reaper.postgres.asyncpg.connect", connect)
        monkeypatch.setattr("reaper.postgres.monotonic", lambda: clock[0])
        listener = PostgresListener(
            PostgresDsn("postgresql://worker:secret@db/reaper"),
            "channel",
            recycle_rate=100.0,
            probe_rate=10.0,
        )

        await listener.connect()
        assert len(connections) == 1
        clock[0] = 10.0
        await listener.maintain()
        assert connections[0].probes == 1
        clock[0] = 100.0
        await listener.maintain()
        assert len(connections) == 2
        assert connections[0].closed
        assert not connections[1].closed
        await listener.close()
        assert connections[1].closed

    asyncio.run(check())


def test_listener_keeps_a_replacement_when_the_dead_connection_will_not_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def check() -> None:
        connections: list[FakeConnection] = []

        async def connect(dsn: str) -> FakeConnection:
            assert dsn == "postgresql://worker:secret@db/reaper"
            connection = FakeConnection()
            connections.append(connection)
            return connection

        monkeypatch.setattr("reaper.postgres.asyncpg.connect", connect)
        listener = PostgresListener(
            PostgresDsn("postgresql://worker:secret@db/reaper"),
            "channel",
            recycle_rate=100.0,
            probe_rate=10.0,
        )

        await listener.connect()
        connections[0].close_error = asyncpg.InterfaceError("connection is closed")
        await listener.connect()
        assert cast(object, listener.connection) is connections[1]
        assert connections[0].closed
        await listener.close()

    asyncio.run(check())


def test_listener_recycles_when_a_liveness_probe_loses_its_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def check() -> None:
        clock = [0.0]
        connections: list[FakeConnection] = []

        async def connect(dsn: str) -> FakeConnection:
            assert dsn == "postgresql://worker:secret@db/reaper"
            connection = FakeConnection()
            connections.append(connection)
            return connection

        monkeypatch.setattr("reaper.postgres.asyncpg.connect", connect)
        monkeypatch.setattr("reaper.postgres.monotonic", lambda: clock[0])
        listener = PostgresListener(
            PostgresDsn("postgresql://worker:secret@db/reaper"),
            "channel",
            recycle_rate=100.0,
            probe_rate=10.0,
        )

        await listener.connect()
        connections[0].probe_error = asyncpg.ConnectionDoesNotExistError("connection was closed")
        clock[0] = 10.0
        assert await listener.maintain() is ListenerActivity.RECYCLED
        assert len(connections) == 2
        assert connections[0].closed
        assert not connections[1].closed
        await listener.close()

    asyncio.run(check())


def test_listener_recycles_when_its_liveness_probe_never_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A changing process heartbeat must not hide a wedged LISTEN connection."""

    async def check() -> None:
        monkeypatch.setattr("reaper.postgres.LISTENER_OPERATION_TIMEOUT", 0.01)
        clock = [0.0]
        connections: list[FakeConnection] = []

        async def connect(dsn: str) -> FakeConnection:
            assert dsn == "postgresql://worker:secret@db/reaper"
            connection = FakeConnection()
            connections.append(connection)
            return connection

        monkeypatch.setattr("reaper.postgres.asyncpg.connect", connect)
        monkeypatch.setattr("reaper.postgres.monotonic", lambda: clock[0])
        listener = PostgresListener(
            PostgresDsn("postgresql://worker:secret@db/reaper"),
            "channel",
            recycle_rate=100.0,
            probe_rate=10.0,
        )
        await listener.connect()
        connections[0].hang_probe = True
        clock[0] = 10.0
        assert await asyncio.wait_for(listener.maintain(), timeout=0.1) is ListenerActivity.RECYCLED
        assert len(connections) == 2
        assert connections[0].closed
        await listener.close()

    asyncio.run(check())
