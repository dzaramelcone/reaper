"""Async PostgreSQL pool and listener links for Reaper."""

import asyncio
import time
from enum import StrEnum

import asyncpg
from pydantic import PostgresDsn

from reaper.database import note_cleanup_failure
from reaper.database.queries import PROBE
from reaper.runtime import NullRuntimeHooks, RuntimeHooks, RuntimeOperation

POOL_BURST_SIZE = 10
POOL_IDLE_LIFETIME = 30.0
POOL_CLOSE_TIMEOUT = 1.0
LISTENER_OPERATION_TIMEOUT = 30.0


def monotonic() -> float:
    """Read listener maintenance time without replacing asyncio's global clock."""

    return time.monotonic()


class ListenerActivity(StrEnum):
    """Report maintenance performed on a LISTEN connection."""

    NONE = "none"
    PROBED = "probed"
    RECYCLED = "recycled"


class ListenerWake(StrEnum):
    """Report why an idle listener resumed polling."""

    NOTIFIED = "notified"
    FALLBACK = "fallback"


async def close_postgres_connection(
    connection: asyncpg.Connection[asyncpg.Record],
) -> None:
    """Bound graceful close and terminate a connection that cannot drain."""

    try:
        async with asyncio.timeout(POOL_CLOSE_TIMEOUT):
            await connection.close()
    except asyncio.CancelledError:
        connection.terminate()
        raise
    except (
        TimeoutError,
        asyncpg.PostgresConnectionError,
        asyncpg.InterfaceError,
        OSError,
    ):
        connection.terminate()


class PostgresListener:
    """Own and periodically recycle one dedicated LISTEN connection."""

    def __init__(
        self,
        dsn: PostgresDsn,
        channel: str,
        *,
        recycle_rate: float,
        probe_rate: float,
        hooks: RuntimeHooks | None = None,
    ) -> None:
        self.dsn = dsn
        self.channel = channel
        self.recycle_rate = recycle_rate
        self.probe_rate = probe_rate
        self.connection: asyncpg.Connection[asyncpg.Record] | None = None
        self.notified = asyncio.Event()
        self.connected_at = 0.0
        self.probed_at = 0.0
        self.hooks = hooks or NullRuntimeHooks()

    def receive(
        self,
        connection: object,
        pid: int,
        channel: str,
        payload: object,
    ) -> None:
        """Wake a waiter after Postgres delivers one notification."""

        del connection, pid, channel, payload
        self.notified.set()

    async def connect(self) -> None:
        """Replace the listener before closing its predecessor."""

        await self.hooks.checkpoint(RuntimeOperation.DB_CONNECT, actor="listener")
        async with asyncio.timeout(LISTENER_OPERATION_TIMEOUT):
            replacement = await asyncpg.connect(str(self.dsn))
        try:
            await self.hooks.checkpoint(
                RuntimeOperation.DB_CONNECT,
                actor="listener",
                phase="after",
            )
            await self.hooks.checkpoint(
                RuntimeOperation.DB_QUERY,
                actor="listener",
                purpose="listen",
            )
            async with asyncio.timeout(LISTENER_OPERATION_TIMEOUT):
                await replacement.add_listener(self.channel, self.receive)
            await self.hooks.checkpoint(
                RuntimeOperation.DB_QUERY,
                actor="listener",
                purpose="listen",
                phase="after",
            )
        except BaseException as error:
            try:
                await close_postgres_connection(replacement)
            except BaseException as cleanup:
                note_cleanup_failure(error, cleanup, "listener setup")
            raise
        previous = self.connection
        self.connection = replacement
        now = monotonic()
        self.connected_at = now
        self.probed_at = now
        self.notified.set()
        if previous is not None:
            await close_postgres_connection(previous)

    async def close(self) -> None:
        """Close the current dedicated connection."""

        connection = self.connection
        self.connection = None
        if connection is not None:
            await close_postgres_connection(connection)

    async def maintain(self) -> ListenerActivity:
        """Probe or recycle the listener when its liveness timers expire."""

        now = monotonic()
        connection = self.connection
        if (
            connection is None
            or connection.is_closed()
            or now - self.connected_at >= self.recycle_rate
        ):
            await self.connect()
            return ListenerActivity.RECYCLED
        if now - self.probed_at >= self.probe_rate:
            try:
                await self.hooks.checkpoint(
                    RuntimeOperation.DB_QUERY,
                    actor="listener",
                    purpose="probe",
                )
                async with asyncio.timeout(LISTENER_OPERATION_TIMEOUT):
                    await connection.fetchval(PROBE)
                await self.hooks.checkpoint(
                    RuntimeOperation.DB_QUERY,
                    actor="listener",
                    purpose="probe",
                    phase="after",
                )
            except asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError:
                await self.connect()
                return ListenerActivity.RECYCLED
            self.probed_at = monotonic()
            return ListenerActivity.PROBED
        return ListenerActivity.NONE

    async def arm(self) -> ListenerActivity:
        """Prepare for a poll followed by a notification wait."""

        activity = await self.maintain()
        self.notified.clear()
        return activity

    async def wait(self, fallback_rate: float) -> tuple[ListenerWake, ListenerActivity]:
        """Wait for work, a liveness probe, recycling, or fallback polling."""

        injected_delivery = await self.hooks.checkpoint(
            RuntimeOperation.DB_NOTIFY,
            actor="listener",
            purpose="wait",
        )
        if injected_delivery is not None and not isinstance(injected_delivery, int):
            raise TypeError("notification fault override must be an integer")
        now = monotonic()
        timeout = min(
            fallback_rate,
            max(0.0, self.recycle_rate - (now - self.connected_at)),
            max(0.0, self.probe_rate - (now - self.probed_at)),
        )
        wake = ListenerWake.NOTIFIED
        if injected_delivery == 0:
            wake = ListenerWake.FALLBACK
        elif injected_delivery is not None:
            # LISTEN/NOTIFY is an edge-triggered hint. Duplicate and reordered
            # deliveries intentionally coalesce into one immediate poll.
            wake = ListenerWake.NOTIFIED
        else:
            try:
                await asyncio.wait_for(self.notified.wait(), timeout=timeout)
            except TimeoutError:
                wake = ListenerWake.FALLBACK
        await self.hooks.checkpoint(
            RuntimeOperation.DB_NOTIFY,
            actor="listener",
            purpose="wait",
            phase="after",
        )
        return wake, await self.maintain()


class PostgresPool:
    """Own one burstable asyncpg connection pool."""

    def __init__(self, dsn: PostgresDsn, hooks: RuntimeHooks | None = None) -> None:
        self.dsn = dsn
        self.pool: asyncpg.Pool[asyncpg.Record] | None = None
        self.hooks = hooks or NullRuntimeHooks()

    async def connect(self) -> None:
        if self.pool is None:
            await self.hooks.checkpoint(RuntimeOperation.DB_CONNECT, actor="pool")
            pool = await asyncpg.create_pool(
                str(self.dsn),
                min_size=1,
                max_size=POOL_BURST_SIZE,
                max_inactive_connection_lifetime=POOL_IDLE_LIFETIME,
            )
            self.pool = pool
            try:
                await self.hooks.checkpoint(
                    RuntimeOperation.DB_CONNECT,
                    actor="pool",
                    phase="after",
                )
            except BaseException as error:
                try:
                    await self.close()
                except BaseException as cleanup:
                    note_cleanup_failure(error, cleanup, "executor connect")
                raise

    async def close(self) -> None:
        pool = self.pool
        self.pool = None
        if pool is None:
            return
        try:
            async with asyncio.timeout(POOL_CLOSE_TIMEOUT):
                await pool.close()
        except asyncio.CancelledError:
            pool.terminate()
            raise
        except (
            TimeoutError,
            asyncpg.PostgresConnectionError,
            asyncpg.InterfaceError,
            OSError,
        ):
            pool.terminate()

    def get_pool(self) -> asyncpg.Pool[asyncpg.Record]:
        """Return the open pool for the domain APIs."""

        if self.pool is None:
            raise RuntimeError("open the Postgres link first")
        return self.pool
