"""Typed observable boundaries for deterministic runtime testing."""

from enum import StrEnum
from typing import Protocol


class RuntimeOperation(StrEnum):
    """Name every nondeterministic operation exposed to runtime hooks."""

    CLOCK = "clock"
    SLEEP = "sleep"
    SOCKET_PAIR = "socket_pair"
    PIPE = "pipe"
    DUP_FD = "dup_fd"
    SPAWN_PROCESS = "spawn_process"
    SPAWN_THREAD = "spawn_thread"
    CLOSE_FD = "close_fd"
    CONFIG_WRITE = "config_write"
    CONTROL_SEND = "control_send"
    CONTROL_RECEIVE = "control_receive"
    KILL = "kill"
    WAIT_PROCESS = "wait_process"
    HEARTBEAT_OPEN = "heartbeat_open"
    HEARTBEAT_READ = "heartbeat_read"
    HEARTBEAT_WRITE = "heartbeat_write"
    DB_CONNECT = "db_connect"
    DB_ACQUIRE = "db_acquire"
    DB_BEGIN = "db_begin"
    DB_QUERY = "db_query"
    DB_COMMIT = "db_commit"
    DB_ROLLBACK = "db_rollback"
    DB_RELEASE = "db_release"
    DB_NOTIFY = "db_notify"


class RuntimeMarker(StrEnum):
    """Name deterministic observation points used to order race tests."""

    STARTUP_WAIT = "startup_wait"
    STARTUP_READY = "startup_ready"
    SLOT_SPAWNED = "slot_spawned"
    SHUTDOWN_STARTED = "shutdown_started"


type RuntimeCheckpoint = RuntimeOperation | RuntimeMarker


class RuntimeHooks(Protocol):
    """Observe or override a named nondeterministic operation."""

    async def checkpoint(
        self,
        operation: RuntimeCheckpoint,
        **details: object,
    ) -> object | None: ...


class NullRuntimeHooks:
    """Discard runtime checkpoints in production."""

    async def checkpoint(
        self,
        operation: RuntimeCheckpoint,
        **details: object,
    ) -> object | None:
        del operation, details
        return None
