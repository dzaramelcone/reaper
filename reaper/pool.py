"""A small pool of warm child jobs."""

import asyncio
import fcntl
import importlib
import logging
import os
import pickle
import signal
import socket
import struct
import sys
import tempfile
import threading
import time
import traceback
import uuid
import weakref
from collections.abc import Callable, Coroutine, Mapping, Sequence
from contextlib import suppress
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Protocol, Self, cast

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, PostgresDsn, SkipValidation, model_validator

from reaper.control import (
    ControlEffect,
    ControlEvent,
    EffectKind,
    EventKind,
    FailureReason,
    ReaperCore,
    SkeletonID,
    SkeletonState,
)
from reaper.database import note_cleanup_failure, shield_cleanup
from reaper.log import configure_logging, write
from reaper.models import DEFAULT_TOPIC, ResultState
from reaper.runtime import NullRuntimeHooks, RuntimeHooks, RuntimeMarker, RuntimeOperation
from reaper.settings import (
    DEFAULT_GC_RATE,
    DEFAULT_LISTENER_PROBE_RATE,
    DEFAULT_LISTENER_RECYCLE_RATE,
    DEFAULT_LOG_LEVEL,
    DEFAULT_MAINTENANCE_RATE,
    DEFAULT_MAX_QUEUED_JOBS,
    DEFAULT_POLL_RATE,
    DEFAULT_RETENTION_MS,
    DEFAULT_SERVICE_FAILURE_ROUNDS,
    DEFAULT_SERVICE_RETRY_BASE,
    DEFAULT_SERVICE_RETRY_MAX,
    DEFAULT_STARTUP_TIMEOUT,
    PoolKind,
    ReaperSettings,
)
from reaper.skeleton import (
    LifecycleEvent,
    LifecycleKind,
    LifecycleLevel,
    ListenerPhase,
    SkeletonCore,
    SkeletonPhase,
    TaskReleaseReason,
    WorkOutcome,
)
from reaper.worker import LifecycleReporter, poll_maintenance, poll_tasks

log = logging.getLogger(__name__)

HEADER = struct.Struct("!I")
MAX_MSG = 64 * 1024 * 1024
CHILD_CONTROL_FD = 3
CHILD_DEATH_FD = 4
CHILD_CONFIG_FD = 5
CHILD_BEAT_FD = 6
MAX_CHILD_CONFIG = 8 * 1024
RECOVERY_BASE_DELAY = 0.05
RECOVERY_MAX_DELAY = 1.0
MAX_PRE_READY_FAILURES = 5
HEARTBEAT_MISSES = 5
HEARTBEAT_MIN_TIMEOUT = 1.0
CHILD_MODE = "child"
PERMANENT_SQLSTATE_CLASSES = ("28", "42")
PERMANENT_SQLSTATES = frozenset({"42501"})
TRANSIENT_SQLSTATE_CLASSES = ("08", "40", "53", "55", "57")
TRANSIENT_EXCEPTION_NAMES = frozenset(
    exception.__name__
    for exception in (
        ConnectionError,
        ConnectionRefusedError,
        ConnectionResetError,
        OSError,
        TimeoutError,
    )
)


class WorkerKind(StrEnum):
    SYNC = "sync"
    ASYNC = "async"
    BASH = "bash"


class SkeletonRole(StrEnum):
    GENERAL = "general"
    TASK = PoolKind.TASK
    MAINTENANCE = PoolKind.MAINTENANCE


class MessageOp(StrEnum):
    """Messages exchanged over a skeleton control socket."""

    READY = "ready"
    HEALTHY = "healthy"
    LIFECYCLE = "lifecycle"
    STATE = "state"
    SERVICE_FAULT = "service_fault"
    FATAL = "fatal"
    RESULT = "result"
    RUN = "run"
    STOP = "stop"


class ChildConfig(BaseModel):
    """Validated configuration transferred over an inherited pipe."""

    model_config = ConfigDict(frozen=True, strict=True)

    beat_rate: float
    event_loop_type: str | None = None
    cgroup_path: Path | None = None
    role: SkeletonRole
    postgres_dsn: PostgresDsn | None = None
    topic: Annotated[str, Field(min_length=1, max_length=255)] | None = None
    poll_rate: float = DEFAULT_POLL_RATE
    maintenance_rate: float = DEFAULT_MAINTENANCE_RATE
    log_level: str = DEFAULT_LOG_LEVEL
    service_retry_base: float = DEFAULT_SERVICE_RETRY_BASE
    service_retry_max: float = DEFAULT_SERVICE_RETRY_MAX
    listener_probe_rate: float = DEFAULT_LISTENER_PROBE_RATE
    listener_recycle_rate: float = DEFAULT_LISTENER_RECYCLE_RATE
    gc_rate: float = DEFAULT_GC_RATE
    retention_ms: int = DEFAULT_RETENTION_MS

    @model_validator(mode="after")
    def validate_service(self) -> Self:
        if self.role is not SkeletonRole.GENERAL and self.postgres_dsn is None:
            raise ValueError("a service skeleton requires a Postgres DSN")
        if self.role is SkeletonRole.TASK and self.topic is None:
            raise ValueError("a task skeleton requires a topic")
        return self


LOG_LEVEL = {
    LifecycleLevel.DEBUG: logging.DEBUG,
    LifecycleLevel.INFO: logging.INFO,
    LifecycleLevel.WARNING: logging.WARNING,
    LifecycleLevel.ERROR: logging.ERROR,
}


class Cgroup(BaseModel):
    """Place a child in Linux."""

    model_config = ConfigDict(frozen=True)

    path: Path

    def join(self) -> None:
        file = self.path / "cgroup.procs"
        file.write_text(str(os.getpid()), encoding="ascii")


class Work(BaseModel):
    """Hold one child job."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: str
    kind: WorkerKind
    target: object
    args: tuple[object, ...] = ()
    options: Mapping[str, object] = Field(default_factory=dict)


class Pending[ValueT](BaseModel):
    """Link one job and wait."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    work: Work
    future: asyncio.Future[ValueT]


class HeartbeatWatch(BaseModel):
    """Decide liveness from marker changes and parent-monotonic elapsed time."""

    model_config = ConfigDict(validate_assignment=True)

    marker: bytes = b""
    unchanged_since: float = 0.0
    checked_at: float = 0.0

    def observe(self, marker: bytes, now: float, timeout: float) -> bool:
        """Return false only after consecutive timely probes see no change."""

        if timeout <= 0:
            raise ValueError("heartbeat timeout must be more than zero")
        first_observation = self.checked_at == 0
        observer_paused = self.checked_at > 0 and now - self.checked_at > timeout
        self.checked_at = now
        if first_observation or observer_paused or marker != self.marker:
            self.marker = marker
            self.unchanged_since = now
            return True
        return now - self.unchanged_since < timeout


class Slot(BaseModel):
    """Hold one live child slot."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    identity: SkeletonID
    control: socket.socket
    death_writer: int
    beat: Path
    beat_fd: Annotated[int, Field(exclude=True)] = -1
    heartbeat: Annotated[HeartbeatWatch, Field(exclude=True)] = Field(
        default_factory=HeartbeatWatch
    )
    pid: Annotated[int, Field(exclude=True)]
    started_at: Annotated[float, Field(exclude=True)]
    state: SkeletonState = SkeletonState.STARTING
    job_id: str | None = None
    reader: asyncio.Task[None] | None = None
    expected_stop: bool = False
    fault_kind: str = ""
    fault_text: str = ""
    fault_trace: str = ""
    skeleton_id: str = ""
    lifecycle_phase: SkeletonPhase = SkeletonPhase.STARTING
    listener_generation: int = 0


class ExitStatus(BaseModel):
    """Describe one reaped process."""

    model_config = ConfigDict(frozen=True)

    pid: int
    exit_code: int
    signal: int


class RemoteWorkerError(RuntimeError):
    """Show a child job fault."""

    def __init__(self, kind: str, text: str, trace: str) -> None:
        super().__init__(f"{kind}: {text}\n{trace}")
        self.kind = kind
        self.trace = trace


class AsyncByteTransport(Protocol):
    """Write one complete byte sequence."""

    async def sendall(self, data: bytes) -> None: ...


class AsyncSocketLoop(Protocol):
    """Expose the event-loop operation needed by a socket transport."""

    async def sock_sendall(self, sock: socket.socket, data: bytes) -> None: ...


class SocketTransport(BaseModel):
    """Adapt an asyncio socket operation to `AsyncByteTransport`."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    loop: Annotated[AsyncSocketLoop, SkipValidation()]
    sock: socket.socket

    async def sendall(self, data: bytes) -> None:
        await self.loop.sock_sendall(self.sock, data)


class FramedWriter(BaseModel):
    """Serialize complete length-prefixed messages onto one byte stream."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    transport: Annotated[AsyncByteTransport, SkipValidation()]
    lock: asyncio.Lock = Field(default_factory=asyncio.Lock)

    async def send(self, value: object) -> None:
        frame = encode_message(value)
        async with self.lock:
            await self.transport.sendall(frame)


class SkeletonPool:
    """Keep a set count of warm jobs."""

    def __init__(
        self,
        slots: int,
        *,
        event_loop_type: type[asyncio.AbstractEventLoop] | None = None,
        beat_rate: float = 1.0,
        beat_dir: Path | None = None,
        cgroup: Cgroup | None = None,
        id_source: Callable[[], str] | None = None,
        role: SkeletonRole = SkeletonRole.GENERAL,
        hooks: RuntimeHooks | None = None,
    ) -> None:
        """Build slots but fork no child yet."""

        if slots <= 0:
            raise ValueError("slots must be more than zero")
        if beat_rate <= 0:
            raise ValueError("beat_rate must be more than zero")
        self.target = slots
        self.event_loop_type = event_loop_type
        self.beat_rate = beat_rate
        self.cgroup = cgroup
        self.id_source = id_source or new_job_id
        self.role = role
        self.hooks = hooks or NullRuntimeHooks()
        self.topic: str | None = None
        self.temp_dir: tempfile.TemporaryDirectory[str] | None = None
        self.anonymous_beats = beat_dir is None
        if beat_dir is None:
            self.temp_dir = tempfile.TemporaryDirectory(prefix="reaper-")
            beat_dir = Path(self.temp_dir.name)
        self.beat_dir = beat_dir
        self.beat_dir.mkdir(parents=True, exist_ok=True)
        self.core = ReaperCore(slots)
        self.slots: dict[int, Slot] = {}
        self.retired_slots: dict[tuple[int, int], Slot] = {}
        self.jobs: dict[str, Pending[object]] = {}
        self.job_sequence = 0
        self.results: dict[str, Mapping[str, object]] = {}
        self.loop: asyncio.AbstractEventLoop | None = None
        self.started = False
        self.closing = False
        self.settings: ReaperSettings | None = None
        self.child_pools: list[SkeletonPool] = []
        self.log_level = DEFAULT_LOG_LEVEL
        self.recovery_task: asyncio.Task[None] | None = None
        self.heartbeat_task: asyncio.Task[None] | None = None
        self.close_task: asyncio.Task[None] | None = None
        self.pre_ready_failures = 0
        self.startup_error: RuntimeError | None = None
        self.startup_timeout = DEFAULT_STARTUP_TIMEOUT
        self.queue_limit = DEFAULT_MAX_QUEUED_JOBS
        self.service_failure_rounds = DEFAULT_SERVICE_FAILURE_ROUNDS
        self.service_fault = ""
        self.service_fault_slots: set[int] = set()
        self.service_fault_round = 0
        self.failure_event = asyncio.Event()
        self.failure_errors: list[RuntimeError] = []

    @classmethod
    def from_settings(cls, settings: ReaperSettings) -> Self:
        """Build a pool from typed settings."""

        if not settings.pools:
            raise ValueError("declare at least one skeleton pool")
        made: list[SkeletonPool] = []
        for config in settings.pools:
            match config.kind:
                case PoolKind.TASK:
                    role = SkeletonRole.TASK
                case PoolKind.MAINTENANCE:
                    role = SkeletonRole.MAINTENANCE
            pool = cls(
                config.skeletons,
                beat_rate=settings.beat_rate,
                role=role,
            )
            pool.settings = settings
            pool.topic = config.topic if config.topic is not None else DEFAULT_TOPIC
            pool.log_level = settings.log_level
            pool.startup_timeout = settings.startup_timeout
            pool.queue_limit = settings.max_queued_jobs
            pool.service_failure_rounds = settings.service_failure_rounds
            made.append(pool)
        root = made.pop(0)
        root.child_pools = made
        for child_pool in root.child_pools:
            child_pool.failure_event = root.failure_event
            child_pool.failure_errors = root.failure_errors
        return cast(Self, root)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, kind: object, value: object, trace: object) -> None:
        await self.close()

    def status(self) -> tuple[tuple[SkeletonID, SkeletonState, Path], ...]:
        """Hide PIDs from pool status."""

        return tuple((slot.identity, slot.state, slot.beat) for slot in self.slots.values())

    async def wait_failure(self) -> RuntimeError:
        """Wait until one pool circuit opens."""

        await self.failure_event.wait()
        assert self.failure_errors
        await self.close()
        return self.failure_errors[0]

    async def start(self) -> None:
        if self.closing:
            raise RuntimeError("SkeletonPool is now shut")
        if self.started:
            return
        started_children: list[SkeletonPool] = []
        try:
            for pool in self.child_pools:
                await pool.start()
                started_children.append(pool)
        except BaseException as error:
            for pool in reversed(started_children):
                try:
                    await pool.close()
                except BaseException as cleanup:
                    note_cleanup_failure(error, cleanup, "child pool startup")
            raise
        self.loop = asyncio.get_running_loop()
        self.started = True
        try:
            await self.finish_start()
        except BaseException as error:
            try:
                await self.close()
            except BaseException as cleanup:
                note_cleanup_failure(error, cleanup, "pool startup")
            raise

    async def finish_start(self) -> None:
        """Reach target capacity after startup ownership is established."""

        self.heartbeat_task = self.get_loop().create_task(self.monitor_heartbeats())
        write(log, logging.INFO, "pool started", role=self.role, slots=self.target)
        await self.drive(self.core.apply(ControlEvent(kind=EventKind.START)))
        deadline = self.get_loop().time() + self.startup_timeout
        while len(self.slots) != self.target or any(
            slot.state is not SkeletonState.IDLE for slot in self.slots.values()
        ):
            if self.startup_error is not None:
                error = self.startup_error
                raise error
            if self.get_loop().time() >= deadline:
                raise TimeoutError("skeleton pool did not reach ready target capacity")
            await self.hooks.checkpoint(
                RuntimeMarker.STARTUP_WAIT,
                actor=self.role.value,
                live=len(self.slots),
                target=self.target,
            )
            await asyncio.sleep(0.01)
        await self.hooks.checkpoint(
            RuntimeMarker.STARTUP_READY,
            actor=self.role.value,
            live=len(self.slots),
            target=self.target,
        )

    async def spawn_effect(self, gen: int) -> object:
        """Spawn one slot with no copied thread state."""

        loop = self.get_loop()
        settings = self.settings
        child_config = ChildConfig(
            beat_rate=self.beat_rate,
            event_loop_type=type_name(self.event_loop_type),
            cgroup_path=self.cgroup.path if self.cgroup is not None else None,
            role=self.role,
            postgres_dsn=settings.postgres_dsn if settings is not None else None,
            topic=self.topic,
            poll_rate=settings.poll_rate if settings is not None else DEFAULT_POLL_RATE,
            maintenance_rate=(
                settings.maintenance_rate if settings is not None else DEFAULT_MAINTENANCE_RATE
            ),
            log_level=self.log_level,
            service_retry_base=(
                settings.service_retry_base if settings is not None else DEFAULT_SERVICE_RETRY_BASE
            ),
            service_retry_max=(
                settings.service_retry_max if settings is not None else DEFAULT_SERVICE_RETRY_MAX
            ),
            listener_probe_rate=(
                settings.listener_probe_rate
                if settings is not None
                else DEFAULT_LISTENER_PROBE_RATE
            ),
            listener_recycle_rate=(
                settings.listener_recycle_rate
                if settings is not None
                else DEFAULT_LISTENER_RECYCLE_RATE
            ),
            gc_rate=settings.gc_rate if settings is not None else DEFAULT_GC_RATE,
            retention_ms=(settings.retention_ms if settings is not None else DEFAULT_RETENTION_MS),
        )
        config = child_config.model_dump_json().encode()
        if len(config) > MAX_CHILD_CONFIG:
            raise ValueError("child configuration is too large")
        parent_sock: socket.socket | None = None
        child_sock: socket.socket | None = None
        death_reader = -1
        death_writer = -1
        config_reader = -1
        config_writer = -1
        control_source = -1
        death_source = -1
        config_source = -1
        beat_fd = -1
        beat_source = -1
        pid = -1
        beat = self.beat_dir / f"slot-{gen}.beat"
        actor = f"slot-{gen}"

        async def discard_spawned_child() -> None:
            nonlocal parent_sock, death_writer, beat_fd, pid
            spawned_pid = pid
            if death_writer >= 0:
                with suppress(OSError):
                    os.close(death_writer)
                death_writer = -1
            if spawned_pid >= 0:
                kill_process_group(spawned_pid, signal.SIGKILL)
                while True:
                    try:
                        await asyncio.to_thread(reap, spawned_pid)
                    except InterruptedError:
                        continue
                    except ChildProcessError:
                        pass
                    break
            if parent_sock is not None:
                parent_sock.close()
                parent_sock = None
            if beat_fd >= 0:
                with suppress(OSError):
                    os.close(beat_fd)
                beat_fd = -1
            beat.unlink(missing_ok=True)
            pid = -1

        try:
            await self.hooks.checkpoint(RuntimeOperation.SOCKET_PAIR, actor=actor)
            parent_sock, child_sock = socket.socketpair()
            await self.hooks.checkpoint(RuntimeOperation.PIPE, actor=actor, purpose="death")
            death_reader, death_writer = os.pipe()
            await self.hooks.checkpoint(RuntimeOperation.PIPE, actor=actor, purpose="config")
            config_reader, config_writer = os.pipe()
            await self.hooks.checkpoint(RuntimeOperation.DUP_FD, actor=actor, purpose="control")
            control_source = fcntl.fcntl(child_sock.fileno(), fcntl.F_DUPFD_CLOEXEC, 10)
            await self.hooks.checkpoint(RuntimeOperation.DUP_FD, actor=actor, purpose="death")
            death_source = fcntl.fcntl(death_reader, fcntl.F_DUPFD_CLOEXEC, 10)
            await self.hooks.checkpoint(RuntimeOperation.DUP_FD, actor=actor, purpose="config")
            config_source = fcntl.fcntl(config_reader, fcntl.F_DUPFD_CLOEXEC, 10)
            await self.hooks.checkpoint(RuntimeOperation.HEARTBEAT_OPEN, actor=actor)
            beat_fd = os.open(
                beat,
                os.O_CREAT | os.O_RDWR | os.O_CLOEXEC,
                0o600,
            )
            initial_marker = bytes(8)
            injected_write = await self.hooks.checkpoint(
                RuntimeOperation.HEARTBEAT_WRITE,
                actor=actor,
                purpose="initialize",
                data=initial_marker,
            )
            if injected_write is None:
                initialized = os.pwrite(beat_fd, initial_marker, 0)
            elif isinstance(injected_write, int):
                initialized = injected_write
            else:
                raise TypeError("heartbeat write fault override must be an integer")
            if initialized != len(initial_marker):
                raise OSError("heartbeat marker initialization made no progress")
            if self.anonymous_beats:
                beat.unlink()
            await self.hooks.checkpoint(RuntimeOperation.DUP_FD, actor=actor, purpose="beat")
            beat_source = fcntl.fcntl(beat_fd, fcntl.F_DUPFD_CLOEXEC, 10)
            while config:
                injected_write = await self.hooks.checkpoint(
                    RuntimeOperation.CONFIG_WRITE,
                    actor=actor,
                    purpose="config",
                    data=config,
                )
                if injected_write is None:
                    written = os.write(config_writer, config)
                elif isinstance(injected_write, int):
                    written = os.write(config_writer, config[:injected_write])
                else:
                    raise TypeError("config write fault override must be an integer")
                if written <= 0:
                    raise BrokenPipeError("child configuration pipe made no progress")
                config = config[written:]
            os.close(config_writer)
            config_writer = -1
            child_env = os.environ.copy()
            child_env.pop("REAPER_POSTGRES_DSN", None)
            args = [
                sys.executable,
                "-m",
                "reaper.pool",
                CHILD_MODE,
            ]
            file_actions = [
                (os.POSIX_SPAWN_DUP2, control_source, CHILD_CONTROL_FD),
                (os.POSIX_SPAWN_DUP2, death_source, CHILD_DEATH_FD),
                (os.POSIX_SPAWN_DUP2, config_source, CHILD_CONFIG_FD),
                (os.POSIX_SPAWN_DUP2, beat_source, CHILD_BEAT_FD),
                (os.POSIX_SPAWN_CLOSE, control_source),
                (os.POSIX_SPAWN_CLOSE, death_source),
                (os.POSIX_SPAWN_CLOSE, config_source),
                (os.POSIX_SPAWN_CLOSE, beat_source),
            ]
            await self.hooks.checkpoint(RuntimeOperation.SPAWN_THREAD, actor=actor)
            await self.hooks.checkpoint(RuntimeOperation.SPAWN_PROCESS, actor=actor)
            spawning = asyncio.create_task(
                asyncio.to_thread(
                    os.posix_spawn,
                    sys.executable,
                    args,
                    child_env,
                    file_actions=file_actions,
                    setpgroup=0,
                    setsigmask={signal.SIGINT},
                )
            )
            try:
                pid = await asyncio.shield(spawning)
            except asyncio.CancelledError as cancellation:
                try:
                    pid = await spawning
                except BaseException as failure:
                    note_cleanup_failure(cancellation, failure, "cancelled process spawn")
                else:
                    try:
                        await shield_cleanup(discard_spawned_child())
                    except BaseException as failure:
                        note_cleanup_failure(
                            cancellation,
                            failure,
                            "cancelled process spawn",
                        )
                raise
            try:
                await self.hooks.checkpoint(
                    RuntimeOperation.SPAWN_PROCESS,
                    actor=actor,
                    phase="after",
                )
            except BaseException as error:
                try:
                    await shield_cleanup(discard_spawned_child())
                except BaseException as cleanup:
                    note_cleanup_failure(error, cleanup, "post-spawn failure")
                raise
        except Exception as fault:
            return fault
        finally:
            for descriptor in (
                control_source,
                death_source,
                config_source,
                beat_source,
                death_reader,
                config_reader,
                config_writer,
            ):
                if descriptor >= 0:
                    with suppress(OSError):
                        os.close(descriptor)
            if child_sock is not None:
                child_sock.close()
            if pid < 0:
                if parent_sock is not None:
                    parent_sock.close()
                if death_writer >= 0:
                    with suppress(OSError):
                        os.close(death_writer)
                if beat_fd >= 0:
                    with suppress(OSError):
                        os.close(beat_fd)
                beat.unlink(missing_ok=True)

        assert parent_sock is not None
        assert pid >= 0
        parent_sock.setblocking(False)
        identity = SkeletonID(fd=parent_sock.fileno(), gen=gen)
        slot = Slot(
            identity=identity,
            control=parent_sock,
            death_writer=death_writer,
            beat=beat,
            beat_fd=beat_fd,
            pid=pid,
            started_at=time.monotonic(),
        )
        self.slots[identity.fd] = slot
        await self.hooks.checkpoint(
            RuntimeMarker.SLOT_SPAWNED,
            actor=f"slot-{gen}",
            pid=pid,
            generation=gen,
        )
        write(
            log,
            logging.DEBUG,
            "skeleton spawned",
            role=self.role,
            pid=pid,
            fd=identity.fd,
            generation=gen,
        )
        slot.reader = loop.create_task(self.read_slot(slot))
        slot.reader.add_done_callback(lambda task: self.reader_done(slot, task))
        return slot

    def reader_done(self, slot: Slot, task: asyncio.Task[None]) -> None:
        """Route reader faults through normal slot loss."""

        if task.cancelled():
            return
        fault = task.exception()
        if not fault:
            return
        slot.fault_kind = f"{type(fault).__module__}.{type(fault).__qualname__}"
        slot.fault_text = str(fault)
        slot.fault_trace = "".join(traceback.format_exception(fault))
        write(
            log,
            logging.ERROR,
            "skeleton reader failed",
            role=self.role,
            skeleton=self.short_skeleton_id(slot.skeleton_id),
            pid=slot.pid,
            fd=slot.identity.fd,
            generation=slot.identity.gen,
            fault=slot.fault_kind,
            text=slot.fault_text,
            trace=slot.fault_trace,
        )
        self.get_loop().create_task(self.lose(slot))

    async def read_slot(self, slot: Slot) -> None:
        while self.slots.get(slot.identity.fd) is slot:
            message = await read_message(
                self.get_loop(),
                slot.control,
                self.hooks,
                actor=f"slot-{slot.identity.gen}",
                purpose="control",
            )
            if message is None:
                await self.lose(slot)
                return
            try:
                op = MessageOp(str(message.get("op")))
            except ValueError:
                slot.fault_kind = "ProtocolError"
                slot.fault_text = f"unknown skeleton message {message.get('op')!r}"
                await self.lose(slot)
                return
            match op:
                case MessageOp.READY:
                    self.pre_ready_failures = 0
                    self.clear_service_faults()
                    write(
                        log,
                        logging.INFO,
                        "skeleton ready",
                        role=self.role,
                        skeleton=self.short_skeleton_id(slot.skeleton_id),
                        pid=slot.pid,
                        generation=slot.identity.gen,
                    )
                    event = ControlEvent(kind=EventKind.READY, identity=slot.identity)
                    await self.drive(self.core.apply(event))
                case MessageOp.HEALTHY:
                    self.clear_service_faults()
                    slot.fault_kind = ""
                    slot.fault_text = ""
                    slot.fault_trace = ""
                    write(
                        log,
                        logging.INFO,
                        "skeleton service recovered",
                        role=self.role,
                        skeleton=self.short_skeleton_id(slot.skeleton_id),
                        pid=slot.pid,
                        generation=slot.identity.gen,
                    )
                case MessageOp.LIFECYCLE:
                    self.log_lifecycle(slot, message)
                case MessageOp.STATE:
                    continue
                case MessageOp.SERVICE_FAULT:
                    slot.fault_kind = str(message.get("kind", "Error"))
                    slot.fault_text = str(message.get("text", ""))
                    slot.fault_trace = str(message.get("trace", ""))
                    write(
                        log,
                        logging.ERROR,
                        "skeleton service failed",
                        role=self.role,
                        skeleton=self.short_skeleton_id(slot.skeleton_id),
                        pid=slot.pid,
                        generation=slot.identity.gen,
                        fault=slot.fault_kind,
                        text=slot.fault_text,
                        trace=slot.fault_trace,
                    )
                    self.record_service_fault(slot, message)
                case MessageOp.FATAL:
                    slot.fault_kind = str(message.get("kind", "Error"))
                    slot.fault_text = str(message.get("text", ""))
                    slot.fault_trace = str(message.get("trace", ""))
                case MessageOp.RESULT:
                    job = str(message["id"])
                    if slot.state is not SkeletonState.RUNNING or slot.job_id != job:
                        write(
                            log,
                            logging.WARNING,
                            "discarded stale skeleton result",
                            role=self.role,
                            skeleton=self.short_skeleton_id(slot.skeleton_id),
                            pid=slot.pid,
                            generation=slot.identity.gen,
                            expected=slot.job_id,
                            received=job,
                        )
                        continue
                    self.results[job] = message
                    event = ControlEvent(
                        kind=EventKind.RESULT,
                        identity=slot.identity,
                        job=job,
                        ok=message.get("ok") is True,
                    )
                    await self.drive(self.core.apply(event))

    def log_lifecycle(self, slot: Slot, message: Mapping[str, object]) -> None:
        """Log one child lifecycle effect at the parent boundary."""

        slot.skeleton_id = str(message.get("skeleton_id", slot.skeleton_id))
        slot.lifecycle_phase = SkeletonPhase(str(message.get("phase", slot.lifecycle_phase)))
        slot.listener_generation = int(
            cast(int | str, message.get("listener_generation", slot.listener_generation))
        )
        level = LOG_LEVEL[LifecycleLevel(str(message.get("level")))]
        event = LifecycleKind(str(message.get("event")))
        skeleton = self.short_skeleton_id(slot.skeleton_id)
        task_id = str(message.get("task_id", ""))
        version = int(cast(int | str, message.get("version", 0)))
        raw_outcome = str(message.get("outcome", ""))
        outcome: ResultState | WorkOutcome | None = None
        if raw_outcome:
            try:
                outcome = ResultState(raw_outcome)
            except ValueError:
                outcome = WorkOutcome(raw_outcome)
        raw_release_reason = str(message.get("release_reason", ""))
        release_reason = TaskReleaseReason(raw_release_reason) if raw_release_reason else None
        count = int(cast(int | str, message.get("count", 0)))
        detail = str(message.get("detail", ""))
        match event:
            case LifecycleKind.LISTENER_RECYCLED:
                write(
                    log,
                    level,
                    "listener recycled",
                    skeleton=skeleton,
                    generation=slot.listener_generation,
                )
                return
            case LifecycleKind.TASK_CLAIMED:
                write(
                    log,
                    level,
                    "task claimed",
                    id=task_id,
                    version=version,
                    skeleton=skeleton,
                )
                return
            case LifecycleKind.TASK_COMMITTED:
                write(
                    log,
                    level,
                    "task committed",
                    id=task_id,
                    version=version,
                    outcome=outcome,
                    skeleton=skeleton,
                )
                return
            case LifecycleKind.TASK_RELEASED:
                write(
                    log,
                    level,
                    "task released",
                    id=task_id,
                    version=version,
                    reason=release_reason,
                    skeleton=skeleton,
                )
                return
            case LifecycleKind.GC_FINISHED:
                write(
                    log,
                    level,
                    "promise garbage collection finished",
                    removed=count,
                    skeleton=skeleton,
                )
                return
        write(
            log,
            level,
            "skeleton lifecycle",
            event=event,
            phase=slot.lifecycle_phase,
            listener=ListenerPhase(str(message.get("listener"))),
            listener_generation=slot.listener_generation,
            role=self.role,
            skeleton=slot.skeleton_id,
            pid=slot.pid,
            fd=slot.identity.fd,
            generation=slot.identity.gen,
            task=task_id,
            version=version,
            outcome=outcome,
            release_reason=release_reason,
            count=count,
            detail=detail,
        )

    @staticmethod
    def short_skeleton_id(skeleton_id: str) -> str:
        """Keep logs readable while preserving the full skeleton identity elsewhere."""

        return skeleton_id.removeprefix("skeleton-")[:12]

    def clear_service_faults(self) -> None:
        """Clear one consecutive pool fault run."""

        self.service_fault = ""
        self.service_fault_slots.clear()
        self.service_fault_round = 0

    def record_service_fault(
        self,
        slot: Slot,
        message: Mapping[str, object],
    ) -> None:
        """Open a circuit for persistent service faults."""

        kind = slot.fault_kind
        code = str(message.get("code", ""))
        signature = f"{kind}:{code}"
        if permanent_service_fault(code):
            self.open_circuit(f"permanent skeleton service fault {signature}")
            return
        if transient_service_fault(kind, code):
            return
        if signature != self.service_fault:
            self.service_fault = signature
            self.service_fault_slots.clear()
            self.service_fault_round = 0
        self.service_fault_slots.add(slot.identity.gen)
        if len(self.service_fault_slots) < self.target:
            return
        self.service_fault_slots.clear()
        self.service_fault_round += 1
        if self.service_fault_round >= self.service_failure_rounds:
            self.open_circuit(
                f"skeleton service fault {signature} repeated for "
                f"{self.service_fault_round} pool rounds"
            )

    def open_circuit(self, reason: str) -> None:
        """Stop a pool after a permanent fault."""

        if self.failure_event.is_set():
            return
        error = RuntimeError(reason)
        self.startup_error = error
        self.failure_errors.append(error)
        self.failure_event.set()
        write(log, logging.ERROR, "pool circuit opened", role=self.role, reason=reason)

    async def lose(self, slot: Slot) -> None:
        """Refill the slot after its FD dies."""

        if self.slots.get(slot.identity.fd) is not slot:
            return
        pre_ready = slot.state is SkeletonState.STARTING
        event = ControlEvent(kind=EventKind.EOF, identity=slot.identity)
        level = logging.DEBUG if self.closing or slot.expected_stop else logging.WARNING
        write(
            log,
            level,
            "skeleton connection lost",
            role=self.role,
            skeleton=self.short_skeleton_id(slot.skeleton_id),
            pid=slot.pid,
            generation=slot.identity.gen,
        )
        await self.drive(self.core.apply(event))
        self.schedule_recovery(pre_ready=pre_ready)

    def schedule_recovery(self, *, pre_ready: bool) -> None:
        """Refill missing capacity after a bounded exponential delay."""

        if self.closing:
            return
        if pre_ready:
            self.pre_ready_failures += 1
            if self.pre_ready_failures >= MAX_PRE_READY_FAILURES:
                self.startup_error = RuntimeError(
                    f"skeleton failed to become ready after {self.pre_ready_failures} attempts"
                )
                if self.recovery_task is not None:
                    self.recovery_task.cancel()
                    self.recovery_task = None
                write(
                    log,
                    logging.ERROR,
                    "skeleton recovery circuit opened",
                    role=self.role,
                    failures=self.pre_ready_failures,
                )
                return
        if self.recovery_task is not None and not self.recovery_task.done():
            return
        exponent = max(0, self.pre_ready_failures - 1)
        delay = min(RECOVERY_BASE_DELAY * (2**exponent), RECOVERY_MAX_DELAY)
        self.recovery_task = self.get_loop().create_task(self.recover_after(delay))

    async def recover_after(self, delay: float) -> None:
        """Ask the reducer to restore capacity after backoff."""

        await asyncio.sleep(delay)
        self.recovery_task = None
        if not self.closing:
            await self.drive(self.core.apply(ControlEvent(kind=EventKind.RECOVER)))

    async def run_sync[ValueT](
        self,
        fn: Callable[..., ValueT],
        *args: object,
        **kwargs: object,
    ) -> ValueT:
        return await self.submit(WorkerKind.SYNC, fn, args, kwargs)

    async def run_async[ValueT](
        self,
        fn: Callable[..., Coroutine[object, object, ValueT]],
        *args: object,
        **kwargs: object,
    ) -> ValueT:
        return await self.submit(WorkerKind.ASYNC, fn, args, kwargs)

    async def run_bash(
        self,
        script: str,
        *,
        env: Mapping[str, str] | None = None,
    ) -> str:
        data: dict[str, object] = {"env": dict(env or {})}
        return await self.submit(WorkerKind.BASH, script, (), data)

    async def submit[ValueT](
        self,
        kind: WorkerKind,
        target: object,
        args: Sequence[object],
        kwargs: Mapping[str, object],
    ) -> ValueT:
        """Queue work when all slots are busy."""

        if self.closing:
            raise RuntimeError("SkeletonPool is now shut")
        if not self.started:
            await self.start()
        if len(self.core.queue) >= self.queue_limit:
            raise RuntimeError("SkeletonPool work queue is full")
        loop = self.get_loop()
        self.job_sequence += 1
        job = f"{self.job_sequence:x}-{self.id_source()}"
        work = Work(
            id=job,
            kind=kind,
            target=target,
            args=tuple(args),
            options=dict(kwargs),
        )
        pickle.dumps(work, protocol=5)
        future: asyncio.Future[ValueT] = loop.create_future()
        pending: Pending[ValueT] = Pending(work=work, future=future)
        self.jobs[job] = cast(Pending[object], pending)
        write(log, logging.INFO, "work submitted", kind=kind, id=job)
        event = ControlEvent(kind=EventKind.SUBMIT, job=job)
        await self.drive(self.core.apply(event))
        return await future

    async def close(self, *, timeout: float = 5.0) -> None:
        """Finish process teardown before propagating caller cancellation."""

        if not self.started:
            return
        if self.close_task is None:
            self.closing = True
            self.close_task = self.get_loop().create_task(self.finish_close(timeout))
        closing = self.close_task
        try:
            await shield_cleanup(closing)
        finally:
            if closing.done() and self.close_task is closing:
                self.close_task = None

    async def finish_close(self, timeout: float) -> None:
        """Perform the single shared teardown operation for all close callers."""

        write(log, logging.INFO, "pool stopping", role=self.role)
        await self.hooks.checkpoint(RuntimeMarker.SHUTDOWN_STARTED, actor=self.role.value)
        if self.heartbeat_task is not None:
            self.heartbeat_task.cancel()
            await asyncio.gather(self.heartbeat_task, return_exceptions=True)
            self.heartbeat_task = None
        if self.recovery_task is not None:
            self.recovery_task.cancel()
            await asyncio.gather(self.recovery_task, return_exceptions=True)
            self.recovery_task = None
        await self.drive(self.core.apply(ControlEvent(kind=EventKind.CLOSE)))
        readers = [slot.reader for slot in self.slots.values() if slot.reader]
        if readers:
            wait_set = await asyncio.wait(readers, timeout=timeout)
            for task in wait_set[1]:
                task.cancel()
        await self.drive(self.core.apply(ControlEvent(kind=EventKind.DEADLINE)))
        for slot in tuple(self.slots.values()):
            event = ControlEvent(kind=EventKind.EOF, identity=slot.identity)
            await self.drive(self.core.apply(event))
        self.slots.clear()
        self.jobs.clear()
        self.started = False
        if self.temp_dir:
            self.temp_dir.cleanup()
        for pool in self.child_pools:
            await pool.close(timeout=timeout)

    def get_loop(self) -> asyncio.AbstractEventLoop:
        if self.loop is None:
            raise RuntimeError("SkeletonPool has not begun")
        return self.loop

    async def monitor_heartbeats(self) -> None:
        """Kill a skeleton only after timely probes repeatedly see one marker."""

        timeout = max(
            self.beat_rate * HEARTBEAT_MISSES,
            HEARTBEAT_MIN_TIMEOUT,
        )
        while self.started and not self.closing:
            await asyncio.sleep(self.beat_rate)
            now = self.get_loop().time()
            for slot in tuple(self.slots.values()):
                if slot.beat_fd < 0:
                    continue
                try:
                    injected = await self.hooks.checkpoint(
                        RuntimeOperation.HEARTBEAT_READ,
                        actor=f"slot-{slot.identity.gen}",
                        purpose="read",
                        data=bytes(8),
                    )
                    if injected is None:
                        marker = os.pread(slot.beat_fd, 8, 0)
                    elif isinstance(injected, bytes):
                        marker = injected
                    else:
                        raise TypeError("heartbeat fault override must be bytes")
                    if len(marker) != 8:
                        raise OSError(f"heartbeat marker read {len(marker)} of 8 bytes")
                except OSError as error:
                    alive = slot.heartbeat.observe(
                        slot.heartbeat.marker,
                        now,
                        timeout,
                    )
                    fault_kind = "HeartbeatReadError"
                    fault_text = str(error)
                else:
                    alive = slot.heartbeat.observe(marker, now, timeout)
                    fault_kind = "HeartbeatTimeout"
                    fault_text = "skeleton heartbeat marker stopped changing"
                if alive:
                    continue
                slot.heartbeat.unchanged_since = now
                slot.fault_kind = fault_kind
                slot.fault_text = fault_text
                write(
                    log,
                    logging.ERROR,
                    "skeleton heartbeat stopped",
                    role=self.role,
                    skeleton=self.short_skeleton_id(slot.skeleton_id),
                    pid=slot.pid,
                    generation=slot.identity.gen,
                )
                with suppress(OSError):
                    kill_process_group(slot.pid, signal.SIGKILL)

    async def drive(self, effects: Sequence[ControlEffect]) -> None:
        pending = list(effects)
        while pending:
            effect = pending.pop(0)
            match effect.kind:
                case EffectKind.SPAWN if effect.gen is not None:
                    spawned = await self.spawn_effect(effect.gen)
                    if isinstance(spawned, BaseException):
                        trace = "".join(traceback.format_exception(spawned))
                        write(
                            log,
                            logging.ERROR,
                            "skeleton spawn failed",
                            role=self.role,
                            generation=effect.gen,
                            fault=type(spawned).__qualname__,
                            trace=trace,
                        )
                        event = ControlEvent(
                            kind=EventKind.SPAWN_FAILED,
                            gen=effect.gen,
                        )
                        pending.extend(self.core.apply(event))
                        self.schedule_recovery(pre_ready=True)
                        continue
                    spawn_slot = cast(Slot, spawned)
                    event = ControlEvent(
                        kind=EventKind.SPAWNED,
                        identity=spawn_slot.identity,
                        gen=effect.gen,
                    )
                    pending.extend(self.core.apply(event))
                case EffectKind.SEND_RUN:
                    self.get_loop().create_task(self.send_run(effect))
                case EffectKind.SEND_STOP:
                    stop_slot = self.slot_for(effect.identity)
                    if stop_slot:
                        stop_slot.expected_stop = True
                    self.get_loop().create_task(self.send_stop(effect))
                case EffectKind.CLOSE_DEATH:
                    life_slot = self.slot_for(effect.identity)
                    if life_slot and life_slot.death_writer >= 0:
                        with suppress(OSError):
                            await self.hooks.checkpoint(
                                RuntimeOperation.CLOSE_FD,
                                actor=f"slot-{life_slot.identity.gen}",
                                purpose="death",
                            )
                        try:
                            os.close(life_slot.death_writer)
                        except OSError:
                            pass
                        finally:
                            life_slot.death_writer = -1
                case EffectKind.CLOSE_CONTROL:
                    control_slot = self.slot_for(effect.identity)
                    if control_slot:
                        with suppress(OSError):
                            await self.hooks.checkpoint(
                                RuntimeOperation.CLOSE_FD,
                                actor=f"slot-{control_slot.identity.gen}",
                                purpose="control",
                            )
                        control_slot.control.close()
                case EffectKind.RESOLVE:
                    self.resolve(effect)
                case EffectKind.FAIL:
                    self.fail(effect)
                case EffectKind.DROP:
                    drop_slot = self.slot_for(effect.identity)
                    if drop_slot:
                        reader = drop_slot.reader
                        current = asyncio.current_task()
                        if reader is not None and reader is not current and not reader.done():
                            reader.cancel()
                            await asyncio.gather(reader, return_exceptions=True)
                        self.drop(drop_slot)
                case EffectKind.REAP:
                    reap_slot = self.slot_for(effect.identity)
                    if reap_slot:
                        child_missing = False
                        reaped_real_child = False
                        injected_status: object | None = None
                        try:
                            injected_status = await self.hooks.checkpoint(
                                RuntimeOperation.WAIT_PROCESS,
                                actor=f"slot-{reap_slot.identity.gen}",
                            )
                        except ChildProcessError:
                            child_missing = True
                        except InterruptedError:
                            pass
                        if child_missing:
                            status = None
                        elif isinstance(injected_status, int) and not isinstance(
                            injected_status,
                            bool,
                        ):
                            status = ExitStatus(
                                pid=reap_slot.pid,
                                exit_code=injected_status,
                                signal=0,
                            )
                        elif injected_status is not None:
                            raise TypeError("wait fault override must be an exit code")
                        else:
                            try:
                                status = await asyncio.to_thread(reap, reap_slot.pid)
                                reaped_real_child = True
                            except ChildProcessError:
                                status = None
                        if status is not None:
                            self.log_exit(reap_slot, status)
                        if reaped_real_child:
                            try:
                                kill_process_group(reap_slot.pid, signal.SIGKILL)
                            except OSError as error:
                                self.open_circuit(
                                    "cannot terminate descendants for skeleton process group "
                                    f"{reap_slot.pid}: {type(error).__qualname__}: {error}"
                                )
                        with suppress(InterruptedError, ChildProcessError):
                            await self.hooks.checkpoint(
                                RuntimeOperation.WAIT_PROCESS,
                                actor=f"slot-{reap_slot.identity.gen}",
                                phase="after",
                            )
                        if reap_slot.beat_fd >= 0:
                            with suppress(OSError):
                                os.close(reap_slot.beat_fd)
                            reap_slot.beat_fd = -1
                        reap_slot.beat.unlink(missing_ok=True)
                        key = (reap_slot.identity.fd, reap_slot.identity.gen)
                        self.retired_slots.pop(key, None)
                case EffectKind.KILL:
                    kill_slot = self.slot_for(effect.identity)
                    if kill_slot:
                        kill_slot.expected_stop = True
                        process_missing = False
                        try:
                            await self.hooks.checkpoint(
                                RuntimeOperation.KILL,
                                actor=f"slot-{kill_slot.identity.gen}",
                            )
                        except ProcessLookupError:
                            process_missing = True
                        except InterruptedError:
                            pass
                        if not process_missing:
                            kill_process_group(kill_slot.pid, signal.SIGKILL)
                case _:
                    continue
            self.sync_slots()
        self.sync_slots()

    def log_exit(self, slot: Slot, status: ExitStatus) -> None:
        """Log the final child process state."""

        if slot.expected_stop and status.exit_code == 0:
            write(
                log,
                logging.INFO,
                "skeleton stopped",
                role=self.role,
                skeleton=self.short_skeleton_id(slot.skeleton_id),
                pid=status.pid,
                uptime=round(time.monotonic() - slot.started_at, 3),
            )
            return

        tags: dict[str, object] = {
            "role": self.role,
            "skeleton": slot.skeleton_id,
            "pid": status.pid,
            "fd": slot.identity.fd,
            "generation": slot.identity.gen,
            "exit_code": status.exit_code,
            "signal": status.signal,
            "expected": slot.expected_stop,
            "uptime": round(time.monotonic() - slot.started_at, 3),
            "job": slot.job_id,
            "fault": slot.fault_kind,
            "text": slot.fault_text,
            "trace": slot.fault_trace,
        }
        if slot.expected_stop and (not status.exit_code or status.signal):
            write(log, logging.INFO, "skeleton exited", **tags)
            return
        write(log, logging.ERROR, "skeleton exited unexpectedly", **tags)

    async def send_run(self, effect: ControlEffect) -> None:
        slot = self.slot_for(effect.identity)
        pending = self.jobs.get(effect.job or "")
        if slot is None or pending is None:
            return
        slot.job_id = pending.work.id
        note: dict[str, object] = {"op": MessageOp.RUN.value, "work": pending.work}
        results = await asyncio.gather(
            self.send_control(slot, note),
            return_exceptions=True,
        )
        fault = results[0]
        if isinstance(fault, BaseException):
            slot.fault_kind = f"{type(fault).__module__}.{type(fault).__qualname__}"
            slot.fault_text = str(fault)
            slot.fault_trace = "".join(traceback.format_exception(fault))
            write(
                log,
                logging.ERROR,
                "send to skeleton failed",
                role=self.role,
                skeleton=self.short_skeleton_id(slot.skeleton_id),
                pid=slot.pid,
                generation=slot.identity.gen,
                fault=slot.fault_kind,
                text=slot.fault_text,
                trace=slot.fault_trace,
            )
            event = ControlEvent(
                kind=EventKind.SEND_FAILED,
                identity=slot.identity,
                job=effect.job,
            )
            await self.drive(self.core.apply(event))
            self.schedule_recovery(pre_ready=False)

    async def send_stop(self, effect: ControlEffect) -> None:
        slot = self.slot_for(effect.identity)
        if slot is None:
            return
        await asyncio.gather(
            self.send_control(slot, {"op": MessageOp.STOP.value}),
            return_exceptions=True,
        )

    async def send_control(self, slot: Slot, note: Mapping[str, object]) -> None:
        """Send one control message through an injectable boundary."""

        await self.hooks.checkpoint(
            RuntimeOperation.CONTROL_SEND,
            actor=f"slot-{slot.identity.gen}",
            purpose="control",
        )
        await send_message(self.get_loop(), slot.control, note)
        await self.hooks.checkpoint(
            RuntimeOperation.CONTROL_SEND,
            actor=f"slot-{slot.identity.gen}",
            purpose="control",
            phase="after",
        )

    def resolve(self, effect: ControlEffect) -> None:
        job = effect.job or ""
        pending = self.jobs.pop(job, None)
        note = self.results.pop(job, {})
        if pending and not pending.future.done():
            pending.future.set_result(note.get("value"))

    def fail(self, effect: ControlEffect) -> None:
        job = effect.job or ""
        pending = self.jobs.pop(job, None)
        note = self.results.pop(job, {})
        if pending is None or pending.future.done():
            return
        error: BaseException
        if effect.reason is FailureReason.WORKER_ERROR:
            error = RemoteWorkerError(
                str(note.get("kind", "Error")),
                str(note.get("text", "")),
                str(note.get("trace", "")),
            )
        elif effect.reason is FailureReason.CLOSED:
            error = asyncio.CancelledError("SkeletonPool is shut")
        else:
            error = RemoteWorkerError("ChildDied", "lost child EOF", "")
        pending.future.set_exception(error)

    def drop(self, slot: Slot) -> None:
        if self.slots.get(slot.identity.fd) is slot:
            del self.slots[slot.identity.fd]
        key = (slot.identity.fd, slot.identity.gen)
        self.retired_slots[key] = slot
        slot.state = SkeletonState.DEAD

    def slot_for(self, identity: SkeletonID | None) -> Slot | None:
        if identity is None:
            return None
        slot = self.slots.get(identity.fd)
        if slot and slot.identity == identity:
            return slot
        return self.retired_slots.get((identity.fd, identity.gen))

    def sync_slots(self) -> None:
        for fd, slot in self.slots.items():
            core_slot = self.core.slots.get(fd)
            if core_slot and core_slot.identity == slot.identity:
                slot.state = core_slot.state
                slot.job_id = core_slot.job


def skeleton_main(
    control: socket.socket,
    death_reader: int,
    beat_fd: int,
    config: ChildConfig,
) -> int:
    """Run one warm child."""

    # The daemon owns terminal interrupts and tells skeletons to stop over the
    # control socket.  Ignore SIGINT here so one Ctrl-C does not produce a
    # traceback from every process in the foreground group.  spawn_effect()
    # blocks it during interpreter startup, closing the race before this point.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
    if config.cgroup_path is not None:
        Cgroup(path=config.cgroup_path).join()
    event_loop_type = load_loop_type(config.event_loop_type)
    loop = event_loop_type() if event_loop_type else asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    control.setblocking(False)
    skeleton_id = new_worker_id()
    lifecycle_core = SkeletonCore()

    async def lifecycle(event: LifecycleEvent) -> None:
        for effect in lifecycle_core.apply(event):
            await send_message(
                loop,
                control,
                {
                    "op": MessageOp.LIFECYCLE.value,
                    "skeleton_id": skeleton_id,
                    "level": effect.level.value,
                    "event": effect.event.value,
                    "phase": effect.phase.value,
                    "listener": effect.listener.value,
                    "listener_generation": effect.listener_generation,
                    "task_id": effect.task_id,
                    "version": effect.version,
                    "outcome": effect.outcome.value if effect.outcome is not None else "",
                    "release_reason": (
                        effect.release_reason.value if effect.release_reason is not None else ""
                    ),
                    "count": effect.count or 0,
                    "detail": effect.detail,
                },
            )

    ending = threading.Event()
    watcher = threading.Thread(
        target=watch_parent,
        args=(death_reader, ending),
        daemon=True,
        name="reaper-parent-watch",
    )
    watcher.start()
    match config.role:
        case SkeletonRole.GENERAL:
            child = child_loop(loop, control, beat_fd, config.beat_rate, lifecycle)
        case SkeletonRole.TASK:
            assert config.postgres_dsn is not None
            assert config.topic is not None
            postgres_dsn = str(config.postgres_dsn)
            topic = config.topic
            child = service_child_loop(
                loop,
                control,
                beat_fd,
                config.beat_rate,
                lambda ready, report: poll_tasks(
                    postgres_dsn,
                    topic,
                    config.poll_rate,
                    ready,
                    config.listener_probe_rate,
                    config.listener_recycle_rate,
                    config.retention_ms,
                    report,
                ),
                config.role,
                lifecycle,
                config.service_retry_base,
                config.service_retry_max,
            )
        case SkeletonRole.MAINTENANCE:
            assert config.postgres_dsn is not None
            postgres_dsn = str(config.postgres_dsn)
            child = service_child_loop(
                loop,
                control,
                beat_fd,
                config.beat_rate,
                lambda ready, report: poll_maintenance(
                    postgres_dsn,
                    config.maintenance_rate,
                    ready,
                    config.gc_rate,
                    config.retention_ms,
                    report,
                ),
                config.role,
                lifecycle,
                config.service_retry_base,
                config.service_retry_max,
            )
    exit_code = loop.run_until_complete(run_child(child, control, config.role))
    ending.set()
    os.close(death_reader)
    os.close(beat_fd)
    control.close()
    loop.close()
    return exit_code


def watch_parent(death_reader: int, ending: threading.Event) -> None:
    """End this child when its parent pipe closes."""

    while not ending.is_set():
        try:
            parent_signal = os.read(death_reader, 1)
        except InterruptedError:
            continue
        if parent_signal:
            continue
        if not ending.is_set():
            kill_process_group(os.getpgrp(), signal.SIGTERM)
        return


async def child_loop(
    loop: asyncio.AbstractEventLoop,
    control: socket.socket,
    beat_fd: int,
    beat_rate: float,
    lifecycle: LifecycleReporter,
) -> None:
    """Run general work under the skeleton lifecycle reducer."""

    beat_task = loop.create_task(touch_beat(beat_fd, beat_rate))
    await lifecycle(LifecycleEvent(kind=LifecycleKind.START))
    await lifecycle(LifecycleEvent(kind=LifecycleKind.READY))
    await send_message(loop, control, {"op": MessageOp.READY.value})
    try:
        running = True
        while running:
            note = await read_message(loop, control)
            if note is None:
                running = False
                continue
            op = MessageOp(str(note.get("op")))
            if op is MessageOp.STOP:
                running = False
                continue
            if op is not MessageOp.RUN:
                running = False
                continue
            work = cast(Work, note["work"])
            await lifecycle(LifecycleEvent(kind=LifecycleKind.WORK_STARTED, task_id=work.id))
            await send_message(
                loop,
                control,
                {"op": MessageOp.STATE.value, "state": SkeletonState.RUNNING.value},
            )
            result = await perform(work)
            await lifecycle(
                LifecycleEvent(
                    kind=LifecycleKind.WORK_FINISHED,
                    task_id=work.id,
                    outcome=(
                        WorkOutcome.SUCCEEDED if result.get("ok") is True else WorkOutcome.FAILED
                    ),
                )
            )
            await send_message(loop, control, result)
    finally:
        await lifecycle(LifecycleEvent(kind=LifecycleKind.STOP))
        beat_task.cancel()
        await asyncio.gather(beat_task, return_exceptions=True)
        await lifecycle(LifecycleEvent(kind=LifecycleKind.STOPPED))


async def service_child_loop(
    loop: asyncio.AbstractEventLoop,
    control: socket.socket,
    beat_fd: int,
    beat_rate: float,
    service: Callable[
        [Callable[[], None], LifecycleReporter],
        Coroutine[object, object, None],
    ],
    role: SkeletonRole,
    lifecycle: LifecycleReporter,
    retry_base: float = 0.25,
    retry_max: float = 30.0,
) -> None:
    """Run one resident skeleton service."""

    beat_task = loop.create_task(touch_beat(beat_fd, beat_rate))
    stop_task = loop.create_task(read_message(loop, control))
    backoff = retry_base
    announced = False
    while not stop_task.done():
        await lifecycle(LifecycleEvent(kind=LifecycleKind.START))
        healthy = asyncio.Event()
        healthy_task = loop.create_task(healthy.wait())
        service_task = loop.create_task(service(healthy.set, lifecycle))
        done, _ = await asyncio.wait(
            {healthy_task, service_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            healthy_task.cancel()
            service_task.cancel()
            await asyncio.gather(healthy_task, service_task, return_exceptions=True)
            break
        if healthy_task in done:
            op = MessageOp.HEALTHY if announced else MessageOp.READY
            await send_message(loop, control, {"op": op.value})
            announced = True
            backoff = retry_base
            done, _ = await asyncio.wait(
                {service_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task in done:
                service_task.cancel()
                await asyncio.gather(service_task, return_exceptions=True)
                break
        else:
            healthy_task.cancel()
            await asyncio.gather(healthy_task, return_exceptions=True)
        result = (await asyncio.gather(service_task, return_exceptions=True))[0]
        fault = (
            result
            if isinstance(result, BaseException)
            else RuntimeError("resident service stopped")
        )
        await lifecycle(
            LifecycleEvent(
                kind=LifecycleKind.FAULT,
                detail=f"{type(fault).__qualname__}: {fault}",
            )
        )
        trace = "".join(traceback.format_exception(fault))
        code = str(fault.sqlstate or "") if isinstance(fault, asyncpg.PostgresError) else ""
        sent = await asyncio.gather(
            send_message(
                loop,
                control,
                {
                    "op": MessageOp.SERVICE_FAULT.value,
                    "kind": f"{type(fault).__module__}.{type(fault).__qualname__}",
                    "text": str(fault),
                    "trace": trace,
                    "code": code,
                },
            ),
            return_exceptions=True,
        )
        if isinstance(sent[0], BaseException):
            write(
                log,
                logging.ERROR,
                "skeleton service failed",
                role=role,
                pid=os.getpid(),
                fault=type(fault).__qualname__,
                text=str(fault),
                trace=trace,
            )
        delay_task = loop.create_task(asyncio.sleep(backoff))
        done, _ = await asyncio.wait(
            {delay_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            delay_task.cancel()
            await asyncio.gather(delay_task, return_exceptions=True)
            break
        backoff = min(backoff * 2, retry_max)
    stop_result = (await asyncio.gather(stop_task, return_exceptions=True))[0]
    beat_task.cancel()
    await asyncio.gather(beat_task, return_exceptions=True)
    await lifecycle(LifecycleEvent(kind=LifecycleKind.STOP))
    await lifecycle(LifecycleEvent(kind=LifecycleKind.STOPPED))
    if isinstance(stop_result, BaseException):
        raise stop_result


async def run_child(
    child: Coroutine[object, object, None],
    control: socket.socket,
    role: SkeletonRole,
) -> int:
    """Report uncaught child faults before exit."""

    result = (await asyncio.gather(child, return_exceptions=True))[0]
    if not isinstance(result, BaseException):
        return 0
    trace = "".join(traceback.format_exception(result))
    note: dict[str, object] = {
        "op": MessageOp.FATAL.value,
        "kind": f"{type(result).__module__}.{type(result).__qualname__}",
        "text": str(result),
        "trace": trace,
    }
    sent = await asyncio.gather(
        send_message(asyncio.get_running_loop(), control, note),
        return_exceptions=True,
    )
    if isinstance(sent[0], BaseException):
        write(
            log,
            logging.ERROR,
            "skeleton failed fatally",
            role=role,
            pid=os.getpid(),
            fault=type(result).__qualname__,
            text=str(result),
            trace=trace,
        )
    return 1


async def perform(work: Work) -> dict[str, object]:
    match work.kind:
        case WorkerKind.SYNC:
            fn = cast(Callable[..., object], work.target)
            call = asyncio.to_thread(fn, *work.args, **work.options)
        case WorkerKind.ASYNC:
            fn = cast(Callable[..., Coroutine[object, object, object]], work.target)
            call = fn(*work.args, **work.options)
        case WorkerKind.BASH:
            env = os.environ.copy()
            env.update(cast(Mapping[str, str], work.options.get("env", {})))
            call = run_bash(str(work.target), env)
        case _:
            raise RuntimeError(f"bad job kind {work.kind}")
    items = await asyncio.gather(call, return_exceptions=True)
    item = items[0]
    if isinstance(item, BaseException):
        return {
            "op": MessageOp.RESULT.value,
            "id": work.id,
            "ok": False,
            "kind": f"{type(item).__module__}.{type(item).__qualname__}",
            "text": str(item),
            "trace": "".join(traceback.format_exception(item)),
        }
    return {
        "op": MessageOp.RESULT.value,
        "id": work.id,
        "ok": True,
        "value": item,
    }


async def run_bash(script: str, env: Mapping[str, str]) -> str:
    proc = await asyncio.create_subprocess_exec(
        WorkerKind.BASH.value,
        "-lc",
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode:
        text = stderr.decode(errors="replace").strip()
        raise RuntimeError(f"bash got code {proc.returncode}: {text}")
    return stdout.decode()


def write_beat(
    fd: int,
    marker: bytes | None = None,
    written: int | None = None,
) -> None:
    """Publish a changing monotonic marker without relying on wall-clock time."""

    marker = marker if marker is not None else time.monotonic_ns().to_bytes(8, "big")
    progress = os.pwrite(fd, marker, 0) if written is None else written
    if progress != len(marker):
        raise OSError("heartbeat marker write made no progress")


async def touch_beat(
    fd: int,
    rate: float,
    hooks: RuntimeHooks | None = None,
    actor: str = "skeleton",
) -> None:
    runtime_hooks = hooks or NullRuntimeHooks()
    while True:
        marker = time.monotonic_ns().to_bytes(8, "big")
        injected_write = await runtime_hooks.checkpoint(
            RuntimeOperation.HEARTBEAT_WRITE,
            actor=actor,
            purpose="publish",
            data=marker,
        )
        if injected_write is not None and not isinstance(injected_write, int):
            raise TypeError("heartbeat write fault override must be an integer")
        write_beat(fd, marker, injected_write)
        await asyncio.sleep(rate)


SOCKET_LOCKS: weakref.WeakKeyDictionary[socket.socket, asyncio.Lock] = weakref.WeakKeyDictionary()


def encode_message(value: object) -> bytes:
    """Encode one bounded IPC frame."""

    data = pickle.dumps(value, protocol=5)
    if len(data) > MAX_MSG:
        raise ValueError("IPC note is too big")
    return HEADER.pack(len(data)) + data


async def send_message(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
    value: object,
) -> None:
    lock = SOCKET_LOCKS.get(sock)
    if lock is None:
        lock = asyncio.Lock()
        SOCKET_LOCKS[sock] = lock
    writer = FramedWriter(
        transport=SocketTransport(loop=loop, sock=sock),
        lock=lock,
    )
    await writer.send(value)


async def read_message(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
    hooks: RuntimeHooks | None = None,
    *,
    actor: str = "runtime",
    purpose: str = "",
) -> Mapping[str, object] | None:
    runtime_hooks = hooks or NullRuntimeHooks()
    head = await read_bytes(
        loop,
        sock,
        HEADER.size,
        runtime_hooks,
        actor=actor,
        purpose=purpose,
    )
    if head is None:
        return None
    size = HEADER.unpack(head)[0]
    if size > MAX_MSG:
        return None
    data = await read_bytes(
        loop,
        sock,
        size,
        runtime_hooks,
        actor=actor,
        purpose=purpose,
    )
    if data is None:
        return None
    value = pickle.loads(data)
    if not isinstance(value, Mapping):
        return None
    return cast(Mapping[str, object], value)


async def read_bytes(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
    size: int,
    hooks: RuntimeHooks | None = None,
    *,
    actor: str = "runtime",
    purpose: str = "",
) -> bytes | None:
    runtime_hooks = hooks or NullRuntimeHooks()
    data = bytearray()
    while len(data) < size:
        remaining = size - len(data)
        injected_read = await runtime_hooks.checkpoint(
            RuntimeOperation.CONTROL_RECEIVE,
            actor=actor,
            purpose=purpose,
            data=bytes(min(remaining, 64 * 1024)),
        )
        if injected_read is None:
            requested = remaining
        elif isinstance(injected_read, int):
            requested = min(remaining, injected_read)
        else:
            raise TypeError("receive fault override must be an integer")
        if requested <= 0:
            return None
        part = await loop.sock_recv(sock, requested)
        await runtime_hooks.checkpoint(
            RuntimeOperation.CONTROL_RECEIVE,
            actor=actor,
            purpose=purpose,
            phase="after",
            data=part,
        )
        if not part:
            return None
        data.extend(part)
    return bytes(data)


def reap(pid: int) -> ExitStatus:
    waited_pid, status = os.waitpid(pid, 0)
    assert waited_pid == pid
    return ExitStatus(
        pid=waited_pid,
        exit_code=os.waitstatus_to_exitcode(status),
        signal=os.WTERMSIG(status) if os.WIFSIGNALED(status) else 0,
    )


def kill_process_group(pid: int, sig: int) -> bool:
    """Signal one skeleton tree, retrying only unambiguous interruptions."""

    while True:
        try:
            os.killpg(pid, sig)
        except InterruptedError:
            continue
        except ProcessLookupError:
            return False
        return True


def permanent_service_fault(code: str) -> bool:
    """Classify fixed PostgreSQL faults."""

    return code.startswith(PERMANENT_SQLSTATE_CLASSES) or code in PERMANENT_SQLSTATES


def transient_service_fault(kind: str, code: str) -> bool:
    """Classify faults that may heal."""

    transient_kind = kind.rpartition(".")[2] in TRANSIENT_EXCEPTION_NAMES
    return transient_kind or code.startswith(TRANSIENT_SQLSTATE_CLASSES)


def new_job_id() -> str:
    return uuid.uuid4().hex


def new_worker_id() -> str:
    """Create a globally unique identity for one skeleton process."""

    return f"skeleton-{uuid.uuid4().hex}"


def type_name(loop_type: type[asyncio.AbstractEventLoop] | None) -> str | None:
    if loop_type is None:
        return None
    return f"{loop_type.__module__}:{loop_type.__qualname__}"


def load_loop_type(name: str | None) -> type[asyncio.AbstractEventLoop] | None:
    if name is None:
        return None
    module_name, qualname = name.split(":", 1)
    value: object = importlib.import_module(module_name)
    for part in qualname.split("."):
        value = getattr(value, part)
    if not isinstance(value, type) or not issubclass(value, asyncio.AbstractEventLoop):
        raise TypeError(f"{name} is not an event loop type")
    return value


def read_child_config(fd: int) -> str:
    """Read bounded secret configuration from an inherited pipe."""

    data = bytearray()
    try:
        while True:
            try:
                part = os.read(fd, MAX_CHILD_CONFIG + 1 - len(data))
            except InterruptedError:
                continue
            if not part:
                break
            data.extend(part)
            if len(data) > MAX_CHILD_CONFIG:
                raise ValueError("child configuration is too large")
    finally:
        os.close(fd)
    return data.decode()


def main(args: Sequence[str] | None = None) -> int:
    values = list(sys.argv[1:] if args is None else args)
    if values != [CHILD_MODE]:
        return 2
    control = socket.socket(fileno=CHILD_CONTROL_FD)
    config = ChildConfig.model_validate_json(read_child_config(CHILD_CONFIG_FD), strict=True)
    configure_logging(config.log_level)
    return skeleton_main(
        control,
        CHILD_DEATH_FD,
        CHILD_BEAT_FD,
        config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
