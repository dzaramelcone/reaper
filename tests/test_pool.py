"""Check warm child pool acts."""

import asyncio
import errno
import logging
import os
import pickle
import signal
import socket
import struct
import tempfile
import threading
import uuid
import warnings
from collections.abc import Callable, Mapping
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from reaper.control import ControlEffect, EffectKind, SkeletonID, SkeletonState
from reaper.models import DEFAULT_TOPIC
from reaper.pool import (
    HEADER,
    MAX_MSG,
    Cgroup,
    RemoteWorkerError,
    SkeletonPool,
    SkeletonRole,
    Slot,
    Work,
    WorkerKind,
    new_worker_id,
    perform,
    read_child_config,
    read_message,
    send_message,
    service_child_loop,
    touch_beat,
    watch_parent,
)
from reaper.runtime import RuntimeMarker, RuntimeOperation
from reaper.settings import PoolConfig, ReaperSettings
from reaper.skeleton import LifecycleEvent, LifecycleKind, SkeletonCore
from reaper.worker import LifecycleReporter
from tests.fault_runtime import FaultHooks, FaultRuntime
from tests.faults import FaultPhase, FaultStep, OutcomeKind
from tests.scheduler import CheckpointHooks, DeterministicScheduler
from tests.workers import add_one, block_loop, fail_sync, nested_wait, twice


async def wait_for(check: Callable[[], bool], limit: int = 300) -> None:
    """Wait for one test state."""

    left = limit
    while left:
        if check():
            return
        await asyncio.sleep(0.01)
        left -= 1
    raise AssertionError("test state did not come")


def log_tags(record: logging.LogRecord) -> Mapping[str, object]:
    """Read structured tags attached by the production logger."""

    return cast(Mapping[str, object], getattr(record, "reaper_tags", {}))


def test_all_job_kinds_and_beats() -> None:
    """Check each job kind."""

    async def check() -> None:
        """Run this async check."""

        async with SkeletonPool(2, beat_rate=0.02) as pool:
            assert await pool.run_sync(twice, 4) == 8
            assert await pool.run_async(add_one, 4) == 5
            assert await pool.run_bash("printf hello") == "hello"
            assert len(pool.status()) == 2
            before = {
                slot.identity: os.fstat(slot.beat_fd).st_mtime_ns for slot in pool.slots.values()
            }
            await asyncio.sleep(0.05)
            assert all(not row[2].exists() for row in pool.status())
            assert all(
                os.fstat(slot.beat_fd).st_mtime_ns != before[slot.identity]
                for slot in pool.slots.values()
            )
            assert all(row[1] is SkeletonState.IDLE for row in pool.status())

    asyncio.run(check())


def test_skeleton_lifecycle_is_logged_by_parent_with_uuid(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Child transitions should be ordered in the parent's log stream."""

    caplog.set_level(logging.DEBUG, logger="reaper.pool")

    async def check() -> None:
        async with SkeletonPool(1, beat_rate=0.02) as pool:
            assert await pool.run_async(add_one, 4) == 5

    asyncio.run(check())
    lifecycle = [record for record in caplog.records if record.msg == "skeleton lifecycle"]
    assert {log_tags(record).get("event") for record in lifecycle} >= {
        "start",
        "work_started",
        "work_finished",
        "stopped",
    }
    assert all(
        str(log_tags(record).get("skeleton", "")).startswith("skeleton-") for record in lifecycle
    )
    info = [record.getMessage() for record in caplog.records if record.levelno >= logging.INFO]
    assert "skeleton ready" in info
    assert "skeleton stopped" in info
    assert "skeleton lifecycle" not in info


def test_partial_and_bad_ipc_frames_are_bounded() -> None:
    async def check() -> None:
        loop = asyncio.get_running_loop()
        sender, receiver = socket.socketpair()
        sender.setblocking(False)
        receiver.setblocking(False)
        data = pickle.dumps({"op": "ready"}, protocol=5)
        frame = HEADER.pack(len(data)) + data
        await loop.sock_sendall(sender, frame[:2])
        await asyncio.sleep(0)
        await loop.sock_sendall(sender, frame[2:7])
        await asyncio.sleep(0)
        await loop.sock_sendall(sender, frame[7:])
        assert await read_message(loop, receiver) == {"op": "ready"}
        sender.close()
        receiver.close()

        sender, receiver = socket.socketpair()
        sender.setblocking(False)
        receiver.setblocking(False)
        await loop.sock_sendall(sender, HEADER.pack(5) + b"12")
        sender.close()
        assert await read_message(loop, receiver) is None
        receiver.close()

        sender, receiver = socket.socketpair()
        sender.setblocking(False)
        receiver.setblocking(False)
        await loop.sock_sendall(sender, HEADER.pack(MAX_MSG + 1))
        assert await read_message(loop, receiver) is None
        sender.close()
        receiver.close()

    asyncio.run(check())


def test_wrong_result_id_is_discarded_without_retaining_payload() -> None:
    """A stale or corrupt result cannot accumulate in the parent result map."""

    async def check() -> None:
        loop = asyncio.get_running_loop()
        pool = SkeletonPool(1)
        pool.loop = loop
        control, peer = socket.socketpair()
        control.setblocking(False)
        peer.setblocking(False)
        slot = Slot.model_construct(
            identity=SkeletonID(fd=control.fileno(), gen=1),
            control=control,
            death_writer=-1,
            beat=Path("slot-1.beat"),
            pid=1,
            started_at=0.0,
            state=SkeletonState.RUNNING,
            job_id="expected",
        )
        pool.slots[slot.identity.fd] = slot
        reader = asyncio.create_task(pool.read_slot(slot))
        try:
            await send_message(
                loop,
                peer,
                {"op": "result", "id": "stale", "ok": True, "value": 7},
            )
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert "stale" not in pool.results
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
            control.close()
            peer.close()
            if pool.temp_dir is not None:
                pool.temp_dir.cleanup()

    asyncio.run(check())


def test_concurrent_ipc_writes_preserve_frame_boundaries() -> None:
    """Every producer sharing a control socket must serialize whole frames."""

    async def check() -> None:
        wire = bytearray()
        first_chunk = asyncio.Event()
        release_first = asyncio.Event()
        calls = 0

        async def fragmented_sendall(sock: socket.socket, data: bytes) -> None:
            del sock
            nonlocal calls
            calls += 1
            if calls == 1:
                split = len(data) // 2
                wire.extend(data[:split])
                first_chunk.set()
                await release_first.wait()
                wire.extend(data[split:])
                return
            wire.extend(data)

        loop = cast(
            asyncio.AbstractEventLoop,
            SimpleNamespace(sock_sendall=fragmented_sendall),
        )
        control, peer = socket.socketpair()
        try:
            first = asyncio.create_task(send_message(loop, control, {"sequence": 1}))
            await first_chunk.wait()
            second = asyncio.create_task(send_message(loop, control, {"sequence": 2}))
            await asyncio.sleep(0)
            release_first.set()
            await asyncio.gather(first, second)
        finally:
            control.close()
            peer.close()

        messages: list[Mapping[str, object]] = []
        offset = 0
        try:
            while offset < len(wire):
                size = HEADER.unpack(wire[offset : offset + HEADER.size])[0]
                offset += HEADER.size
                payload = bytes(wire[offset : offset + size])
                offset += size
                value = pickle.loads(payload)
                assert isinstance(value, Mapping)
                messages.append(value)
        except EOFError, pickle.UnpicklingError, struct.error, ValueError:
            messages = []

        assert messages == [{"sequence": 1}, {"sequence": 2}]

    asyncio.run(check())


def test_runtime_short_receive_is_a_real_partial_socket_read() -> None:
    """A scripted short read must exercise the production framing loop."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.CONTROL_RECEIVE,
                    outcome=OutcomeKind.SHORT,
                    site="control",
                    amount=1,
                )
            ],
            DeterministicScheduler(),
        )
        actual_loop = asyncio.get_running_loop()
        requested: list[int] = []

        async def recording_recv(sock: socket.socket, size: int) -> bytes:
            requested.append(size)
            return await actual_loop.sock_recv(sock, size)

        loop = cast(
            asyncio.AbstractEventLoop,
            SimpleNamespace(sock_recv=recording_recv),
        )
        control, peer = socket.socketpair()
        control.setblocking(False)
        peer.setblocking(False)
        try:
            await send_message(actual_loop, peer, {"op": "ready"})
            message = await read_message(
                loop,
                control,
                FaultHooks(runtime),
                actor="slot-1",
                purpose="control",
            )
        finally:
            control.close()
            peer.close()
        assert message == {"op": "ready"}
        assert requested[0] == 1
        assert not runtime.steps

    asyncio.run(check())


def test_malformed_ipc_kills_and_replaces_the_skeleton(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR, logger="reaper.pool")

    async def check() -> None:
        async with SkeletonPool(1, beat_rate=0.02) as pool:
            slot = next(iter(pool.slots.values()))
            old_gen = slot.identity.gen
            await pool.get_loop().sock_sendall(
                slot.control,
                HEADER.pack(3) + b"bad",
            )
            await wait_for(lambda: bool(pool.status()) and pool.status()[0][0].gen > old_gen)
            assert len(pool.status()) == pool.target

    asyncio.run(check())
    exits = [
        record for record in caplog.records if record.message == "skeleton exited unexpectedly"
    ]
    assert len(exits) == 1
    tags = log_tags(exits[0])
    assert tags["exit_code"] == 1
    assert tags["signal"] == 0
    assert "UnpicklingError" in str(tags["fault"])
    assert "Traceback" in str(tags["trace"])


def test_service_fault_retries_in_the_same_skeleton() -> None:
    async def check() -> None:
        loop = asyncio.get_running_loop()
        parent, child = socket.socketpair()
        parent.setblocking(False)
        child.setblocking(False)
        attempts = 0
        recovered = asyncio.Event()

        events: list[LifecycleKind] = []
        core = SkeletonCore()

        async def lifecycle(event: LifecycleEvent) -> None:
            events.append(event.kind)
            core.apply(event)

        async def service(
            ready: Callable[[], None],
            report: LifecycleReporter,
        ) -> None:
            del report
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ValueError("link failed")
            ready()
            recovered.set()
            await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as folder:
            beat_fd = os.open(
                Path(folder) / "service.beat",
                os.O_CREAT | os.O_RDWR,
                0o600,
            )
            running = asyncio.create_task(
                service_child_loop(
                    loop,
                    child,
                    beat_fd,
                    0.01,
                    service,
                    SkeletonRole.TASK,
                    lifecycle,
                )
            )
            fault = await read_message(loop, parent)
            assert fault
            assert fault["op"] == "service_fault"
            assert fault["kind"] == "builtins.ValueError"
            assert "link failed" in str(fault["trace"])
            await asyncio.wait_for(recovered.wait(), timeout=1)
            assert await read_message(loop, parent) == {"op": "ready"}
            assert not running.done()
            await send_message(loop, parent, {"op": "stop"})
            await asyncio.wait_for(running, timeout=1)
            assert events.count(LifecycleKind.START) == 2
            assert LifecycleKind.FAULT in events
            assert events[-2:] == [LifecycleKind.STOP, LifecycleKind.STOPPED]
            os.close(beat_fd)
        parent.close()
        child.close()

    asyncio.run(check())


def test_repeated_service_fault_opens_the_pool_circuit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR, logger="reaper.pool")

    async def check() -> None:
        pool = SkeletonPool(1, beat_rate=0.02, role=SkeletonRole.TASK)
        pool.settings = ReaperSettings(
            postgres_dsn="postgresql://127.0.0.1/reaper?sslmode=invalid",
            service_retry_base=0.01,
            service_retry_max=0.01,
            pools=[PoolConfig(skeletons=1)],
        )
        pool.topic = DEFAULT_TOPIC
        pool.service_failure_rounds = 3
        faults = await asyncio.gather(pool.start(), return_exceptions=True)
        assert isinstance(faults[0], RuntimeError)
        assert "repeated for 3 pool rounds" in str(faults[0])
        assert pool.failure_event.is_set()

    asyncio.run(check())


def test_service_circuit_classifies_and_counts_pool_rounds() -> None:
    pool = SkeletonPool(2)
    pool.service_failure_rounds = 3
    sockets = [socket.socketpair(), socket.socketpair()]
    slots = [
        Slot(
            identity=SkeletonID(fd=pair[0].fileno(), gen=index + 1),
            control=pair[0],
            death_writer=-1,
            beat=Path("beat"),
            pid=index + 1,
            started_at=0,
            fault_kind="builtins.ValueError",
        )
        for index, pair in enumerate(sockets)
    ]

    for _ in range(2):
        for slot in slots:
            pool.record_service_fault(slot, {"code": ""})
    assert not pool.failure_event.is_set()

    pool.clear_service_faults()
    for _ in range(2):
        for slot in slots:
            pool.record_service_fault(slot, {"code": ""})
    assert not pool.failure_event.is_set()

    for slot in slots:
        pool.record_service_fault(slot, {"code": ""})
    assert pool.failure_event.is_set()

    transient = SkeletonPool(2)
    for _ in range(10):
        for slot in slots:
            slot.fault_kind = "builtins.ConnectionError"
            transient.record_service_fault(slot, {"code": "08006"})
    assert not transient.failure_event.is_set()

    for code in ("55P03", "57014"):
        timed = SkeletonPool(2)
        for _ in range(10):
            for slot in slots:
                slot.fault_kind = "asyncpg.exceptions.PostgresError"
                timed.record_service_fault(slot, {"code": code})
        assert not timed.failure_event.is_set()

    permanent = SkeletonPool(2)
    slots[0].fault_kind = "asyncpg.exceptions.UndefinedColumnError"
    permanent.record_service_fault(slots[0], {"code": "42703"})
    assert permanent.failure_event.is_set()

    for parent, child in sockets:
        parent.close()
        child.close()


def test_job_fault_crosses_the_fd() -> None:
    """Check one remote job fault."""

    async def check() -> None:
        """Run this async check."""

        async with SkeletonPool(1) as pool:
            items = await asyncio.gather(
                pool.run_sync(fail_sync),
                return_exceptions=True,
            )
            assert isinstance(items[0], RemoteWorkerError)
            assert "sync fault" in str(items[0])

    asyncio.run(check())


def test_dead_slot_gets_a_new_gen(caplog: pytest.LogCaptureFixture) -> None:
    """Check lost slots get filled."""

    caplog.set_level(logging.ERROR, logger="reaper.pool")

    async def check() -> None:
        """Run this async check."""

        async with SkeletonPool(2, beat_rate=0.02) as pool:
            old_gen = max(slot.identity.gen for slot in pool.slots.values())
            victim = next(iter(pool.slots.values()))
            os.kill(victim.pid, signal.SIGKILL)
            await wait_for(
                lambda: (
                    len(pool.status()) == 2 and max(item[0].gen for item in pool.status()) > old_gen
                )
            )
            assert len(pool.status()) == pool.target

    asyncio.run(check())
    exits = [
        record for record in caplog.records if record.message == "skeleton exited unexpectedly"
    ]
    assert len(exits) == 1
    assert log_tags(exits[0])["exit_code"] == -9
    assert log_tags(exits[0])["signal"] == 9


def test_terminal_interrupt_is_left_to_the_supervisor() -> None:
    """A foreground Ctrl-C must not interrupt each warm child."""

    async def check() -> None:
        async with SkeletonPool(1, beat_rate=0.02) as pool:
            slot = next(iter(pool.slots.values()))
            os.kill(slot.pid, signal.SIGINT)
            await asyncio.sleep(0.05)

            current = next(iter(pool.slots.values()))
            assert current.pid == slot.pid
            assert current.identity == slot.identity
            assert await pool.run_sync(twice, 3) == 6

    asyncio.run(check())


def test_worker_ids_are_skeleton_uuids() -> None:
    """Skeleton identities remain unique across hosts and PID namespaces."""

    first = new_worker_id()
    second = new_worker_id()

    assert first != second
    assert uuid.UUID(first.removeprefix("skeleton-"))
    assert uuid.UUID(second.removeprefix("skeleton-"))


def test_parent_eof_kills_a_child_tree() -> None:
    """Check EOF ends a child tree."""

    async def check() -> None:
        """Run this async check."""

        with tempfile.TemporaryDirectory() as folder:
            beat_dir = Path(folder)
            async with SkeletonPool(1, beat_rate=0.02) as pool:
                job = asyncio.create_task(pool.run_async(nested_wait, beat_dir))
                await wait_for(lambda: bool(list(beat_dir.glob("*.beat"))))
                outer = next(slot for slot in pool.slots.values() if slot.job_id)
                old_gen = outer.identity.gen
                os.kill(outer.pid, signal.SIGKILL)
                items = await asyncio.gather(job, return_exceptions=True)
                assert isinstance(items[0], RemoteWorkerError)
                beat = next(iter(beat_dir.glob("*.beat")))
                await asyncio.sleep(0.08)
                stamp = beat.stat().st_mtime_ns
                await asyncio.sleep(0.08)
                assert beat.stat().st_mtime_ns == stamp
                await wait_for(
                    lambda: len(pool.status()) == 1 and pool.status()[0][0].gen > old_gen
                )

    asyncio.run(check())


def test_parent_eof_ends_a_blocked_child_loop() -> None:
    """Check death watching does not need the child loop."""

    async def check() -> None:
        async with SkeletonPool(1, beat_rate=0.02) as pool:
            job = asyncio.create_task(pool.run_async(block_loop, 30.0))
            await wait_for(lambda: pool.status()[0][1] is SkeletonState.RUNNING)
            victim = next(iter(pool.slots.values()))
            old_gen = victim.identity.gen
            os.close(victim.death_writer)
            victim.death_writer = -1
            result = await asyncio.wait_for(
                asyncio.gather(job, return_exceptions=True),
                timeout=2,
            )
            assert isinstance(result[0], RemoteWorkerError)
            await wait_for(
                lambda: bool(pool.status()) and pool.status()[0][0].gen > old_gen,
            )

    asyncio.run(check())


def test_stalled_child_heartbeat_is_killed_and_replaced() -> None:
    """A live process with a blocked event loop cannot occupy a slot forever."""

    async def check() -> None:
        async with SkeletonPool(1, beat_rate=0.02) as pool:
            result = await asyncio.wait_for(
                asyncio.gather(
                    pool.run_async(block_loop, 5.0),
                    return_exceptions=True,
                ),
                timeout=3.0,
            )
            assert isinstance(result[0], RemoteWorkerError)
            await wait_for(
                lambda: len(pool.status()) == 1 and pool.status()[0][1] is SkeletonState.IDLE
            )
            assert await pool.run_sync(twice, 4) == 8

    asyncio.run(check())


def test_pool_guards_and_bash_fault() -> None:
    """Check pool guard paths."""

    with pytest.raises(ValueError):
        SkeletonPool(0)
    with pytest.raises(ValueError):
        SkeletonPool(1, beat_rate=0)
    pool = SkeletonPool(1)
    with pytest.raises(RuntimeError):
        pool.get_loop()

    async def check() -> None:
        """Run this async check."""

        await pool.start()
        await pool.start()
        pool.closing = True
        blocked = await asyncio.gather(
            pool.run_sync(twice, 2),
            return_exceptions=True,
        )
        assert isinstance(blocked[0], RuntimeError)
        pool.closing = False
        failed = await asyncio.gather(
            pool.run_bash("exit 9"),
            return_exceptions=True,
        )
        assert isinstance(failed[0], RemoteWorkerError)
        await pool.close()
        await pool.close()

    asyncio.run(check())


def test_local_work_queue_is_bounded() -> None:
    async def check() -> None:
        pool = SkeletonPool(1, beat_rate=0.02)
        pool.queue_limit = 1
        await pool.start()
        running = asyncio.create_task(pool.run_async(block_loop, 30.0))
        await wait_for(lambda: pool.status()[0][1] is SkeletonState.RUNNING)
        queued = asyncio.create_task(pool.run_async(block_loop, 30.0))
        await wait_for(lambda: len(pool.core.queue) == 1)
        faults = await asyncio.gather(
            pool.run_async(block_loop, 30.0),
            return_exceptions=True,
        )
        assert isinstance(faults[0], RuntimeError)
        assert "queue is full" in str(faults[0])
        await pool.close(timeout=0.1)
        await asyncio.gather(running, queued, return_exceptions=True)

    asyncio.run(check())


def test_submit_after_close_fails_without_restarting() -> None:
    """A terminal pool must reject work instead of waiting for impossible slots."""

    async def check() -> None:
        pool = SkeletonPool(1)
        await pool.start()
        await pool.close(timeout=0.1)

        with pytest.raises(RuntimeError, match="shut"):
            async with asyncio.timeout(0.1):
                await pool.run_sync(twice, 2)

    asyncio.run(check())


def test_start_waits_for_target_capacity_after_starting_slot_is_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-ready death must not make the pool's readiness predicate vacuous."""

    async def check() -> None:
        pool = SkeletonPool(2)

        def make_slot(fd: int, generation: int, state: SkeletonState) -> Slot:
            return Slot.model_construct(
                identity=SkeletonID(fd=fd, gen=generation),
                control=cast(socket.socket, object()),
                death_writer=-1,
                beat=Path(f"slot-{generation}.beat"),
                pid=generation,
                started_at=0.0,
                state=state,
            )

        async def fake_drive(effects: object) -> None:
            del effects
            if pool.slots:
                return
            pool.slots = {
                10: make_slot(10, 1, SkeletonState.IDLE),
                11: make_slot(11, 2, SkeletonState.STARTING),
            }

        sleep_calls = 0
        real_sleep = asyncio.sleep

        async def replace_lost_slot(delay: float) -> None:
            nonlocal sleep_calls
            await real_sleep(0)
            if delay != 0.01:
                return
            sleep_calls += 1
            if sleep_calls == 1:
                del pool.slots[11]
            elif sleep_calls == 2:
                pool.slots[12] = make_slot(12, 3, SkeletonState.IDLE)

        pool.drive = fake_drive  # type: ignore[method-assign]
        monkeypatch.setattr(asyncio, "sleep", replace_lost_slot)
        try:
            await pool.start()
            assert len(pool.slots) == pool.target
            assert all(slot.state is SkeletonState.IDLE for slot in pool.slots.values())
        finally:
            if pool.temp_dir is not None:
                pool.temp_dir.cleanup()

    asyncio.run(check())


def test_runtime_start_replaces_a_child_killed_at_spawn_checkpoint() -> None:
    """Kill a real child before its reader starts without relying on polling."""

    async def check() -> None:
        scheduler = DeterministicScheduler()
        hooks = CheckpointHooks(scheduler, [RuntimeMarker.SLOT_SPAWNED])
        pool = SkeletonPool(1, beat_rate=0.02, hooks=hooks)
        starting = asyncio.create_task(pool.start())
        try:
            first = await asyncio.wait_for(
                scheduler.wait_for(operation="slot_spawned"),
                timeout=2,
            )
            victim = next(iter(pool.slots.values()))
            assert first[0].details["generation"] == str(victim.identity.gen)
            os.kill(victim.pid, signal.SIGKILL)
            await scheduler.release_matching("slot_spawned")

            second = await asyncio.wait_for(
                scheduler.wait_for(operation="slot_spawned"),
                timeout=2,
            )
            assert int(second[0].details["generation"]) > victim.identity.gen
            await scheduler.release_matching("slot_spawned")
            await asyncio.wait_for(starting, timeout=2)
            assert len(pool.slots) == pool.target
            assert all(slot.state is SkeletonState.IDLE for slot in pool.slots.values())
        finally:
            await scheduler.release_all()
            if not starting.done():
                starting.cancel()
                await asyncio.gather(starting, return_exceptions=True)
            await pool.close(timeout=0.2)

    asyncio.run(check())


def test_spawn_setup_failure_closes_every_opened_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial spawn must become a recoverable result without leaking FDs."""

    async def check() -> None:
        pool = SkeletonPool(1)
        pool.loop = asyncio.get_running_loop()
        opened: list[int] = []
        made_sockets: list[socket.socket] = []
        real_socketpair = socket.socketpair
        real_pipe = os.pipe
        pipe_calls = 0

        def tracked_socketpair() -> tuple[socket.socket, socket.socket]:
            pair = real_socketpair()
            made_sockets.extend(pair)
            opened.extend(item.fileno() for item in pair)
            return pair

        def failing_pipe() -> tuple[int, int]:
            nonlocal pipe_calls
            pipe_calls += 1
            if pipe_calls == 2:
                raise OSError(errno.EMFILE, "scripted pipe exhaustion")
            pair = real_pipe()
            opened.extend(pair)
            return pair

        monkeypatch.setattr(socket, "socketpair", tracked_socketpair)
        monkeypatch.setattr(os, "pipe", failing_pipe)
        outcome = (await asyncio.gather(pool.spawn_effect(1), return_exceptions=True))[0]
        try:
            assert isinstance(outcome, OSError)
            assert outcome.errno == errno.EMFILE
            assert all(item.fileno() == -1 for item in made_sockets)
            for descriptor in opened:
                with pytest.raises(OSError):
                    os.fstat(descriptor)
        finally:
            for item in made_sockets:
                item.close()
            for descriptor in opened:
                with suppress(OSError):
                    os.close(descriptor)
            if pool.temp_dir is not None:
                pool.temp_dir.cleanup()

    asyncio.run(check())


def test_runtime_pipe_exhaustion_recovers_to_full_capacity() -> None:
    """A scripted setup fault must pass through live pool recovery."""

    async def check() -> None:
        runtime = FaultRuntime(
            [FaultStep(call=RuntimeOperation.PIPE, outcome=OutcomeKind.RESOURCE_LIMIT)],
            DeterministicScheduler(),
        )
        async with SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime)) as pool:
            assert not runtime.steps
            assert len(pool.status()) == 1
            assert pool.status()[0][1] is SkeletonState.IDLE
            assert await pool.run_sync(twice, 3) == 6

    asyncio.run(check())


def test_runtime_short_heartbeat_initialization_recovers_to_full_capacity() -> None:
    """A partial marker initialization must discard and replace the slot."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.HEARTBEAT_WRITE,
                    outcome=OutcomeKind.SHORT,
                    site="initialize",
                )
            ],
            DeterministicScheduler(),
        )
        async with SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime)) as pool:
            assert not runtime.steps
            assert len(pool.status()) == 1
            assert pool.status()[0][1] is SkeletonState.IDLE
            assert await pool.run_sync(twice, 3) == 6

    asyncio.run(check())


def test_runtime_short_config_write_preserves_the_complete_child_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial pipe write must advance only by the bytes actually written."""

    async def check() -> None:
        settings = ReaperSettings(
            postgres_dsn="postgresql://worker:secret@db/reaper",
            pools=[PoolConfig(skeletons=1)],
        )
        writes: list[int] = []
        real_write = os.write

        def tracking_write(fd: int, data: bytes) -> int:
            if data:
                writes.append(len(data))
            return real_write(fd, data)

        monkeypatch.setattr("reaper.pool.os.write", tracking_write)
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.CONFIG_WRITE,
                    outcome=OutcomeKind.SHORT,
                    site="config",
                    amount=1,
                )
            ],
            DeterministicScheduler(),
        )
        pool = SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime))
        pool.settings = settings
        async with pool:
            assert not runtime.steps
            assert await pool.run_sync(twice, 3) == 6
        assert writes[-2] == 1
        assert writes[-1] > writes[-2]

    asyncio.run(check())


def test_runtime_short_child_heartbeat_write_stops_the_publisher() -> None:
    """The child must treat a partial marker publication as liveness failure."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.HEARTBEAT_WRITE,
                    outcome=OutcomeKind.SHORT,
                    site="publish",
                )
            ],
            DeterministicScheduler(),
        )
        with tempfile.TemporaryFile() as beat:
            result = (
                await asyncio.gather(
                    touch_beat(
                        beat.fileno(),
                        0.01,
                        FaultHooks(runtime),
                        "child",
                    ),
                    return_exceptions=True,
                )
            )[0]
        assert isinstance(result, OSError)
        assert not runtime.steps

    asyncio.run(check())


def test_runtime_thread_spawn_failure_recovers_to_full_capacity() -> None:
    """Failure to start the spawn helper thread must refill the pool."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.SPAWN_THREAD,
                    outcome=OutcomeKind.RESOURCE_LIMIT,
                )
            ],
            DeterministicScheduler(),
        )
        async with SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime)) as pool:
            assert not runtime.steps
            assert len(pool.status()) == 1
            assert pool.status()[0][1] is SkeletonState.IDLE
            assert await pool.run_sync(twice, 3) == 6

    asyncio.run(check())


def test_runtime_post_spawn_failure_reaps_child_and_recovers_capacity() -> None:
    """An ambiguous spawn must not leave an untracked child or empty slot."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.SPAWN_PROCESS,
                    outcome=OutcomeKind.PERMANENT_ERROR,
                    phase=FaultPhase.AFTER,
                )
            ],
            DeterministicScheduler(),
        )
        async with SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime)) as pool:
            assert not runtime.steps
            assert len(pool.status()) == 1
            assert pool.status()[0][1] is SkeletonState.IDLE
            assert await pool.run_sync(twice, 3) == 6

    asyncio.run(check())


def test_cancel_during_posix_spawn_reaps_the_late_returning_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation must wait for the spawn PID and reap its ambiguous child."""

    async def check() -> None:
        real_spawn = cast(Callable[..., int], os.posix_spawn)
        real_kill = os.killpg
        spawned = threading.Event()
        return_pid = threading.Event()
        pids: list[int] = []
        kill_attempts = 0

        def delayed_spawn(*args: object, **kwargs: object) -> int:
            pid = real_spawn(*args, **kwargs)
            pids.append(pid)
            spawned.set()
            if not return_pid.wait(2):
                raise TimeoutError("test did not release posix_spawn result")
            return pid

        monkeypatch.setattr("reaper.pool.os.posix_spawn", delayed_spawn)

        def interrupted_kill(pid: int, sig: int) -> None:
            nonlocal kill_attempts
            kill_attempts += 1
            if kill_attempts == 1:
                raise InterruptedError(errno.EINTR, "scripted cleanup interruption")
            real_kill(pid, sig)

        monkeypatch.setattr("reaper.pool.os.killpg", interrupted_kill)
        pool = SkeletonPool(1, beat_rate=0.02)
        pool.loop = asyncio.get_running_loop()
        spawning = asyncio.create_task(pool.spawn_effect(1))
        assert await asyncio.to_thread(spawned.wait, 2)
        spawning.cancel()
        return_pid.set()
        result = (await asyncio.gather(spawning, return_exceptions=True))[0]
        assert isinstance(result, asyncio.CancelledError)
        assert len(pids) == 1

        reaped_by_pool = False
        try:
            async with asyncio.timeout(2):
                while True:
                    try:
                        waited, _ = os.waitpid(pids[0], os.WNOHANG)
                    except ChildProcessError:
                        reaped_by_pool = True
                        break
                    if waited == pids[0]:
                        break
                    await asyncio.sleep(0.01)
        finally:
            if not reaped_by_pool:
                with suppress(ProcessLookupError):
                    os.kill(pids[0], signal.SIGKILL)
                with suppress(ChildProcessError):
                    await asyncio.to_thread(os.waitpid, pids[0], 0)
            if pool.temp_dir is not None:
                pool.temp_dir.cleanup()
        assert reaped_by_pool
        assert kill_attempts >= 2

    asyncio.run(check())


@pytest.mark.parametrize(
    "outcome",
    (OutcomeKind.PERMANENT_ERROR, OutcomeKind.SHORT, OutcomeKind.EOF),
)
def test_persistent_heartbeat_read_failures_replace_the_blind_slot(
    monkeypatch: pytest.MonkeyPatch,
    outcome: OutcomeKind,
) -> None:
    """A broken heartbeat descriptor must not disable liveness forever."""

    async def check() -> None:
        monkeypatch.setattr("reaper.pool.HEARTBEAT_MIN_TIMEOUT", 0.2)
        runtime = FaultRuntime([], DeterministicScheduler())
        async with SkeletonPool(1, beat_rate=0.01, hooks=FaultHooks(runtime)) as pool:
            old_gen = pool.status()[0][0].gen
            runtime.steps.extend(
                [
                    FaultStep(
                        call=RuntimeOperation.HEARTBEAT_READ,
                        outcome=outcome,
                        site="read",
                    )
                    for _ in range(25)
                ]
            )
            await wait_for(
                lambda: (
                    not runtime.steps
                    and bool(pool.status())
                    and pool.status()[0][0].gen > old_gen
                    and pool.status()[0][1] is SkeletonState.IDLE
                )
            )
            assert await pool.run_sync(twice, 4) == 8

    asyncio.run(check())


@pytest.mark.parametrize(
    "call",
    (
        RuntimeOperation.SOCKET_PAIR,
        RuntimeOperation.DUP_FD,
        RuntimeOperation.HEARTBEAT_OPEN,
        RuntimeOperation.SPAWN_PROCESS,
    ),
)
def test_runtime_spawn_boundary_failure_recovers_to_full_capacity(
    call: RuntimeOperation,
) -> None:
    """Every remaining process setup boundary must close and refill cleanly."""

    async def check() -> None:
        runtime = FaultRuntime(
            [FaultStep(call=call, outcome=OutcomeKind.RESOURCE_LIMIT)],
            DeterministicScheduler(),
        )
        async with SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime)) as pool:
            assert not runtime.steps
            assert len(pool.status()) == 1
            assert pool.status()[0][1] is SkeletonState.IDLE
            assert await pool.run_sync(twice, 3) == 6

    asyncio.run(check())


@pytest.mark.parametrize("outcome", (OutcomeKind.NOT_FOUND, OutcomeKind.PERMISSION))
def test_runtime_spawn_lookup_and_permission_failures_replace_the_slot(
    outcome: OutcomeKind,
) -> None:
    """Executable lookup and permission failures must not become successful spawns."""

    async def check() -> None:
        runtime = FaultRuntime(
            [FaultStep(call=RuntimeOperation.SPAWN_PROCESS, outcome=outcome)],
            DeterministicScheduler(),
        )
        async with SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime)) as pool:
            assert not runtime.steps
            assert pool.status()[0][0].gen > 1
            assert pool.status()[0][1] is SkeletonState.IDLE

    asyncio.run(check())


@pytest.mark.parametrize(
    "call",
    (RuntimeOperation.CONTROL_SEND, RuntimeOperation.CONTROL_RECEIVE),
)
@pytest.mark.parametrize("phase", (FaultPhase.BEFORE, FaultPhase.AFTER))
def test_runtime_control_transport_fault_recovers_to_full_capacity(
    call: RuntimeOperation,
    phase: FaultPhase,
) -> None:
    """A scripted parent IPC fault must replace the affected live slot."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=call,
                    outcome=OutcomeKind.PERMANENT_ERROR,
                    phase=phase,
                    site="control",
                )
            ],
            DeterministicScheduler(),
        )
        async with SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime)) as pool:
            if runtime.steps:
                result = await asyncio.gather(
                    pool.run_sync(twice, 3),
                    return_exceptions=True,
                )
                if phase is FaultPhase.BEFORE:
                    assert isinstance(result[0], RemoteWorkerError)
                else:
                    assert result[0] == 6 or isinstance(result[0], RemoteWorkerError)
            await wait_for(
                lambda: (
                    not runtime.steps
                    and len(pool.status()) == 1
                    and pool.status()[0][1] is SkeletonState.IDLE
                )
            )
            assert await pool.run_sync(twice, 4) == 8

    asyncio.run(check())


def test_runtime_close_failure_does_not_derail_pool_shutdown() -> None:
    """A failed descriptor close is contained and retried during teardown."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.CLOSE_FD,
                    outcome=OutcomeKind.PERMANENT_ERROR,
                    site="death",
                )
            ],
            DeterministicScheduler(),
        )
        pool = SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime))
        await pool.start()
        assert await pool.run_sync(twice, 3) == 6
        await pool.close(timeout=0.1)
        assert not runtime.steps
        assert not pool.status()

    asyncio.run(check())


def test_runtime_interrupted_wait_reaps_child_during_shutdown() -> None:
    """An interrupted wait is retried so shutdown leaves no zombie child."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.WAIT_PROCESS,
                    outcome=OutcomeKind.INTERRUPTED,
                )
            ],
            DeterministicScheduler(),
        )
        pool = SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime))
        await pool.start()
        await pool.close(timeout=0.1)
        assert not runtime.steps
        assert not pool.status()

    asyncio.run(check())


@pytest.mark.parametrize("call", (RuntimeOperation.KILL, RuntimeOperation.WAIT_PROCESS))
def test_runtime_missing_process_is_an_idempotent_shutdown_outcome(
    call: RuntimeOperation,
) -> None:
    """An already-gone child must not interrupt pool teardown."""

    async def check() -> None:
        runtime = FaultRuntime(
            [FaultStep(call=call, outcome=OutcomeKind.NOT_FOUND)],
            DeterministicScheduler(),
        )
        pool = SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime))
        await pool.start()
        job = asyncio.create_task(pool.run_async(block_loop, 2.0))
        await wait_for(lambda: pool.status()[0][1] is SkeletonState.RUNNING)
        await pool.close(timeout=0.02)
        await asyncio.gather(job, return_exceptions=True)
        assert not runtime.steps
        assert not pool.status()

    asyncio.run(check())


@pytest.mark.parametrize(
    ("call", "effect_kind"),
    (
        (RuntimeOperation.KILL, EffectKind.KILL),
        (RuntimeOperation.WAIT_PROCESS, EffectKind.REAP),
    ),
)
def test_runtime_missing_process_skips_the_impossible_real_syscall(
    monkeypatch: pytest.MonkeyPatch,
    call: RuntimeOperation,
    effect_kind: EffectKind,
) -> None:
    """ESRCH/ECHILD is terminal; retrying can act on a reused process identifier."""

    async def check() -> None:
        runtime = FaultRuntime(
            [FaultStep(call=call, outcome=OutcomeKind.NOT_FOUND)],
            DeterministicScheduler(),
        )
        pool = SkeletonPool(1, hooks=FaultHooks(runtime))
        pool.loop = asyncio.get_running_loop()
        identity = SkeletonID(fd=71, gen=1)
        pool.slots[identity.fd] = Slot.model_construct(
            identity=identity,
            control=cast(socket.socket, object()),
            death_writer=-1,
            beat=pool.beat_dir / "missing.beat",
            beat_fd=-1,
            pid=73,
            started_at=0.0,
        )
        syscalls: list[int] = []
        monkeypatch.setattr("reaper.pool.os.killpg", lambda pid, sig: syscalls.append(pid))
        monkeypatch.setattr(
            "reaper.pool.reap",
            lambda pid: syscalls.append(pid),
        )
        try:
            await pool.drive((ControlEffect(kind=effect_kind, identity=identity),))
            assert not runtime.steps
            assert syscalls == []
        finally:
            if pool.temp_dir is not None:
                pool.temp_dir.cleanup()

    asyncio.run(check())


def test_runtime_process_exit_supplies_wait_status_without_a_real_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A modeled wait result must drive exit handling instead of being discarded."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.WAIT_PROCESS,
                    outcome=OutcomeKind.PROCESS_EXIT,
                    amount=17,
                )
            ],
            DeterministicScheduler(),
        )
        pool = SkeletonPool(1, hooks=FaultHooks(runtime))
        pool.loop = asyncio.get_running_loop()
        identity = SkeletonID(fd=79, gen=1)
        slot = Slot.model_construct(
            identity=identity,
            control=cast(socket.socket, object()),
            death_writer=-1,
            beat=pool.beat_dir / "exited.beat",
            beat_fd=-1,
            pid=83,
            started_at=0.0,
        )
        pool.retired_slots[(identity.fd, identity.gen)] = slot
        syscalls: list[int] = []
        monkeypatch.setattr("reaper.pool.reap", lambda pid: syscalls.append(pid))
        exits: list[int] = []
        monkeypatch.setattr(pool, "log_exit", lambda found, status: exits.append(status.exit_code))
        try:
            await pool.drive((ControlEffect(kind=EffectKind.REAP, identity=identity),))
            assert not runtime.steps
            assert syscalls == []
            assert exits == [17]
            assert not pool.retired_slots
        finally:
            if pool.temp_dir is not None:
                pool.temp_dir.cleanup()

    asyncio.run(check())


def test_runtime_post_wait_failure_keeps_reaped_child_cleanup_idempotent() -> None:
    """Losing the wait result after waitpid must still retire the slot."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.WAIT_PROCESS,
                    outcome=OutcomeKind.NOT_FOUND,
                    phase=FaultPhase.AFTER,
                )
            ],
            DeterministicScheduler(),
        )
        pool = SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime))
        await pool.start()
        await pool.close(timeout=0.1)
        assert not runtime.steps
        assert not pool.status()

    asyncio.run(check())


def test_cancelled_close_finishes_teardown_before_propagating_cancellation() -> None:
    """Cancellation must not strand a permanently-closing live process pool."""

    async def check() -> None:
        scheduler = DeterministicScheduler()
        hooks = CheckpointHooks(scheduler, [RuntimeMarker.SHUTDOWN_STARTED])
        pool = SkeletonPool(1, beat_rate=0.02, hooks=hooks)
        await pool.start()
        closing = asyncio.create_task(pool.close(timeout=0.1))
        await scheduler.wait_for(operation="shutdown_started")
        closing.cancel()
        await scheduler.release()
        result = (await asyncio.gather(closing, return_exceptions=True))[0]
        try:
            assert isinstance(result, asyncio.CancelledError)
            assert not pool.status()
            assert not pool.started
        finally:
            if pool.status():
                pool.closing = False
                pool.hooks = CheckpointHooks(scheduler, [])
                await pool.close(timeout=0.1)

    asyncio.run(check())


def test_cancelled_start_cleans_partial_processes_before_propagating() -> None:
    """A cancelled startup must not leave an ownerless partial pool."""

    async def check() -> None:
        scheduler = DeterministicScheduler()
        hooks = CheckpointHooks(scheduler, [RuntimeMarker.STARTUP_WAIT])
        pool = SkeletonPool(1, beat_rate=0.02, hooks=hooks)
        starting = asyncio.create_task(pool.start())
        await scheduler.wait_for(operation="startup_wait")
        starting.cancel()
        await scheduler.release()
        result = (await asyncio.gather(starting, return_exceptions=True))[0]
        try:
            assert isinstance(result, asyncio.CancelledError)
            assert not pool.status()
            assert not pool.started
        finally:
            if pool.status():
                pool.hooks = CheckpointHooks(scheduler, [])
                pool.closing = False
                await pool.close(timeout=0.1)

    asyncio.run(check())


def test_runtime_interrupted_kill_still_terminates_a_stuck_child() -> None:
    """An interrupted hard kill is retried after the shutdown deadline."""

    async def check() -> None:
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.KILL,
                    outcome=OutcomeKind.INTERRUPTED,
                )
            ],
            DeterministicScheduler(),
        )
        pool = SkeletonPool(1, beat_rate=0.02, hooks=FaultHooks(runtime))
        await pool.start()
        job = asyncio.create_task(pool.run_async(block_loop, 2.0))
        await wait_for(lambda: pool.status()[0][1] is SkeletonState.RUNNING)
        await pool.close(timeout=0.02)
        await asyncio.gather(job, return_exceptions=True)
        assert not runtime.steps
        assert not pool.status()

    asyncio.run(check())


def test_job_id_collision_is_namespaced_before_submission() -> None:
    """A random identifier collision cannot leave a submitted future orphaned."""

    async def check() -> None:
        ids = iter(("same", "same", "different"))
        async with SkeletonPool(1, id_source=lambda: next(ids)) as pool:
            assert await pool.run_sync(twice, 2) == 4
            assert await asyncio.wait_for(pool.run_sync(twice, 3), timeout=1) == 6

    asyncio.run(check())


def test_completed_jobs_do_not_accumulate_identifier_history() -> None:
    """A long-running daemon must not retain one object per completed job."""

    async def check() -> None:
        async with SkeletonPool(2) as pool:
            for value in range(256):
                assert await pool.run_sync(twice, value) == value * 2
            assert not pool.core.done
            assert not pool.core.failed
            assert not pool.jobs

    asyncio.run(check())


def test_cgroup_and_bad_work() -> None:
    """Check cgroup and bad jobs."""

    with tempfile.TemporaryDirectory() as folder:
        file = Path(folder) / "cgroup.procs"
        file.touch()
        Cgroup(path=Path(folder)).join()
        assert file.read_text(encoding="ascii") == str(os.getpid())

    async def check() -> None:
        """Run this async check."""

        work = Work.model_construct(
            id="bad",
            kind=cast(WorkerKind, "odd"),
            target=None,
        )
        items = await asyncio.gather(
            perform(work),
            return_exceptions=True,
        )
        assert isinstance(items[0], RuntimeError)

    asyncio.run(check())


def test_parent_watch_retries_an_interrupted_pipe_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A signal must not disable the skeleton's parent-death guarantee."""

    reads = iter((InterruptedError(errno.EINTR, "signal"), b""))
    killed: list[tuple[int, int]] = []

    def read(fd: int, size: int) -> bytes:
        assert (fd, size) == (17, 1)
        result = next(reads)
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(os, "read", read)
    monkeypatch.setattr(os, "getpgrp", lambda: 23)
    monkeypatch.setattr(os, "killpg", lambda pid, sig: killed.append((pid, sig)))

    watch_parent(17, threading.Event())

    assert killed == [(23, signal.SIGTERM)]


def test_child_config_retries_an_interrupted_pipe_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Signals must not make an inherited child configuration flaky."""

    reads = iter((InterruptedError(errno.EINTR, "signal"), b"postgres", b""))
    closed: list[int] = []

    def read(fd: int, size: int) -> bytes:
        assert fd == 19
        assert size > 0
        result = next(reads)
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(os, "read", read)
    monkeypatch.setattr(os, "close", closed.append)

    assert read_child_config(19) == "postgres"
    assert closed == [19]


def test_invalid_cgroup_placement_opens_the_startup_circuit_without_orphans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child-side cgroup failure must remain bounded and fully reaped."""

    async def check() -> None:
        monkeypatch.setattr("reaper.pool.MAX_PRE_READY_FAILURES", 2)
        monkeypatch.setattr("reaper.pool.RECOVERY_BASE_DELAY", 0.01)
        with tempfile.TemporaryDirectory() as folder:
            pool = SkeletonPool(
                1,
                beat_rate=0.02,
                cgroup=Cgroup(path=Path(folder) / "missing-cgroup"),
            )
            with pytest.raises(RuntimeError, match="failed to become ready after 2 attempts"):
                await asyncio.wait_for(pool.start(), timeout=2)
            assert not pool.status()
            assert not pool.started
            assert pool.recovery_task is None

    asyncio.run(check())


def test_pool_starts_with_a_live_thread() -> None:
    async def check() -> None:
        gate = threading.Event()
        thread = asyncio.create_task(asyncio.to_thread(gate.wait, 5))
        await wait_for(lambda: threading.active_count() > 1)
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always", DeprecationWarning)
            async with SkeletonPool(2) as pool:
                assert await pool.run_sync(twice, 5) == 10
        gate.set()
        await thread
        fork_notes = [item for item in seen if "fork" in str(item.message).lower()]
        assert fork_notes == []

    asyncio.run(check())
