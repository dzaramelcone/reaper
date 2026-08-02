"""Check bounded liveness under fair scheduling."""

import asyncio
import socket
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies

from reaper.control import ControlEvent, EventKind, SkeletonID, SkeletonState
from reaper.pool import HeartbeatWatch, SkeletonPool, Slot
from tests.dst import ReaperModel, ScenarioCompiler
from tests.startup_model import StartupLife, StartupModel


def test_process_heartbeat_uses_marker_changes_and_parent_monotonic_time() -> None:
    """A delayed observer gets a new baseline instead of declaring false death."""

    watch = HeartbeatWatch()
    assert watch.observe(b"one", now=10.0, timeout=5.0)
    assert watch.observe(b"one", now=14.9, timeout=5.0)
    assert not watch.observe(b"one", now=15.0, timeout=5.0)
    assert watch.observe(b"two", now=15.0, timeout=5.0)

    paused = HeartbeatWatch()
    assert paused.observe(b"one", now=10.0, timeout=5.0)
    assert paused.observe(b"one", now=100.0, timeout=5.0)
    assert paused.observe(b"one", now=104.9, timeout=5.0)
    assert not paused.observe(b"one", now=105.0, timeout=5.0)


def test_heartbeat_monitor_ignores_slots_without_a_marker_fd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An incompletely constructed slot cannot make the monitor signal an unrelated PID."""

    async def scenario() -> None:
        pool = SkeletonPool(1, beat_rate=0.001)
        pool.loop = asyncio.get_running_loop()
        pool.started = True
        control, peer = socket.socketpair()
        slot = Slot.model_construct(
            identity=SkeletonID(fd=control.fileno(), gen=1),
            control=control,
            death_writer=-1,
            beat=Path("missing.beat"),
            beat_fd=-1,
            pid=1,
            started_at=0.0,
        )
        pool.slots[slot.identity.fd] = slot
        monkeypatch.setattr("reaper.pool.HEARTBEAT_MIN_TIMEOUT", 0.002)

        def unexpected_kill(pid: int, sent: int) -> None:
            raise AssertionError(f"unexpected kill({pid}, {sent})")

        monkeypatch.setattr("reaper.pool.os.kill", unexpected_kill)
        monitor = asyncio.create_task(pool.monitor_heartbeats())
        try:
            await asyncio.sleep(0.01)
            assert not monitor.done()
        finally:
            pool.closing = True
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            control.close()
            peer.close()

    asyncio.run(scenario())


def test_startup_cannot_return_after_losing_its_only_starting_slot() -> None:
    model = StartupModel(target=2)
    model.spawn(1)
    model.spawn(2)
    model.ready(1)
    model.lose(2)
    assert not model.return_if_ready()
    assert model.life is StartupLife.WAITING
    model.spawn(3)
    model.ready(3)
    assert model.return_if_ready()


@settings(max_examples=200, deadline=None)
@given(seed=strategies.integers(min_value=0, max_value=2**32 - 1))
def test_open_pool_eventually_drains_under_fair_replies(seed: int) -> None:
    scenario = ScenarioCompiler().compile(seed, target=4, count=100)
    model = ReaperModel(4)
    for event in scenario.events:
        model.apply(event)
    if model.core.closing:
        for slot in tuple(model.slots.values()):
            model.apply(ControlEvent(kind=EventKind.EOF, identity=slot.identity))
        assert not model.slots
        return
    limit = len(model.core.queue) + len(model.slots) + 1
    rounds = 0
    while model.core.queue or any(
        slot.state in {SkeletonState.STARTING, SkeletonState.RUNNING}
        for slot in model.slots.values()
    ):
        assert rounds < limit
        for slot in tuple(model.slots.values()):
            match slot.state:
                case SkeletonState.STARTING:
                    model.apply(ControlEvent(kind=EventKind.READY, identity=slot.identity))
                case SkeletonState.RUNNING:
                    model.apply(
                        ControlEvent(
                            kind=EventKind.RESULT,
                            identity=slot.identity,
                            job=slot.job,
                        )
                    )
                case _:
                    continue
        rounds += 1
    assert not model.core.queue
    assert all(slot.state is SkeletonState.IDLE for slot in model.slots.values())
