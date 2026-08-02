"""Control effect order with no live syscalls."""

from reaper.control import ControlEvent, EffectKind, EventKind, SkeletonID, SkeletonState
from tests.fake_control import DeterministicDriver, FakeClock


def test_spawn_replies_can_arrive_out_of_order() -> None:
    driver = DeterministicDriver(2)
    driver.deliver(ControlEvent(kind=EventKind.START))
    second = driver.take(1)
    first = driver.take(0)
    assert second.effect.gen == 2
    driver.reply(
        second,
        ControlEvent(
            kind=EventKind.SPAWNED,
            identity=SkeletonID(fd=10, gen=2),
            gen=2,
        ),
    )
    driver.reply(
        first,
        ControlEvent(
            kind=EventKind.SPAWNED,
            identity=SkeletonID(fd=11, gen=1),
            gen=1,
        ),
    )
    assert [slot.identity.gen for slot in driver.view().slots] == [2, 1]
    assert driver.view().pending_spawns == ()


def test_send_fail_and_stale_eof_have_exact_effects() -> None:
    driver = DeterministicDriver(1)
    driver.deliver(ControlEvent(kind=EventKind.START))
    spawn = driver.take()
    identity = SkeletonID(fd=10, gen=1)
    driver.reply(
        spawn,
        ControlEvent(kind=EventKind.SPAWNED, identity=identity, gen=1),
    )
    driver.deliver(ControlEvent(kind=EventKind.READY, identity=identity))
    driver.deliver(ControlEvent(kind=EventKind.SUBMIT, job="job"))
    run = driver.take()
    assert run.effect.kind == EffectKind.SEND_RUN
    driver.reply(
        run,
        ControlEvent(kind=EventKind.SEND_FAILED, identity=identity, job="job"),
    )
    kinds = [call.effect.kind for call in driver.effects]
    assert kinds == [
        EffectKind.DROP,
        EffectKind.CLOSE_CONTROL,
        EffectKind.CLOSE_DEATH,
        EffectKind.REAP,
        EffectKind.FAIL,
    ]
    driver.deliver(ControlEvent(kind=EventKind.EOF, identity=identity))
    assert [call.effect.kind for call in driver.effects] == kinds


def test_pre_ready_worker_loss_requires_delayed_respawn() -> None:
    """Do not turn a persistent startup fault into a tight spawn loop."""

    driver = DeterministicDriver(1)
    driver.deliver(ControlEvent(kind=EventKind.START))
    spawn = driver.take()
    identity = SkeletonID(fd=10, gen=1)
    driver.reply(
        spawn,
        ControlEvent(kind=EventKind.SPAWNED, identity=identity, gen=1),
    )

    driver.deliver(ControlEvent(kind=EventKind.EOF, identity=identity))

    assert [call.effect.kind for call in driver.effects] == [
        EffectKind.DROP,
        EffectKind.CLOSE_CONTROL,
        EffectKind.CLOSE_DEATH,
        EffectKind.REAP,
    ]


def test_fake_clock_orders_close_deadline() -> None:
    driver = DeterministicDriver(1)
    clock = FakeClock(start=10.0)
    driver.deliver(ControlEvent(kind=EventKind.START))
    spawn = driver.take()
    identity = SkeletonID(fd=10, gen=1)
    driver.reply(
        spawn,
        ControlEvent(kind=EventKind.SPAWNED, identity=identity, gen=1),
    )
    driver.deliver(ControlEvent(kind=EventKind.READY, identity=identity))
    driver.deliver(ControlEvent(kind=EventKind.CLOSE))
    assert driver.view().slots[0].state == SkeletonState.STOPPING
    assert [call.effect.kind for call in driver.effects] == [EffectKind.SEND_STOP]
    clock.schedule(5.0, ControlEvent(kind=EventKind.DEADLINE))
    assert clock.advance(4.0) == ()
    due = clock.advance(1.0)
    assert len(due) == 1
    driver.deliver(due[0])
    assert driver.effects[-1].effect.kind == EffectKind.KILL
