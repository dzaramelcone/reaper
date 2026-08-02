"""Check deterministic scheduling and executable fault scripts."""

import asyncio

import asyncpg

from reaper.pool import HeartbeatWatch
from reaper.runtime import RuntimeOperation
from tests.fault_runtime import FaultRuntime, ScriptedClock
from tests.faults import FaultStep, OutcomeKind
from tests.scheduler import DeterministicScheduler


def test_scheduler_releases_the_selected_actor_and_records_trace() -> None:
    async def check() -> None:
        scheduler = DeterministicScheduler()
        finished: list[str] = []

        async def actor(name: str) -> None:
            await scheduler.pause(name, "operation", "before")
            finished.append(name)

        first = asyncio.create_task(actor("first"))
        second = asyncio.create_task(actor("second"))
        points = await scheduler.wait_for(count=2, operation="operation")
        assert {point.actor for point in points} == {"first", "second"}
        await scheduler.release(1)
        await asyncio.sleep(0)
        assert finished == ["second"]
        await scheduler.release(0)
        await asyncio.gather(first, second)
        assert [point.actor for point in scheduler.trace().decisions] == [
            "second",
            "first",
        ]

    asyncio.run(check())


def test_fault_runtime_turns_delayed_send_into_a_checkpoint() -> None:
    async def check() -> None:
        scheduler = DeterministicScheduler()
        runtime = FaultRuntime(
            [FaultStep(call=RuntimeOperation.CONTROL_SEND, outcome=OutcomeKind.DELAYED)],
            scheduler,
        )
        sending = asyncio.create_task(runtime.send("writer", b"frame"))
        async with asyncio.timeout(1):
            points = await scheduler.wait_for(
                operation=RuntimeOperation.CONTROL_SEND.value,
                phase="delayed",
            )
        assert points[0].actor == "writer"
        await scheduler.release()
        assert await sending == len(b"frame")

    asyncio.run(check())


def test_fault_runtime_executes_clock_database_and_heartbeat_outcomes() -> None:
    async def check() -> None:
        scheduler = DeterministicScheduler()
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.CLOCK,
                    outcome=OutcomeKind.CLOCK_JUMP,
                    amount=7,
                ),
                FaultStep(
                    call=RuntimeOperation.DB_QUERY,
                    outcome=OutcomeKind.DEADLOCK,
                ),
                FaultStep(
                    call=RuntimeOperation.HEARTBEAT_READ,
                    outcome=OutcomeKind.SHORT,
                ),
            ],
            scheduler,
        )
        clock = ScriptedClock(start=3)
        assert await runtime.clock("timer", clock) == 10
        try:
            await runtime.database("worker")
        except asyncpg.DeadlockDetectedError as fault:
            assert fault.sqlstate == "40P01"
        else:
            raise AssertionError("deadlock fault was not injected")
        assert not await runtime.heartbeat("worker")

    asyncio.run(check())


def test_clock_jump_cannot_turn_one_heartbeat_sample_into_false_death() -> None:
    """A delayed parent probe establishes a new monotonic liveness baseline."""

    async def check() -> None:
        clock = ScriptedClock(start=10)
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.CLOCK,
                    outcome=OutcomeKind.CLOCK_JUMP,
                    amount=90,
                )
            ],
            DeterministicScheduler(),
        )
        watch = HeartbeatWatch()
        assert watch.observe(b"marker", float(clock.now()), timeout=5.0)
        jumped = await runtime.clock("reaper", clock)
        assert watch.observe(b"marker", float(jumped), timeout=5.0)
        assert watch.observe(b"marker", 104.9, timeout=5.0)
        assert not watch.observe(b"marker", 105.0, timeout=5.0)

    asyncio.run(check())
