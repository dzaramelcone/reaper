"""Race checks that use real child trees."""

import asyncio
import os
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path

import pytest

from reaper.control import SkeletonState
from reaper.pool import RemoteWorkerError, SkeletonPool
from reaper.runtime import RuntimeOperation
from tests.fault_runtime import FaultHooks, FaultRuntime
from tests.faults import FaultStep, OutcomeKind
from tests.scheduler import DeterministicScheduler
from tests.workers import add_one, churn_tree, fail_some, paced, tree_sum, twice


async def wait_state(check: Callable[[], bool], ticks: int = 1_000) -> None:
    left = ticks
    while left:
        if check():
            return
        await asyncio.sleep(0.002)
        left -= 1
    raise AssertionError("pool state did not settle")


def process_exists(pid: int) -> bool:
    """Check whether one PID still has a process row."""

    result = subprocess.run(
        ("ps", "-p", str(pid)),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def child_processes(pid: int) -> list[int]:
    """List direct child PIDs with the portable ps fields available on macOS."""

    result = subprocess.run(
        ("ps", "-Ao", "pid=,ppid="),
        capture_output=True,
        text=True,
        check=False,
    )
    rows = (line.split() for line in result.stdout.splitlines())
    return [int(child) for child, parent in rows if int(parent) == pid]


@pytest.mark.postgres
@pytest.mark.stress
def test_systemd_sigterm_stops_the_daemon_and_all_skeletons_cleanly() -> None:
    """SIGTERM must produce exit zero, no traceback, and no surviving children."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")
    environment = os.environ.copy()
    environment["REAPER_POSTGRES_DSN"] = dsn
    process = subprocess.Popen(
        (sys.executable, "-m", "reaper.cli", "--pool", "2"),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=environment,
    )
    descendants: list[int] = []
    try:
        left = 1_000
        while left:
            descendants = child_processes(process.pid)
            if len(descendants) == 2:
                break
            assert process.poll() is None
            time.sleep(0.01)
            left -= 1
        assert len(descendants) == 2
        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=10)
        assert process.returncode == 0
        assert "Traceback" not in output
        assert "KeyboardInterrupt" not in output
        assert not any(process_exists(pid) for pid in descendants)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


@pytest.mark.stress
def test_reaper_death_leaves_no_orphan_or_zombie_tree() -> None:
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder)
        process = subprocess.Popen(
            (sys.executable, "-m", "tests.reaper_daemon", str(path)),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        files: list[Path] = []
        left = 1_000
        while left:
            files = list(path.glob("*.pid"))
            if len(files) == 8:
                break
            time.sleep(0.01)
            left -= 1
        assert len(files) == 8
        descendants = [
            int(file.stem.rpartition("-")[2]) for file in files if file.name.startswith("skeleton-")
        ]
        assert len(descendants) == 7
        assert all(process_exists(pid) for pid in descendants)
        process.kill()
        assert process.wait(timeout=5) != 0
        left = 1_000
        while left and any(process_exists(pid) for pid in descendants):
            time.sleep(0.01)
            left -= 1
        assert not any(process_exists(pid) for pid in descendants)


@pytest.mark.stress
def test_hard_stopping_a_skeleton_does_not_orphan_its_bash_process() -> None:
    """The supervisor must terminate the complete skeleton process group."""

    async def check() -> None:
        with tempfile.TemporaryDirectory() as folder:
            pid_file = Path(folder) / "bash.pid"
            pool = SkeletonPool(1, beat_rate=0.01)
            await pool.start()
            running = asyncio.create_task(
                pool.run_bash(f"printf '%s' \"$$\" > {shlex.quote(str(pid_file))}; exec sleep 30")
            )
            await wait_state(pid_file.exists)
            bash_pid = int(pid_file.read_text())
            try:
                assert process_exists(bash_pid)
                await pool.close(timeout=0.02)
                await asyncio.gather(running, return_exceptions=True)
                await wait_state(lambda: not process_exists(bash_pid))
            finally:
                if process_exists(bash_pid):
                    os.kill(bash_pid, signal.SIGKILL)
                    with suppress(ChildProcessError):
                        os.waitpid(bash_pid, 0)
                if pool.started:
                    await pool.close(timeout=0.02)

    asyncio.run(check())


@pytest.mark.stress
def test_mixed_batch_keeps_all_slots_live() -> None:
    async def check() -> None:
        async with SkeletonPool(4, beat_rate=0.01) as pool:
            jobs = [pool.run_async(fail_some, value, 7) for value in range(120)]
            results = await asyncio.wait_for(
                asyncio.gather(*jobs, return_exceptions=True),
                timeout=15,
            )
            faults = [item for item in results if isinstance(item, RemoteWorkerError)]
            values = [item for item in results if isinstance(item, int)]
            assert len(faults) == 18
            assert len(values) == 102
            assert len(pool.status()) == pool.target
            assert all(row[1] is SkeletonState.IDLE for row in pool.status())

    asyncio.run(check())


@pytest.mark.stress
def test_rapid_death_reloads_each_slot_gen() -> None:
    async def check() -> None:
        async with SkeletonPool(3, beat_rate=0.01) as pool:
            first_gen = max(row[0].gen for row in pool.status())
            rounds = 18
            for round_id in range(rounds):
                jobs = [
                    asyncio.create_task(pool.run_async(paced, round_id + branch, 200))
                    for branch in range(pool.target)
                ]
                await wait_state(
                    lambda: (
                        sum(row[1] is SkeletonState.RUNNING for row in pool.status()) == pool.target
                    )
                )
                victim = min(pool.slots.values(), key=lambda slot: slot.identity.gen)
                os.kill(victim.pid, signal.SIGKILL)
                results = await asyncio.wait_for(
                    asyncio.gather(*jobs, return_exceptions=True),
                    timeout=5,
                )
                assert any(isinstance(item, RemoteWorkerError) for item in results)
                await wait_state(
                    lambda: (
                        len(pool.status()) == pool.target
                        and all(row[1] is SkeletonState.IDLE for row in pool.status())
                    )
                )
                ids = {(row[0].fd, row[0].gen) for row in pool.status()}
                assert len(ids) == pool.target
            last_gen = max(row[0].gen for row in pool.status())
            assert last_gen >= first_gen + rounds
            assert not list(pool.beat_dir.glob("slot-*.beat"))
            assert all(slot.beat_fd >= 0 for slot in pool.slots.values())
            values = await asyncio.gather(
                *(pool.run_async(add_one, value) for value in range(pool.target))
            )
            assert values == [1, 2, 3]

    asyncio.run(check())


@pytest.mark.stress
def test_alternating_control_faults_do_not_lose_replacement_ready_frames() -> None:
    """Repeated stale readers must never consume a replacement slot's traffic."""

    async def check() -> None:
        rounds = 20
        steps = [
            step
            for _ in range(rounds)
            for step in (
                FaultStep(
                    call=RuntimeOperation.CONTROL_SEND,
                    outcome=OutcomeKind.PERMANENT_ERROR,
                    site="control",
                ),
                FaultStep(
                    call=RuntimeOperation.CONTROL_RECEIVE,
                    outcome=OutcomeKind.PERMANENT_ERROR,
                    site="control",
                ),
            )
        ]
        runtime = FaultRuntime(steps, DeterministicScheduler())
        async with SkeletonPool(1, beat_rate=0.01, hooks=FaultHooks(runtime)) as pool:
            first_generation = pool.status()[0][0].gen
            for round_id in range(rounds):
                result = await asyncio.wait_for(
                    asyncio.gather(
                        pool.run_sync(twice, round_id),
                        return_exceptions=True,
                    ),
                    timeout=5,
                )
                assert isinstance(result[0], RemoteWorkerError)
                expected_steps = (rounds - round_id - 1) * 2

                def replacements_ready(expected: int = expected_steps) -> bool:
                    return (
                        len(runtime.steps) == expected
                        and len(pool.status()) == 1
                        and pool.status()[0][1] is SkeletonState.IDLE
                    )

                await wait_state(replacements_ready)
            assert not runtime.steps
            assert pool.status()[0][0].gen >= first_generation + rounds * 2
            assert await pool.run_async(add_one, 9) == 10

    asyncio.run(check())


@pytest.mark.stress
def test_nested_tree_batches_finish() -> None:
    async def check() -> None:
        async with SkeletonPool(2, beat_rate=0.01) as pool:
            values = await asyncio.wait_for(
                asyncio.gather(*(pool.run_async(tree_sum, 2, 2, value) for value in range(8))),
                timeout=20,
            )
            assert values == [4 * value + 4 for value in range(8)]
            assert len(pool.status()) == pool.target

    asyncio.run(check())


@pytest.mark.stress
def test_nested_trees_churn_their_own_slots() -> None:
    async def check() -> None:
        async with SkeletonPool(2, beat_rate=0.01) as pool:
            jobs = []
            left = 6
            while left:
                jobs.append(pool.run_async(churn_tree, 6, 2))
                left -= 1
            results = await asyncio.wait_for(
                asyncio.gather(*jobs),
                timeout=30,
            )
            assert all(last >= first + 6 for first, last in results)
            assert len(pool.status()) == pool.target

    asyncio.run(check())
