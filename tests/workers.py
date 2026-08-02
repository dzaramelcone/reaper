"""Import-safe jobs for spawned pool tests."""

import asyncio
import os
import signal
import time
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path

from pydantic import JsonValue

from reaper.pool import RemoteWorkerError, SkeletonPool
from reaper.promise import Context, DurableFunction, durable
from reaper.promises.models import PromiseRecord, SubmitTimer
from reaper.tasks.models import SubmitCall


def twice(value: int) -> int:
    return value * 2


def fail_sync() -> None:
    raise ValueError("sync fault")


async def add_one(value: int) -> int:
    return value + 1


async def fail_async() -> None:
    raise ValueError("async fault")


async def paced(value: int, steps: int = 3) -> int:
    left = steps
    while left:
        await asyncio.sleep(0)
        left -= 1
    return value


async def block_loop(seconds: float) -> None:
    time.sleep(seconds)


async def fail_some(value: int, divisor: int) -> int:
    await asyncio.sleep(0)
    if value % divisor == 0:
        raise ValueError(f"fail {value}")
    return value


@durable(execution_timeout=1.0, topic="workflow")
async def promise_value(value: int) -> int:
    return value


@durable(execution_timeout=1.0, topic="workflow")
async def promise_add(left: int, right: int) -> int:
    return left + right


@durable(execution_timeout=1.0, topic="workflow")
async def promise_mul(left: int, right: int) -> int:
    return left * right


@durable(execution_timeout=1.0, topic="workflow")
async def promise_square(value: int) -> int:
    return value * value


@durable(execution_timeout=1.0, topic="workflow")
async def promise_sum(values: list[int]) -> int:
    return sum(values)


@durable(execution_timeout=1.0, topic="workflow")
async def promise_hold(value: int, delay: float) -> int:
    await asyncio.sleep(delay)
    return value


@durable(execution_timeout=30.0, topic="workflow")
async def square_workflow(values: list[int]) -> int:
    squares = await durable.gather(*(promise_square(value) for value in values))
    return sum(promise.result() for promise in squares)


@durable(execution_timeout=30.0, topic="workflow")
async def diamond_workflow() -> int:
    roots = await durable.gather(promise_value(3))
    root = roots[0].result()
    branches = await durable.gather(
        promise_add(root, 4),
        promise_mul(root, 5),
    )
    return sum(promise.result() for promise in branches)


@durable(execution_timeout=30.0, topic="workflow")
async def gather_workflow() -> int:
    promises = await durable.gather(*(promise_value(value) for value in range(8)))
    return sum(promise.result() for promise in promises)


@durable(execution_timeout=30.0, topic="workflow")
async def fanout_workflow() -> int:
    promises = await durable.fanout(promise_value, range(10, 16))
    return sum(promise.result() for promise in promises)


@durable(execution_timeout=30.0, topic="workflow")
async def join_workflow() -> int:
    joined = await durable.join(
        promise_sum,
        promise_value(2),
        promise_value(3),
        promise_value(5),
    )
    return joined.result()


@durable(execution_timeout=30.0, topic="workflow")
async def first_workflow() -> int:
    winner = await durable.first(
        promise_hold(1, 0.08),
        promise_hold(2, 0.01),
        promise_hold(3, 0.05),
    )
    return winner.result()


@durable(execution_timeout=30.0, topic="workflow")
async def mapreduce_workflow() -> int:
    reduced = await durable.mapreduce(
        promise_square,
        promise_sum,
        range(1, 7),
    )
    return reduced.result()


@durable(execution_timeout=30.0, topic="workflow")
async def dag_workflow() -> int:
    roots = await durable.gather(promise_value(2), promise_value(7))
    root = [promise.result() for promise in roots]
    middle = await durable.gather(
        promise_add(root[0], root[1]),
        promise_mul(root[0], root[1]),
        promise_mul(root[1], root[1]),
    )
    values = [promise.result() for promise in middle]
    edge = await durable.gather(promise_add(values[0], values[1]))
    end = await durable.gather(promise_sum([edge[0].result(), values[2]]))
    return end[0].result()


@durable(execution_timeout=30.0, topic="workflow")
async def dynamic_dag_workflow() -> int:
    frontier = [1]
    for width in (2, 3, 2):
        nodes = await durable.gather(
            *(promise_add(parent, branch) for parent in frontier for branch in range(1, width + 1))
        )
        frontier = [promise.result() for promise in nodes]
    end = await durable.gather(promise_sum(frontier))
    return end[0].result()


@durable(execution_timeout=30.0, topic="workflow")
async def durable_timer_workflow() -> str:
    await durable.sleep(timedelta(milliseconds=50))
    return "awake"


class WorkerStore:
    """Reject child calls outside the test graph."""

    retention_ms = 604_800_000

    async def submit_call(self, params: SubmitCall) -> PromiseRecord:
        raise RuntimeError(f"nested call {params.id} to {params.function} has no route")

    async def read_promise(self, promise_id: str) -> PromiseRecord:
        raise RuntimeError(f"promise {promise_id!r} has no route")

    async def submit_timer(self, params: SubmitTimer) -> PromiseRecord:
        raise RuntimeError(f"timer {params.id!r} has no route")


async def run_promise_task(
    func: str,
    args: Mapping[str, object],
) -> JsonValue:
    tasks: dict[str, DurableFunction[..., int]] = {
        promise_value.name: promise_value,
        promise_add.name: promise_add,
        promise_mul.name: promise_mul,
        promise_square.name: promise_square,
        promise_sum.name: promise_sum,
        promise_hold.name: promise_hold,
    }
    task = tasks.get(func)
    if task is None:
        raise ValueError(f"unknown durable task {func}")
    context = Context(store=WorkerStore(), task_id="pool-test")
    result = await task.execute(args, context)
    if result.error is not None:
        raise RuntimeError(result.error.text)
    return result.value


async def nested_wait(path: Path) -> None:
    async with SkeletonPool(1, beat_rate=0.02, beat_dir=path):
        await asyncio.sleep(30)


async def living_tree(path: Path, depth: int, width: int) -> None:
    """Keep one recorded worker tree alive."""

    path.joinpath(f"skeleton-{os.getpid()}.pid").touch()
    if not depth:
        await asyncio.sleep(30)
        return
    async with SkeletonPool(width, beat_rate=0.02) as reaper:
        await asyncio.gather(
            *(reaper.run_async(living_tree, path, depth - 1, width) for _ in range(width))
        )


async def tree_sum(depth: int, width: int, value: int) -> int:
    if depth == 0:
        return value
    async with SkeletonPool(width, beat_rate=0.02) as pool:
        jobs = [
            pool.run_async(tree_sum, depth - 1, width, value + branch) for branch in range(width)
        ]
        values = await asyncio.gather(*jobs)
    return sum(values)


async def churn_tree(rounds: int, width: int) -> tuple[int, int]:
    async with SkeletonPool(width, beat_rate=0.01) as pool:
        first_gen = max(row[0].gen for row in pool.status())
        for round_id in range(rounds):
            jobs = [
                asyncio.create_task(pool.run_async(paced, round_id + branch, 1_000))
                for branch in range(width)
            ]
            while sum(row[1].value == "running" for row in pool.status()) != width:
                await asyncio.sleep(0)
            victim = min(pool.slots.values(), key=lambda slot: slot.identity.gen)
            os.kill(victim.pid, signal.SIGKILL)
            results = await asyncio.gather(*jobs, return_exceptions=True)
            assert any(isinstance(result, RemoteWorkerError) for result in results)
            while len(pool.status()) != width:
                await asyncio.sleep(0)
            while any(row[1].value == "starting" for row in pool.status()):
                await asyncio.sleep(0)
        last_gen = max(row[0].gen for row in pool.status())
        checks = await asyncio.gather(*(pool.run_async(add_one, value) for value in range(width)))
        assert checks == [value + 1 for value in range(width)]
        return first_gen, last_gen
