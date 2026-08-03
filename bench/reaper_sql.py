"""Run durable graph workloads and report PostgreSQL pressure."""

import argparse
import asyncio
import json
import logging
import os
import resource
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, cast

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, JsonValue

import reaper
from reaper.log import configure_logging, write
from reaper.tasks import TaskExecution

QUERIES = Path(__file__).with_name("queries")
SNAPSHOT = (QUERIES / "snapshot.sql").read_text()
PRESSURE = (QUERIES / "pressure.sql").read_text()
CLEANUP = (QUERIES / "cleanup.sql").read_text()
VACUUM = (QUERIES / "vacuum.sql").read_text()
FLUSH_STATS = (QUERIES / "flush_stats.sql").read_text()
log = logging.getLogger(__name__)


class BenchmarkConfig(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    roots: Annotated[int, Field(ge=1)] = 8
    workers: Annotated[int, Field(ge=1)] = 24
    connections: Annotated[int, Field(ge=2)] = 32
    gather_width: Annotated[int, Field(ge=1)] = 16
    map_width: Annotated[int, Field(ge=1)] = 16
    dag_depth: Annotated[int, Field(ge=1)] = 4
    dag_width: Annotated[int, Field(ge=1)] = 6
    tree_depth: Annotated[int, Field(ge=1)] = 3
    tree_width: Annotated[int, Field(ge=1)] = 3
    chain_depth: Annotated[int, Field(ge=1)] = 32


class Pressure(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    connections: int
    active: int
    lock_waiters: int


class PressurePeak(BaseModel):
    model_config = ConfigDict(frozen=False, strict=True)

    connections: int = 0
    active: int = 0
    lock_waiters: int = 0

    def observe(self, pressure: Pressure) -> None:
        self.connections = max(self.connections, pressure.connections)
        self.active = max(self.active, pressure.active)
        self.lock_waiters = max(self.lock_waiters, pressure.lock_waiters)


class Workload(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    kind: str
    input: dict[str, JsonValue]
    tasks: int


class RunMetrics(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    prefix: str
    roots: int
    tasks: int
    workers: int
    connections: int
    seed_seconds: float
    execute_seconds: float
    tasks_per_second: float
    root_latency_ms: float
    topology_latency_ms: dict[str, dict[str, float]]
    client_peak_rss_bytes: int
    pressure: PressurePeak
    vacuum_seconds: float
    cleanup_seconds: float
    cleaned_roots: int
    database_delta: dict[str, Any]
    before_cleanup_sizes: dict[str, Any]
    after_vacuum_sizes: dict[str, Any]
    post_run_tables: dict[str, Any]
    post_vacuum_tables: dict[str, Any]


def graph_workloads(config: BenchmarkConfig) -> tuple[Workload, ...]:
    tree_tasks = sum(config.tree_width**level for level in range(config.tree_depth + 1))
    return (
        Workload(
            kind="gather",
            input={"kind": "gather", "width": config.gather_width},
            tasks=1 + config.gather_width,
        ),
        Workload(
            kind="mapreduce",
            input={"kind": "mapreduce", "width": config.map_width},
            tasks=1 + config.map_width,
        ),
        Workload(kind="diamond", input={"kind": "diamond"}, tasks=4),
        Workload(
            kind="dag",
            input={
                "kind": "dag",
                "depth": config.dag_depth,
                "width": config.dag_width,
            },
            tasks=1 + config.dag_depth * config.dag_width,
        ),
        Workload(
            kind="tree",
            input={
                "kind": "tree",
                "depth": config.tree_depth,
                "width": config.tree_width,
                "level": 0,
                "path": "root",
            },
            tasks=tree_tasks,
        ),
        Workload(
            kind="chain",
            input={"kind": "chain", "depth": config.chain_depth, "level": 0},
            tasks=1 + config.chain_depth,
        ),
    )


def as_object(value: JsonValue) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise TypeError("workflow input must be a JSON object")
    return value


def integer(params: Mapping[str, JsonValue], name: str) -> int:
    value = params[name]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def text(params: Mapping[str, JsonValue], name: str) -> str:
    value = params[name]
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    return value


def preload(execution: TaskExecution) -> dict[str, JsonValue]:
    return {item.id: item.result for item in execution.task.waits}


def child_params(
    execution: TaskExecution,
    child_id: str,
    function: str,
    payload: dict[str, JsonValue],
) -> reaper.SubmitCall:
    root_id = execution.task.root_id or execution.task.id
    return reaper.SubmitCall(
        id=child_id,
        root_id=root_id,
        function=function,
        input=payload,
        topic=execution.task.topic,
        expires_at=datetime.now(UTC) + timedelta(minutes=15),
    )


async def submit_children(
    execution: TaskExecution,
    children: Sequence[tuple[str, str, dict[str, JsonValue]]],
) -> None:
    await execution.submit_many(
        tuple(
            child_params(execution, child_id, function, payload)
            for child_id, function, payload in children
        )
    )


async def wait_or_sum(execution: TaskExecution, child_ids: Sequence[str]) -> bool:
    memo = preload(execution)
    if not all(child_id in memo for child_id in child_ids):
        await execution.suspend(tuple(child_ids))
        return False
    await execution.complete(sum(cast(int, memo[child_id]) for child_id in child_ids))
    return True


async def execute_task(execution: TaskExecution) -> bool:
    params = as_object(execution.task.input)
    kind = execution.task.function

    if kind == "leaf":
        await execution.complete(integer(params, "value"))
        return True

    if kind in {"gather", "mapreduce"}:
        width = integer(params, "width")
        child_ids = [f"{execution.task.id}:item:{index}" for index in range(width)]
        if not execution.task.waits:
            children: list[tuple[str, str, dict[str, JsonValue]]] = [
                (
                    child_id,
                    "leaf",
                    {"value": index if kind == "gather" else index * index},
                )
                for index, child_id in enumerate(child_ids)
            ]
            await submit_children(execution, children)
        return await wait_or_sum(execution, child_ids)

    if kind == "diamond":
        shared = f"{execution.task.id}:shared"
        sides = [f"{execution.task.id}:{side}" for side in ("left", "right")]
        if not execution.task.waits:
            await submit_children(
                execution,
                [
                    (shared, "leaf", {"value": 2}),
                    (sides[0], "diamond_side", {"shared": shared, "value": 3}),
                    (sides[1], "diamond_side", {"shared": shared, "value": 5}),
                ],
            )
        return await wait_or_sum(execution, sides)

    if kind == "diamond_side":
        shared = text(params, "shared")
        memo = preload(execution)
        if shared not in memo:
            await execution.suspend((shared,))
            return False
        await execution.complete(cast(int, memo[shared]) + integer(params, "value"))
        return True

    if kind == "dag":
        depth = integer(params, "depth")
        width = integer(params, "width")
        final_ids = [f"{execution.task.id}:layer:{depth - 1}:{index}" for index in range(width)]
        if not execution.task.waits:
            children = [
                (
                    f"{execution.task.id}:layer:{level}:{index}",
                    "dag_node",
                    {
                        "root": execution.task.id,
                        "level": level,
                        "index": index,
                        "width": width,
                    },
                )
                for level in range(depth)
                for index in range(width)
            ]
            await submit_children(execution, children)
        return await wait_or_sum(execution, final_ids)

    if kind == "dag_node":
        level = integer(params, "level")
        index = integer(params, "index")
        width = integer(params, "width")
        if level == 0:
            await execution.complete(index + 1)
            return True
        root = text(params, "root")
        dependencies = [f"{root}:layer:{level - 1}:{item}" for item in range(width)]
        return await wait_or_sum(execution, dependencies)

    if kind == "tree":
        level = integer(params, "level")
        depth = integer(params, "depth")
        width = integer(params, "width")
        path = text(params, "path")
        if level == depth:
            await execution.complete(1)
            return True
        child_ids = [f"{execution.task.id}:child:{index}" for index in range(width)]
        if not execution.task.waits:
            await submit_children(
                execution,
                [
                    (
                        child_id,
                        "tree",
                        {
                            "level": level + 1,
                            "depth": depth,
                            "width": width,
                            "path": f"{path}.{index}",
                        },
                    )
                    for index, child_id in enumerate(child_ids)
                ],
            )
        return await wait_or_sum(execution, child_ids)

    if kind == "chain":
        level = integer(params, "level")
        depth = integer(params, "depth")
        if level == depth:
            await execution.complete(1)
            return True
        child_id = f"{execution.task.id}:next"
        if not execution.task.waits:
            await submit_children(
                execution,
                [(child_id, "chain", {"level": level + 1, "depth": depth})],
            )
        return await wait_or_sum(execution, (child_id,))

    raise RuntimeError(f"unknown benchmark function {kind!r}")


async def snapshot(pool: asyncpg.Pool[asyncpg.Record]) -> dict[str, Any]:
    async with pool.acquire() as executor:
        value = await executor.fetchval(SNAPSHOT)
    return cast(dict[str, Any], json.loads(cast(str, value)))


def counter_delta(before: Any, after: Any) -> Any:
    if isinstance(before, dict) and isinstance(after, dict):
        return {
            key: counter_delta(before.get(key), value)
            for key, value in after.items()
            if key in before
        }
    if isinstance(before, (int, float)) and isinstance(after, (int, float)):
        return after - before
    return after


def peak_rss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


async def sample_pressure(
    pool: asyncpg.Pool[asyncpg.Record],
    stop: asyncio.Event,
    peak: PressurePeak,
) -> None:
    while not stop.is_set():
        async with pool.acquire() as executor:
            row = await executor.fetchrow(PRESSURE)
        if row is None:
            raise RuntimeError("PostgreSQL returned no pressure sample")
        peak.observe(Pressure.model_validate(dict(row)))
        await asyncio.sleep(0.025)


async def run_workers(
    store: reaper.Store,
    topic: str,
    workers: int,
    expected: int,
) -> dict[str, dict[str, float]]:
    completed = 0
    done = asyncio.Event()
    started = time.perf_counter()
    root_latencies: dict[str, list[float]] = {}

    async def worker() -> None:
        nonlocal completed
        while not done.is_set():
            try:
                async with store.tasks.claim(topic) as execution:
                    if execution is None:
                        await asyncio.sleep(0.01)
                        continue
                    settled = await execute_task(execution)
            except asyncpg.DeadlockDetectedError as error:
                write(
                    log,
                    logging.WARNING,
                    "task transaction deadlocked; retrying",
                    topic=topic,
                    sqlstate=error.sqlstate,
                )
                await asyncio.sleep(0)
                continue
            if settled:
                completed += 1
                if execution.task.root_id is None:
                    root_latencies.setdefault(execution.task.function, []).append(
                        (time.perf_counter() - started) * 1000
                    )
                if completed == expected:
                    done.set()

    async with asyncio.TaskGroup() as group:
        for _ in range(workers):
            group.create_task(worker())
        await done.wait()
    if completed != expected:
        raise RuntimeError(f"completed {completed} tasks; expected {expected}")
    return {
        kind: {
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "max": max(latencies),
        }
        for kind, latencies in root_latencies.items()
    }


def percentile(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(values)
    index = min(round((len(ordered) - 1) * quantile), len(ordered) - 1)
    return ordered[index]


def summarize(metrics: RunMetrics) -> dict[str, Any]:
    delta = metrics.database_delta
    database = cast(dict[str, Any], delta["database"])
    wal = cast(dict[str, Any], delta["wal"])
    tables = cast(dict[str, dict[str, Any]], delta["tables"])
    io = cast(dict[str, dict[str, Any]], delta["io"])
    return {
        "workload": {
            "roots": metrics.roots,
            "tasks": metrics.tasks,
            "workers": metrics.workers,
            "connections": metrics.connections,
            "seed_seconds": metrics.seed_seconds,
            "execute_seconds": metrics.execute_seconds,
            "tasks_per_second": metrics.tasks_per_second,
            "mean_root_latency_ms": metrics.root_latency_ms,
            "topology_latency_ms": metrics.topology_latency_ms,
        },
        "client": {
            "peak_rss_bytes": metrics.client_peak_rss_bytes,
            "connections_peak": metrics.pressure.connections,
            "active_peak": metrics.pressure.active,
            "lock_waiters_peak": metrics.pressure.lock_waiters,
        },
        "postgres": {
            "wal": {
                key: wal.get(key)
                for key in ("wal_bytes", "wal_records", "wal_fpi", "wal_buffers_full")
            },
            "transactions": {
                key: database.get(key)
                for key in ("xact_commit", "xact_rollback", "deadlocks", "conflicts")
            },
            "buffers": {
                key: database.get(key)
                for key in ("blks_read", "blks_hit", "temp_files", "temp_bytes")
            },
            "tuples": {
                key: database.get(key)
                for key in ("tup_inserted", "tup_updated", "tup_deleted", "tup_fetched")
            },
            "tables": {
                name: {
                    key: values.get(key)
                    for key in (
                        "n_tup_ins",
                        "n_tup_upd",
                        "n_tup_hot_upd",
                        "n_tup_del",
                        "seq_scan",
                        "idx_scan",
                        "n_live_tup",
                        "n_dead_tup",
                    )
                }
                for name, values in tables.items()
            },
            "io": io,
            "retained_bytes_before_cleanup": sum(
                cast(int, size["total_bytes"]) for size in metrics.before_cleanup_sizes.values()
            ),
        },
        "cleanup": {
            "roots": metrics.cleaned_roots,
            "seconds": metrics.cleanup_seconds,
            "vacuum_seconds": metrics.vacuum_seconds,
            "post_vacuum_sizes": metrics.after_vacuum_sizes,
            "dead_tuples_before_vacuum": {
                name: values.get("n_dead_tup") for name, values in metrics.post_run_tables.items()
            },
            "dead_tuples_after_vacuum": {
                name: values.get("n_dead_tup")
                for name, values in metrics.post_vacuum_tables.items()
            },
        },
    }


async def flush_stats(pool: asyncpg.Pool[asyncpg.Record], connection_count: int) -> None:
    connections = await asyncio.gather(*(pool.acquire() for _ in range(connection_count)))
    try:
        await asyncio.gather(*(connection.fetchval(FLUSH_STATS) for connection in connections))
    finally:
        await asyncio.gather(*(pool.release(connection) for connection in connections))


async def benchmark(config: BenchmarkConfig, dsn: str) -> RunMetrics:
    pool_size = config.connections
    pool = await asyncpg.create_pool(
        dsn,
        min_size=pool_size,
        max_size=pool_size,
    )
    store = reaper.Store(pool)
    prefix = f"bench-{uuid.uuid4().hex}"
    topic = prefix
    workloads = graph_workloads(config)
    root_count = config.roots * len(workloads)
    expected_tasks = config.roots * sum(workload.tasks for workload in workloads)
    peak = PressurePeak()
    stop_sampling = asyncio.Event()
    sampler: asyncio.Task[None] | None = None
    try:
        await flush_stats(pool, pool_size)
        before = await snapshot(pool)
        seed_started = time.perf_counter()
        roots: list[reaper.SubmitCall] = []
        for workload in workloads:
            for index in range(config.roots):
                roots.append(
                    reaper.SubmitCall(
                        id=f"{prefix}:{workload.kind}:{index}",
                        function=workload.kind,
                        input=workload.input,
                        topic=topic,
                    )
                )
        await store.tasks.submit_many(tuple(roots))
        seed_seconds = time.perf_counter() - seed_started

        sampler = asyncio.create_task(sample_pressure(pool, stop_sampling, peak))
        execute_started = time.perf_counter()
        async with asyncio.timeout(120):
            topology_latency = await run_workers(store, topic, config.workers, expected_tasks)
        execute_seconds = time.perf_counter() - execute_started
        stop_sampling.set()
        await sampler
        sampler = None
        await flush_stats(pool, pool_size)
        after = await snapshot(pool)

        cleanup_started = time.perf_counter()
        async with pool.acquire() as executor:
            cleaned = len(await executor.fetch(CLEANUP, prefix))
        cleanup_seconds = time.perf_counter() - cleanup_started
        await flush_stats(pool, pool_size)

        vacuum_started = time.perf_counter()
        async with pool.acquire() as executor:
            await executor.execute(VACUUM)
        vacuum_seconds = time.perf_counter() - vacuum_started
        await flush_stats(pool, pool_size)
        post_vacuum = await snapshot(pool)
        return RunMetrics(
            prefix=prefix,
            roots=root_count,
            tasks=expected_tasks,
            workers=config.workers,
            connections=config.connections,
            seed_seconds=seed_seconds,
            execute_seconds=execute_seconds,
            tasks_per_second=expected_tasks / execute_seconds,
            root_latency_ms=execute_seconds * 1000 / root_count,
            topology_latency_ms=topology_latency,
            client_peak_rss_bytes=peak_rss_bytes(),
            pressure=peak,
            vacuum_seconds=vacuum_seconds,
            cleanup_seconds=cleanup_seconds,
            cleaned_roots=cleaned,
            database_delta=counter_delta(before, after),
            before_cleanup_sizes=cast(dict[str, Any], after["sizes"]),
            after_vacuum_sizes=cast(dict[str, Any], post_vacuum["sizes"]),
            post_run_tables=cast(dict[str, Any], after["tables"]),
            post_vacuum_tables=cast(dict[str, Any], post_vacuum["tables"]),
        )
    finally:
        stop_sampling.set()
        if sampler is not None:
            await sampler
        await pool.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "REAPER_POSTGRES_DSN",
            "postgresql://reaper:reaper@127.0.0.1:55433/reaper",
        ),
    )
    parser.add_argument("--roots", type=int, default=8)
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--connections", type=int, default=32)
    return parser.parse_args()


def main() -> None:
    configure_logging("WARNING")
    args = parse_args()
    config = BenchmarkConfig(
        roots=args.roots,
        workers=args.workers,
        connections=args.connections,
    )
    metrics = asyncio.run(benchmark(config, args.dsn))
    print(json.dumps(summarize(metrics), indent=2))


if __name__ == "__main__":
    main()
