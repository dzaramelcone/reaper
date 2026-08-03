"""Collect actual PostgreSQL plans for Reaper's hot SQL paths."""

import asyncio
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import asyncpg

import reaper
from reaper.database import TransactionExecutor
from reaper.maintenance.queries import DELETE_EXPIRED, PROCESS_DUE
from reaper.tasks.queries import CLAIM, SETTLE, SUBMIT_CALLS
from reaper.waits.queries import SUSPEND_TASK

BENCH_QUERIES = Path(__file__).with_name("queries")
CLEANUP = (BENCH_QUERIES / "cleanup.sql").read_text()
EXPLAIN = "EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) "


def plan_nodes(node: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = [
        {
            key: node[key]
            for key in (
                "Node Type",
                "Relation Name",
                "Index Name",
                "Actual Rows",
                "Actual Loops",
                "Rows Removed by Filter",
                "Shared Hit Blocks",
                "Shared Read Blocks",
                "WAL Records",
                "WAL Bytes",
            )
            if key in node
        }
    ]
    for child in cast(list[dict[str, Any]], node.get("Plans", [])):
        result.extend(plan_nodes(child))
    return result


async def explain(
    connection: TransactionExecutor,
    name: str,
    query: str,
    *args: object,
) -> dict[str, Any]:
    transaction = connection.transaction()
    await transaction.start()
    try:
        raw = await connection.fetchval(EXPLAIN + query, *args)
    finally:
        await transaction.rollback()
    document = cast(list[dict[str, Any]], json.loads(cast(str, raw)))[0]
    return {
        "name": name,
        "planning_ms": document["Planning Time"],
        "execution_ms": document["Execution Time"],
        "nodes": plan_nodes(cast(dict[str, Any], document["Plan"])),
    }


async def seed_ready(store: reaper.Store, prefix: str, count: int) -> str:
    topic = f"{prefix}:ready"
    await store.tasks.submit_many(
        tuple(
            reaper.SubmitCall(
                id=f"{prefix}:ready:{index}",
                function="plan.ready",
                input={"index": index},
                topic=topic,
            )
            for index in range(count)
        )
    )
    return topic


async def seed_future(store: reaper.Store, prefix: str, count: int) -> str:
    topic = f"{prefix}:future"
    available_at = datetime.now(UTC) + timedelta(days=1)
    await store.tasks.submit_many(
        tuple(
            reaper.SubmitCall(
                id=f"{prefix}:future:{index}",
                function="plan.future",
                input={"index": index},
                topic=topic,
                available_at=available_at,
            )
            for index in range(count)
        )
    )
    return topic


async def seed_fan_in(store: reaper.Store, prefix: str, waiters: int) -> tuple[str, str]:
    root_id = f"{prefix}:fan-in"
    shared_id = f"{root_id}:shared"
    topic = f"{prefix}:waiters"
    await store.tasks.submit(
        reaper.SubmitCall(
            id=root_id,
            function="plan.root",
            input=None,
            topic=f"{prefix}:root",
        )
    )
    async with store.tasks.claim(f"{prefix}:root") as execution:
        assert execution is not None
        await execution.submit(
            reaper.SubmitCall(
                id=shared_id,
                root_id=root_id,
                function="plan.shared",
                input=None,
                topic=f"{prefix}:shared",
                available_at=datetime.now(UTC) + timedelta(days=1),
            )
        )
        for index in range(waiters):
            await execution.submit(
                reaper.SubmitCall(
                    id=f"{root_id}:waiter:{index}",
                    root_id=root_id,
                    function="plan.waiter",
                    input=None,
                    topic=topic,
                )
            )
        await execution.suspend((shared_id,))

    for _ in range(waiters):
        async with store.tasks.claim(topic) as execution:
            assert execution is not None
            await execution.suspend((shared_id,))
    return topic, shared_id


async def collect(dsn: str) -> list[dict[str, Any]]:
    pool = await asyncpg.create_pool(dsn, min_size=16, max_size=32)
    store = reaper.Store(pool)
    prefix = f"plan-{uuid.uuid4().hex}"
    try:
        topic = await seed_ready(store, prefix, 2_000)
        future_topic = await seed_future(store, prefix, 2_000)
        waiter_topic, shared_id = await seed_fan_in(store, prefix, 101)
        before = datetime.now(UTC)
        async with pool.acquire() as connection:
            plans = [
                await explain(connection, "claim_ready", CLAIM, topic, [], []),
                await explain(connection, "claim_empty", CLAIM, f"{prefix}:empty", [], []),
                await explain(connection, "claim_future", CLAIM, future_topic, [], []),
                await explain(connection, "process_due_empty", PROCESS_DUE, 500),
                await explain(connection, "delete_expired_empty", DELETE_EXPIRED, before, 500),
                await explain(
                    connection,
                    "submit",
                    SUBMIT_CALLS,
                    json.dumps(
                        [
                            {
                                **(
                                    submit := reaper.SubmitCall(
                                        id=f"{prefix}:explain-submit",
                                        function="plan.submit",
                                        input=None,
                                        topic=topic,
                                        available_at=before,
                                        expires_at=before + timedelta(minutes=5),
                                        retention_ms=86_400_000,
                                    )
                                ).model_dump(mode="json"),
                                "idempotency_key": submit.fingerprint(),
                            }
                        ]
                    ),
                ),
            ]
            transaction = connection.transaction()
            await transaction.start()
            try:
                claimed = await connection.fetchrow(CLAIM, waiter_topic, [], [])
                assert claimed is None
                plans.append(
                    await explain(
                        connection,
                        "settle_101_waiters",
                        SETTLE,
                        shared_id,
                        reaper.PromiseState.RESOLVED.value,
                        "1",
                        None,
                    )
                )
            finally:
                await transaction.rollback()

            plan_waiter = f"{prefix}:plan-suspend"
            await store.tasks.submit(
                reaper.SubmitCall(
                    id=plan_waiter,
                    root_id=f"{prefix}:fan-in",
                    function="plan.waiter",
                    input=None,
                    topic=f"{prefix}:plan-suspend",
                )
            )
            transaction = connection.transaction()
            await transaction.start()
            try:
                row = await connection.fetchrow(CLAIM, f"{prefix}:plan-suspend", [], [])
                assert row is not None
                plans.append(
                    await explain(connection, "suspend", SUSPEND_TASK, plan_waiter, [shared_id])
                )
            finally:
                await transaction.rollback()
        return plans
    finally:
        async with pool.acquire() as connection:
            await connection.fetch(CLEANUP, prefix)
        await pool.close()


def main() -> None:
    dsn = os.environ.get(
        "REAPER_POSTGRES_DSN",
        "postgresql://reaper:reaper@127.0.0.1:55433/reaper_bench",
    )
    print(json.dumps(asyncio.run(collect(dsn)), indent=2))


if __name__ == "__main__":
    main()
