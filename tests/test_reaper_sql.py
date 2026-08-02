"""Exercise the minimal reaper.schema and its typed query boundary."""

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from asyncpg import (
    ConnectionDoesNotExistError,
    DeadlockDetectedError,
    LockNotAvailableError,
    ProtocolViolationError,
    QueryCanceledError,
    connect,
    create_pool,
)
from pydantic import ValidationError

import reaper
from reaper.database import TransactionExecutor
from reaper.promises import submit_timer
from reaper.runtime import RuntimeOperation
from reaper.tasks import submit_call
from reaper.waits import suspend_task
from reaper.waits.models import SuspendTask
from tests.fault_runtime import FaultRuntime
from tests.faults import FaultPhase, FaultStep, OutcomeKind
from tests.reaper_sql_faults import FaultPool
from tests.scheduler import DeterministicScheduler


def test_query_parameters_are_strict_pydantic_models() -> None:
    call = reaper.SubmitCall(function="tests.echo", input={"value": 1})

    assert call.id
    assert call.id != reaper.SubmitCall(function="tests.echo", input=None).id
    with pytest.raises(ValidationError, match="greater than or equal to 60000"):
        reaper.SubmitCall(
            function="tests.echo",
            input=None,
            retention_ms=1,
        )
    with pytest.raises(ValidationError, match="awaited_ids must be unique"):
        SuspendTask(id="root", awaited_ids=("child", "child"))


@pytest.mark.postgres
def test_idempotent_replays_use_one_statement() -> None:
    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    class CountingExecutor:
        def __init__(self, executor: Any) -> None:
            self.executor = executor
            self.statements = 0

        async def execute(self, query: str, *args: object) -> Any:
            self.statements += 1
            return await self.executor.execute(query, *args)

        async def fetch(self, query: str, *args: object) -> Any:
            self.statements += 1
            return await self.executor.fetch(query, *args)

        async def fetchrow(self, query: str, *args: object) -> Any:
            self.statements += 1
            return await self.executor.fetchrow(query, *args)

        async def fetchval(self, query: str, *args: object) -> Any:
            self.statements += 1
            return await self.executor.fetchval(query, *args)

    async def check() -> None:
        pool = await create_pool(dsn, min_size=1, max_size=1)
        try:
            async with pool.acquire() as executor:
                counted = CountingExecutor(executor)
                now = datetime.now(UTC)
                call = reaper.SubmitCall(
                    function="tests.one_statement",
                    input=None,
                    expires_at=now + timedelta(minutes=1),
                )
                await submit_call(counted, call)
                counted.statements = 0
                await submit_call(counted, call)
                assert counted.statements == 1

                timer = reaper.SubmitTimer(due_at=now + timedelta(minutes=1))
                await submit_timer(counted, timer)
                counted.statements = 0
                await submit_timer(counted, timer)
                assert counted.statements == 1
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
def test_wait_registration_rejects_missing_and_cross_graph_rows() -> None:
    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=2, max_size=3)
        store = reaper.Reaper(pool)
        topic = f"reaper-invalid-waits-{uuid.uuid4().hex}"
        first_id = f"{topic}:first"
        second_id = f"{topic}:second"
        try:
            async with pool.acquire() as executor:
                active = executor.transaction()
                await active.start()
                try:
                    with pytest.raises(reaper.TaskNotFoundError):
                        await suspend_task(
                            cast(TransactionExecutor, executor),
                            SuspendTask(id="missing-task", awaited_ids=("missing-promise",)),
                        )
                finally:
                    await active.rollback()

            await store.tasks.submit_many(
                (
                    reaper.SubmitCall(
                        id=first_id,
                        function="tests.first_root",
                        input=None,
                        topic=topic,
                    ),
                    reaper.SubmitCall(
                        id=second_id,
                        function="tests.second_root",
                        input=None,
                        topic=topic,
                    ),
                )
            )
            async with store.tasks.claim(topic) as execution:
                assert execution is not None
                with pytest.raises(reaper.PromiseNotFoundError):
                    await execution.suspend(("missing-promise",))
            async with store.tasks.claim(topic) as execution:
                assert execution is not None
                awaited = second_id if execution.task.id == first_id else first_id
                with pytest.raises(reaper.CrossGraphWaitError):
                    await execution.suspend((awaited,))
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
def test_claim_excludes_function_versions_unsupported_by_worker() -> None:
    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=1, max_size=2)
        store = reaper.Reaper(pool)
        topic = f"reaper-versions-{uuid.uuid4().hex}"
        try:
            for version in (1, 2):
                await store.tasks.submit(
                    reaper.SubmitCall(
                        function="tests.versioned",
                        input=None,
                        topic=topic,
                        version=version,
                    )
                )
            excluded = (reaper.FunctionVersion(function="tests.versioned", version=1),)
            async with store.tasks.claim(topic, excluded) as execution:
                assert execution is not None
                assert execution.task.version == 2
                await execution.complete(None)
            async with store.tasks.claim(topic) as execution:
                assert execution is not None
                assert execution.task.version == 1
                await execution.complete(None)
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
def test_reaper_sql_lock_replay_timer_retry_and_retention() -> None:
    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=2, max_size=4)
        store = reaper.Reaper(pool)
        try:
            now = datetime.now(UTC)
            topic = f"reaper-sql-{uuid.uuid4().hex}"
            root_params = reaper.SubmitCall(
                function="tests.workflow",
                input={"value": 1},
                topic=topic,
                expires_at=now + timedelta(minutes=5),
                max_failures=2,
            )
            root = await store.tasks.submit(root_params)
            repeated = await store.tasks.submit(root_params)
            assert repeated == root

            timing_replays = (
                root_params.model_copy(
                    update={"expires_at": root_params.expires_at + timedelta(seconds=1)}
                ),
                root_params.model_copy(
                    update={"available_at": root_params.available_at + timedelta(seconds=1)}
                ),
            )
            for replay in timing_replays:
                assert await store.tasks.submit(replay) == root

            conflicting_calls = (
                root_params.model_copy(update={"input": {"value": 2}}),
                root_params.model_copy(update={"retention_ms": root_params.retention_ms + 1}),
            )
            for conflicting in conflicting_calls:
                with pytest.raises(reaper.IdempotencyConflictError):
                    await store.tasks.submit(conflicting)

            timer_collision = reaper.SubmitTimer(
                id=f"{root.id}:timer-collision",
                due_at=now + timedelta(minutes=1),
            )
            timer = await store.promises.timer(timer_collision)
            assert await store.promises.timer(timer_collision) == timer
            with pytest.raises(reaper.IdempotencyConflictError):
                await store.promises.timer(
                    timer_collision.model_copy(
                        update={"retention_ms": timer_collision.retention_ms + 1}
                    )
                )
            with pytest.raises(reaper.IdempotencyConflictError):
                await store.tasks.submit(
                    reaper.SubmitCall(
                        id=timer.id,
                        function="tests.timer_collision",
                        input=None,
                        topic=topic,
                    )
                )

            async with store.tasks.claim(topic) as first_execution:
                assert first_execution is not None
                assert first_execution.task.id == root.id
                async with store.tasks.claim(topic) as second_execution:
                    assert second_execution is None

            async with store.tasks.claim(topic) as execution:
                assert execution is not None
                retried = await execution.retry({"type": "Retryable"})
                assert not retried.rejected
                assert retried.failures == 1

            timer_due = datetime.now(UTC) + timedelta(milliseconds=30)
            async with store.tasks.claim(topic) as execution:
                assert execution is not None
                timer_params = reaper.SubmitTimer(
                    id=f"{execution.task.id}:sleep:0",
                    root_id=execution.task.id,
                    due_at=timer_due,
                )
                timer = await store.promises.timer(timer_params)
                await execution.suspend((timer.id,))

            await asyncio.sleep(0.05)
            processed = await store.maintenance.process_due()
            assert processed.timers == 1
            assert processed.timeouts == 0

            async with store.tasks.claim(topic) as execution:
                assert execution is not None
                assert [(item.id, item.state) for item in execution.task.waits] == [
                    (timer.id, reaper.PromiseState.RESOLVED)
                ]
                completed = await execution.complete({"ok": True})
                assert completed.state is reaper.PromiseState.RESOLVED

            loaded = await store.promises.get(root.id)
            assert loaded is not None and loaded.result == {"ok": True}
            replayed_terminal = await store.tasks.submit(root_params)
            assert replayed_terminal.state is reaper.PromiseState.RESOLVED
            assert replayed_terminal.result == {"ok": True}

            old_due = datetime.now(UTC) - timedelta(days=2)
            old_timer = await store.promises.timer(
                reaper.SubmitTimer(
                    due_at=old_due,
                    retention_ms=24 * 60 * 60 * 1_000,
                ),
            )
            assert (await store.maintenance.process_due()).timers == 1
            deleted = await store.maintenance.delete_expired()
            assert deleted.roots == 1
            assert await store.promises.get(old_timer.id) is None
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
def test_reaper_sql_concurrent_wait_registration_and_fan_in() -> None:
    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=4, max_size=32)
        store = reaper.Reaper(pool)
        prefix = f"reaper-races-{uuid.uuid4().hex}"
        root_topic = f"{prefix}:root"
        child_topic = f"{prefix}:child"
        waiter_topic = f"{prefix}:waiter"
        try:
            root = await store.tasks.submit(
                reaper.SubmitCall(
                    id=prefix,
                    function="tests.root",
                    input=None,
                    topic=root_topic,
                )
            )
            shared_ids = tuple(f"{prefix}:shared:{index}" for index in range(4))
            waiter_ids = tuple(f"{prefix}:waiter:{index}" for index in range(16))
            race_pairs = tuple(
                (f"{prefix}:race-child:{index}", f"{prefix}:race-waiter:{index}")
                for index in range(12)
            )

            async with store.tasks.claim(root_topic) as execution:
                assert execution is not None
                for promise_id in shared_ids:
                    await execution.submit(
                        reaper.SubmitCall(
                            id=promise_id,
                            root_id=root.id,
                            function="tests.child",
                            input=None,
                            topic=child_topic,
                        )
                    )
                for promise_id in waiter_ids:
                    await execution.submit(
                        reaper.SubmitCall(
                            id=promise_id,
                            root_id=root.id,
                            function="tests.waiter",
                            input=None,
                            topic=waiter_topic,
                        )
                    )
                for index, (child_id, waiter_id) in enumerate(race_pairs):
                    await execution.submit(
                        reaper.SubmitCall(
                            id=child_id,
                            root_id=root.id,
                            function="tests.child",
                            input=None,
                            topic=f"{child_topic}:race:{index}",
                        )
                    )
                    await execution.submit(
                        reaper.SubmitCall(
                            id=waiter_id,
                            root_id=root.id,
                            function="tests.waiter",
                            input=None,
                            topic=f"{waiter_topic}:race:{index}",
                        )
                    )
                await execution.complete(None)

            for _ in waiter_ids:
                async with store.tasks.claim(waiter_topic) as execution:
                    assert execution is not None
                    await execution.suspend(shared_ids)

            async def complete_child(topic: str) -> None:
                async with store.tasks.claim(topic) as execution:
                    assert execution is not None
                    await execution.complete(execution.task.id)

            async def suspend_waiter(topic: str, awaited_id: str) -> None:
                async with store.tasks.claim(topic) as execution:
                    assert execution is not None
                    await execution.suspend((awaited_id,))

            async with asyncio.timeout(10):
                await asyncio.gather(*(complete_child(child_topic) for _ in shared_ids))
                await asyncio.gather(
                    *(
                        coroutine
                        for index, (child_id, _waiter_id) in enumerate(race_pairs)
                        for coroutine in (
                            complete_child(f"{child_topic}:race:{index}"),
                            suspend_waiter(f"{waiter_topic}:race:{index}", child_id),
                        )
                    )
                )

            awakened: set[str] = set()
            for _ in waiter_ids:
                async with store.tasks.claim(waiter_topic) as execution:
                    assert execution is not None
                    awakened.add(execution.task.id)
                    assert execution.task.waits
                    assert all(
                        waited.state is reaper.PromiseState.RESOLVED
                        for waited in execution.task.waits
                    )
                    await execution.complete(None)
            assert awakened == set(waiter_ids)
            for index, (_child_id, waiter_id) in enumerate(race_pairs):
                async with store.tasks.claim(f"{waiter_topic}:race:{index}") as execution:
                    assert execution is not None
                    assert execution.task.id == waiter_id
                    assert len(execution.task.waits) == 1
                    assert execution.task.waits[0].state is reaper.PromiseState.RESOLVED
                    await execution.complete(None)

            timer_pairs = tuple(
                (f"{prefix}:race-timer:{index}", f"{prefix}:timer-waiter:{index}")
                for index in range(12)
            )
            due_at = datetime.now(UTC) - timedelta(seconds=1)
            for index, (timer_id, waiter_id) in enumerate(timer_pairs):
                await store.promises.timer(
                    reaper.SubmitTimer(
                        id=timer_id,
                        root_id=root.id,
                        due_at=due_at,
                    )
                )
                await store.tasks.submit(
                    reaper.SubmitCall(
                        id=waiter_id,
                        root_id=root.id,
                        function="tests.waiter",
                        input=None,
                        topic=f"{waiter_topic}:timer:{index}",
                    )
                )

            async with asyncio.timeout(10):
                await asyncio.gather(
                    *(
                        coroutine
                        for index, (timer_id, _waiter_id) in enumerate(timer_pairs)
                        for coroutine in (
                            store.maintenance.process_due(reaper.ProcessDue(limit=1)),
                            suspend_waiter(f"{waiter_topic}:timer:{index}", timer_id),
                        )
                    )
                )
                await store.maintenance.process_due(reaper.ProcessDue(limit=100))

            for index, (_timer_id, waiter_id) in enumerate(timer_pairs):
                async with store.tasks.claim(f"{waiter_topic}:timer:{index}") as execution:
                    assert execution is not None
                    assert execution.task.id == waiter_id
                    assert len(execution.task.waits) == 1
                    assert execution.task.waits[0].state is reaper.PromiseState.RESOLVED
                    await execution.complete(None)
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
def test_reaper_sql_concurrent_fan_in_settles_without_deadlock() -> None:
    """Sibling settlements must acquire their shared waiter locks consistently."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        width = 24
        pool = await create_pool(dsn, min_size=width, max_size=width + 2)
        store = reaper.Reaper(pool)
        prefix = f"reaper-lock-order-{uuid.uuid4().hex}"
        root_topic = f"{prefix}:root"
        producer_topic = f"{prefix}:producer"
        waiter_topic = f"{prefix}:waiter"
        producer_ids = tuple(f"{prefix}:producer:{index:02}" for index in range(width))
        waiter_ids = tuple(f"{prefix}:waiter:{index:02}" for index in range(width))

        try:
            root = await store.tasks.submit(
                reaper.SubmitCall(
                    id=prefix,
                    function="tests.root",
                    input=None,
                    topic=root_topic,
                )
            )
            async with store.tasks.claim(root_topic) as execution:
                assert execution is not None
                for promise_id in producer_ids:
                    await execution.submit(
                        reaper.SubmitCall(
                            id=promise_id,
                            root_id=root.id,
                            function="tests.producer",
                            input=None,
                            topic=producer_topic,
                        )
                    )
                for promise_id in reversed(waiter_ids):
                    await execution.submit(
                        reaper.SubmitCall(
                            id=promise_id,
                            root_id=root.id,
                            function="tests.waiter",
                            input=None,
                            topic=waiter_topic,
                        )
                    )
                await execution.complete(None)

            for _ in waiter_ids:
                async with store.tasks.claim(waiter_topic) as execution:
                    assert execution is not None
                    await execution.suspend(producer_ids)

            claimed = 0
            all_claimed = asyncio.Event()

            async def settle_producer() -> None:
                nonlocal claimed
                async with store.tasks.claim(producer_topic) as execution:
                    assert execution is not None
                    await execution.connection.execute("SET LOCAL deadlock_timeout = '100ms'")
                    claimed += 1
                    if claimed == width:
                        all_claimed.set()
                    await all_claimed.wait()
                    await execution.complete(execution.task.id)

            async with asyncio.timeout(10):
                await asyncio.gather(*(settle_producer() for _ in producer_ids))

            for _ in waiter_ids:
                async with store.tasks.claim(waiter_topic) as execution:
                    assert execution is not None
                    assert len(execution.task.waits) == width
                    assert all(
                        waited.state is reaper.PromiseState.RESOLVED
                        for waited in execution.task.waits
                    )
                    await execution.complete(None)
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
def test_reaper_sql_deadlock_victim_remains_claimable() -> None:
    """A PostgreSQL-selected deadlock victim must roll back to durable work."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=4, max_size=6)
        store = reaper.Reaper(pool)
        prefix = f"reaper-deadlock-{uuid.uuid4().hex}"
        root_topic = f"{prefix}:root"
        producer_topic = f"{prefix}:producer"
        waiter_topic = f"{prefix}:waiter"
        producer_ids = (f"{prefix}:producer:0", f"{prefix}:producer:1")
        waiter_ids = (f"{prefix}:waiter:0", f"{prefix}:waiter:1")

        try:
            root = await store.tasks.submit(
                reaper.SubmitCall(
                    id=prefix,
                    function="tests.root",
                    input=None,
                    topic=root_topic,
                )
            )
            async with store.tasks.claim(root_topic) as execution:
                assert execution is not None
                for promise_id in (*producer_ids, *waiter_ids):
                    await execution.submit(
                        reaper.SubmitCall(
                            id=promise_id,
                            root_id=root.id,
                            function="tests.node",
                            input=None,
                            topic=(producer_topic if promise_id in producer_ids else waiter_topic),
                        )
                    )
                await execution.complete(None)

            for _ in waiter_ids:
                async with store.tasks.claim(waiter_topic) as execution:
                    assert execution is not None
                    await execution.suspend(producer_ids)

            entered = 0
            both_locked = asyncio.Event()

            async def collide(waiter_id: str) -> None:
                nonlocal entered
                async with store.tasks.claim(producer_topic) as execution:
                    assert execution is not None
                    await execution.connection.execute("SET LOCAL deadlock_timeout = '100ms'")
                    await execution.connection.fetchval(
                        "SELECT promise_id FROM reaper.tasks WHERE promise_id = $1 FOR UPDATE",
                        waiter_id,
                    )
                    entered += 1
                    if entered == len(producer_ids):
                        both_locked.set()
                    await both_locked.wait()
                    await execution.complete(execution.task.id)

            outcomes = await asyncio.gather(
                *(collide(waiter_id) for waiter_id in waiter_ids),
                return_exceptions=True,
            )
            victims = [item for item in outcomes if isinstance(item, BaseException)]
            assert len(victims) == 1
            assert isinstance(victims[0], DeadlockDetectedError)

            async with store.tasks.claim(producer_topic) as execution:
                assert execution is not None
                assert execution.task.id in producer_ids
                await execution.complete(execution.task.id)

            awakened: set[str] = set()
            for _ in waiter_ids:
                async with store.tasks.claim(waiter_topic) as execution:
                    assert execution is not None
                    awakened.add(execution.task.id)
                    assert all(
                        waited.state is reaper.PromiseState.RESOLVED
                        for waited in execution.task.waits
                    )
                    await execution.complete(None)
            assert awakened == set(waiter_ids)
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
@pytest.mark.stress
@pytest.mark.parametrize("fault", ("cancellation", "connection-loss", "idle-timeout"))
def test_reaper_sql_claims_survive_cancellation_connection_loss_and_idle_timeout(
    fault: str,
) -> None:
    """Every non-commit exit must release the row for another execution."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=2, max_size=6)
        store = reaper.Reaper(pool)
        promise_id = f"reaper-claim-faults-{fault}-{uuid.uuid4().hex}"
        await store.tasks.submit(
            reaper.SubmitCall(
                id=promise_id,
                function="tests.fault",
                input=None,
                topic=promise_id,
                execution_timeout_ms=50 if fault == "idle-timeout" else 30_000,
            )
        )

        async def finish_reclaimed(promise_id: str) -> None:
            async with asyncio.timeout(5):
                while True:
                    async with store.tasks.claim(promise_id) as execution:
                        if execution is None:
                            await asyncio.sleep(0.01)
                            continue
                        assert execution.task.id == promise_id
                        await execution.complete("reclaimed")
                        return

        def assert_pool_balanced() -> None:
            assert pool.get_idle_size() == pool.get_size(), (
                f"connection remained checked out after {fault}: "
                f"idle={pool.get_idle_size()} size={pool.get_size()}"
            )

        try:
            if fault == "cancellation":
                entered = asyncio.Event()

                async def cancelled_owner() -> None:
                    async with store.tasks.claim(promise_id) as execution:
                        assert execution is not None
                        entered.set()
                        await asyncio.Event().wait()

                owner = asyncio.create_task(cancelled_owner())
                async with asyncio.timeout(5):
                    await entered.wait()
                owner.cancel()
            elif fault == "connection-loss":
                claimed = asyncio.Event()
                terminated = asyncio.Event()
                backend_pid = 0

                async def disconnected_owner() -> None:
                    nonlocal backend_pid
                    async with store.tasks.claim(promise_id) as execution:
                        assert execution is not None
                        backend_pid = int(
                            await execution.connection.fetchval("SELECT pg_backend_pid()")
                        )
                        claimed.set()
                        await terminated.wait()
                        await execution.complete("must-not-commit")

                owner = asyncio.create_task(disconnected_owner())
                async with asyncio.timeout(5):
                    await claimed.wait()
                admin = await connect(dsn)
                try:
                    assert await admin.fetchval("SELECT pg_terminate_backend($1)", backend_pid)
                finally:
                    await admin.close()
                terminated.set()
            else:

                async def timed_out_owner() -> None:
                    async with store.tasks.claim(promise_id) as execution:
                        assert execution is not None
                        await asyncio.sleep(0.15)
                        await execution.complete("must-not-commit")

                owner = asyncio.create_task(timed_out_owner())

            owner_result = (await asyncio.gather(owner, return_exceptions=True))[0]
            assert isinstance(owner_result, BaseException)
            assert_pool_balanced()
            await finish_reclaimed(promise_id)
            assert_pool_balanced()
        finally:
            try:
                async with asyncio.timeout(5):
                    await pool.close()
            except TimeoutError as error:
                pool.terminate()
                raise AssertionError(f"connection pool did not close after {fault}") from error

    asyncio.run(check())


@pytest.mark.postgres
def test_task_execution_timeout_also_bounds_active_row_lock_waits() -> None:
    """An active blocked statement must not evade idle-in-transaction timeout."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=2, max_size=3)
        store = reaper.Reaper(pool)
        promise_id = f"reaper-lock-timeout-{uuid.uuid4().hex}"
        await store.tasks.submit(
            reaper.SubmitCall(
                id=promise_id,
                function="tests.lock_timeout",
                input=None,
                topic=promise_id,
                execution_timeout_ms=50,
            )
        )
        blocker = await pool.acquire()
        blocked = blocker.transaction()
        await blocked.start()
        try:
            async with store.tasks.claim(promise_id) as execution:
                assert execution is not None
                await blocker.fetchval(
                    "SELECT id FROM reaper.promises WHERE id = $1 FOR UPDATE",
                    promise_id,
                )
                async with asyncio.timeout(1):
                    with pytest.raises(LockNotAvailableError):
                        await execution.complete("blocked")
        finally:
            await blocked.rollback()
            await pool.release(blocker)
        try:
            async with store.tasks.claim(promise_id) as execution:
                assert execution is not None
                await execution.complete("recovered")
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
def test_task_execution_timeout_also_bounds_active_queries() -> None:
    """A running statement must not bypass the task execution timeout."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=1, max_size=2)
        store = reaper.Reaper(pool)
        promise_id = f"reaper-statement-timeout-{uuid.uuid4().hex}"
        await store.tasks.submit(
            reaper.SubmitCall(
                id=promise_id,
                function="tests.statement_timeout",
                input=None,
                topic=promise_id,
                execution_timeout_ms=50,
            )
        )
        try:
            async with store.tasks.claim(promise_id) as execution:
                assert execution is not None
                async with asyncio.timeout(1):
                    with pytest.raises(QueryCanceledError):
                        await execution.connection.fetchval("SELECT pg_sleep(1)")
            async with store.tasks.claim(promise_id) as execution:
                assert execution is not None
                await execution.complete("recovered")
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
def test_reaper_sql_query_and_commit_ambiguity_remain_idempotent() -> None:
    """Before/after database failures must resolve to replay or committed state."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=2, max_size=6)
        plain = reaper.Reaper(pool)
        prefix = f"reaper-db-faults-{uuid.uuid4().hex}"

        async def run_fault(suffix: str, step: FaultStep) -> str:
            promise_id = f"{prefix}:{suffix}"
            runtime = FaultRuntime([step], DeterministicScheduler())
            faulted = reaper.Reaper(FaultPool(pool, runtime))
            await faulted.tasks.submit(
                reaper.SubmitCall(
                    id=promise_id,
                    function="tests.fault",
                    input=None,
                    topic=promise_id,
                )
            )
            outcome = await asyncio.gather(
                complete_once(faulted, promise_id),
                return_exceptions=True,
            )
            assert isinstance(outcome[0], ConnectionDoesNotExistError)
            assert not runtime.steps
            return promise_id

        async def complete_once(store: reaper.Reaper, promise_id: str) -> None:
            async with store.tasks.claim(promise_id) as execution:
                assert execution is not None
                await execution.complete("committed")

        try:
            after_query = await run_fault(
                "after-query",
                FaultStep(
                    call=RuntimeOperation.DB_QUERY,
                    outcome=OutcomeKind.CONNECTION_LOST,
                    phase=FaultPhase.AFTER,
                    site="settle",
                ),
            )
            await complete_once(plain, after_query)

            before_commit = await run_fault(
                "before-commit",
                FaultStep(
                    call=RuntimeOperation.DB_COMMIT,
                    outcome=OutcomeKind.CONNECTION_LOST,
                    site="transaction",
                ),
            )
            await complete_once(plain, before_commit)

            after_commit = await run_fault(
                "after-commit",
                FaultStep(
                    call=RuntimeOperation.DB_COMMIT,
                    outcome=OutcomeKind.CONNECTION_LOST,
                    phase=FaultPhase.AFTER,
                    site="transaction",
                ),
            )
            committed = await plain.promises.get(after_commit)
            assert committed is not None
            assert committed.state is reaper.PromiseState.RESOLVED
            assert committed.result == "committed"
            async with plain.tasks.claim(after_commit) as execution:
                assert execution is None
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
@pytest.mark.parametrize("phase", (FaultPhase.BEFORE, FaultPhase.AFTER))
def test_reaper_sql_commit_cancellation_resolves_by_durable_state(
    phase: FaultPhase,
) -> None:
    """Cancellation on either side of COMMIT must remain safely replayable."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=2, max_size=4)
        plain = reaper.Reaper(pool)
        promise_id = f"reaper-commit-cancel-{phase}-{uuid.uuid4().hex}"
        await plain.tasks.submit(
            reaper.SubmitCall(
                id=promise_id,
                function="tests.cancel",
                input=None,
                topic=promise_id,
            )
        )
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.DB_COMMIT,
                    outcome=OutcomeKind.CANCELLED,
                    phase=phase,
                    site="transaction",
                )
            ],
            DeterministicScheduler(),
        )
        faulted = reaper.Reaper(FaultPool(pool, runtime))
        try:

            async def complete_faulted() -> None:
                async with faulted.tasks.claim(promise_id) as execution:
                    assert execution is not None
                    await execution.complete("committed")

            result = (
                await asyncio.gather(
                    complete_faulted(),
                    return_exceptions=True,
                )
            )[0]
            assert isinstance(result, asyncio.CancelledError)
            assert not runtime.steps

            async with plain.tasks.claim(promise_id) as execution:
                if phase is FaultPhase.BEFORE:
                    assert execution is not None
                    await execution.complete("committed")
                else:
                    assert execution is None
            promise = await plain.promises.get(promise_id)
            assert promise is not None
            assert promise.state is reaper.PromiseState.RESOLVED
            assert promise.result == "committed"
        finally:
            await pool.close()

    asyncio.run(check())


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("outcome", "error_type"),
    (
        (OutcomeKind.CANCELLED, asyncio.CancelledError),
        (OutcomeKind.CONNECTION_LOST, ConnectionDoesNotExistError),
        (OutcomeKind.TIMEOUT, TimeoutError),
        (OutcomeKind.PROTOCOL_ERROR, ProtocolViolationError),
    ),
)
def test_reaper_sql_release_fault_does_not_leak_a_pool_holder(
    outcome: OutcomeKind,
    error_type: type[BaseException],
) -> None:
    """The DST release adapter must preserve asyncpg's holder-cleanup contract."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=1, max_size=1)
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.DB_RELEASE,
                    outcome=outcome,
                    site="pool",
                )
            ],
            DeterministicScheduler(),
        )
        faulted = reaper.Reaper(FaultPool(pool, runtime))
        promise_id = f"reaper-release-cancel-{uuid.uuid4().hex}"
        result = (
            await asyncio.gather(
                faulted.tasks.submit(
                    reaper.SubmitCall(
                        id=promise_id,
                        function="tests.cancel",
                        input=None,
                        topic=promise_id,
                    )
                ),
                return_exceptions=True,
            )
        )[0]
        assert isinstance(result, error_type)
        assert not runtime.steps
        try:
            async with asyncio.timeout(1):
                await pool.close()
        except TimeoutError:
            pool.terminate()
            raise

    asyncio.run(check())


@pytest.mark.postgres
def test_reaper_sql_post_acquire_cancellation_does_not_leak_a_pool_holder() -> None:
    """A connection acquired just before cancellation must be returned."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=1, max_size=1)
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.DB_ACQUIRE,
                    outcome=OutcomeKind.CANCELLED,
                    phase=FaultPhase.AFTER,
                    site="pool",
                )
            ],
            DeterministicScheduler(),
        )
        faulted = reaper.Reaper(FaultPool(pool, runtime))
        result = (
            await asyncio.gather(
                faulted.tasks.submit(
                    reaper.SubmitCall(
                        function="tests.cancel",
                        input=None,
                    )
                ),
                return_exceptions=True,
            )
        )[0]
        assert isinstance(result, asyncio.CancelledError)
        assert not runtime.steps
        try:
            async with asyncio.timeout(1):
                await pool.close()
        except TimeoutError:
            pool.terminate()
            raise

    asyncio.run(check())


@pytest.mark.postgres
def test_reaper_sql_claim_failure_survives_rollback_failure() -> None:
    """Cleanup failure must not replace the fault that aborted task acquisition."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")

    async def check() -> None:
        pool = await create_pool(dsn, min_size=1, max_size=2)
        plain = reaper.Reaper(pool)
        promise_id = f"reaper-double-fault-{uuid.uuid4().hex}"
        runtime = FaultRuntime(
            [
                FaultStep(
                    call=RuntimeOperation.DB_QUERY,
                    outcome=OutcomeKind.DEADLOCK,
                    site="claim",
                ),
                FaultStep(
                    call=RuntimeOperation.DB_ROLLBACK,
                    outcome=OutcomeKind.PROTOCOL_ERROR,
                    site="transaction",
                ),
            ],
            DeterministicScheduler(),
        )
        faulted = reaper.Reaper(FaultPool(pool, runtime))

        try:
            await plain.tasks.submit(
                reaper.SubmitCall(
                    id=promise_id,
                    function="tests.double_fault",
                    input=None,
                    topic=promise_id,
                )
            )

            async def claim_once() -> None:
                async with faulted.tasks.claim(promise_id):
                    raise AssertionError("faulted claim unexpectedly entered its body")

            result = (await asyncio.gather(claim_once(), return_exceptions=True))[0]
            assert isinstance(result, DeadlockDetectedError)
            assert result.sqlstate == "40P01"
            assert not runtime.steps

            async with plain.tasks.claim(promise_id) as execution:
                assert execution is not None
                await execution.complete("reclaimed")
        finally:
            await pool.close()

    asyncio.run(check())
