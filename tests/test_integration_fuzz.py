"""Replay generated failures on real processes and PostgreSQL."""

import asyncio
import os
import signal
from enum import StrEnum

import pytest
from asyncpg import connect
from hypothesis import given, settings, strategies
from pydantic import BaseModel, ConfigDict

from reaper.pool import SkeletonPool
from reaper.promise import ReaperClient
from reaper.settings import PoolConfig, PoolKind, ReaperSettings
from tests.workers import durable_timer_workflow, promise_hold, promise_value


class IntegrationAction(StrEnum):
    VALUE = "value"
    TIMER = "timer"
    KILL_TASK = "kill_task"
    KILL_TIMER = "kill_timer"
    KILL_DATABASE_LINKS = "kill_database_links"


class IntegrationScenario(BaseModel):
    """Hold one generated real-system replay."""

    model_config = ConfigDict(frozen=True, strict=True)

    actions: tuple[IntegrationAction, ...]


async def wait_for_gen(pool: SkeletonPool, old_gen: int) -> None:
    """Wait until one dead slot is replaced."""

    async with asyncio.timeout(5.0):
        while not pool.slots or max(slot.identity.gen for slot in pool.slots.values()) <= old_gen:
            await asyncio.sleep(0.005)


@pytest.mark.fuzz
@pytest.mark.postgres
@pytest.mark.stress
@settings(max_examples=5, deadline=None, print_blob=True)
@given(actions=strategies.permutations(tuple(IntegrationAction)))
def test_generated_scenarios_use_real_processes_and_postgres(
    actions: list[IntegrationAction],
) -> None:
    """Replay generated failure order on live backends."""

    dsn = os.environ.get("REAPER_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set REAPER_POSTGRES_DSN to run Postgres checks")
    scenario = IntegrationScenario(actions=tuple(actions))
    assert set(scenario.actions) == set(IntegrationAction)

    async def check() -> None:
        application_name = f"reaper_fuzz_{os.getpid()}"
        separator = "&" if "?" in dsn else "?"
        tagged_dsn = f"{dsn}{separator}application_name={application_name}"
        settings_model = ReaperSettings(
            postgres_dsn=tagged_dsn,
            poll_rate=0.005,
            maintenance_rate=0.01,
            service_retry_base=0.01,
            service_retry_max=0.05,
            pools=[
                PoolConfig(skeletons=1, topic="workflow"),
                PoolConfig(kind=PoolKind.MAINTENANCE, skeletons=1),
            ],
        )
        async with SkeletonPool.from_settings(settings_model) as reaper:
            assert len(reaper.child_pools) == 1
            timer_pool = reaper.child_pools[0]
            async with ReaperClient.from_settings(settings_model):
                for index, action in enumerate(scenario.actions):
                    match action:
                        case IntegrationAction.VALUE:
                            assert (
                                await asyncio.wait_for(promise_value(index), timeout=5.0) == index
                            )
                        case IntegrationAction.TIMER:
                            assert (
                                await asyncio.wait_for(durable_timer_workflow(), timeout=5.0)
                                == "awake"
                            )
                        case IntegrationAction.KILL_TASK:
                            running = asyncio.ensure_future(promise_hold(index, 0.1))
                            await asyncio.sleep(0.03)
                            victim = next(iter(reaper.slots.values()))
                            old_gen = victim.identity.gen
                            os.kill(victim.pid, signal.SIGKILL)
                            assert await asyncio.wait_for(running, timeout=5.0) == index
                            await wait_for_gen(reaper, old_gen)
                        case IntegrationAction.KILL_TIMER:
                            victim = next(iter(timer_pool.slots.values()))
                            old_gen = victim.identity.gen
                            os.kill(victim.pid, signal.SIGKILL)
                            await wait_for_gen(timer_pool, old_gen)
                            assert (
                                await asyncio.wait_for(durable_timer_workflow(), timeout=5.0)
                                == "awake"
                            )
                        case IntegrationAction.KILL_DATABASE_LINKS:
                            controller = await connect(dsn)
                            try:
                                rows = await controller.fetch(
                                    """
                                    SELECT pid, pg_terminate_backend(pid) AS terminated
                                    FROM pg_stat_activity
                                    WHERE application_name = $1
                                      AND pid <> pg_backend_pid()
                                    """,
                                    application_name,
                                )
                                assert rows
                                assert all(row["terminated"] for row in rows)
                            finally:
                                await controller.close()
                            await asyncio.sleep(0.1)
                            assert (
                                await asyncio.wait_for(promise_value(index), timeout=5.0) == index
                            )

    asyncio.run(check())
