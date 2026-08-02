"""Check the Reaper benchmark contract without requiring PostgreSQL."""

import asyncio
import logging
from typing import Any, cast

import pytest
from asyncpg import DeadlockDetectedError

from bench.reaper_sql import BenchmarkConfig, graph_workloads, run_workers


def test_benchmark_uses_the_expected_graph_count() -> None:
    workloads = graph_workloads(BenchmarkConfig(roots=1, workers=1, connections=2))

    assert sum(workload.tasks for workload in workloads) == 136


def test_reaper_benchmark_retries_transaction_deadlocks(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A rolled-back claim must be polled again instead of killing every worker."""

    class Task:
        root_id = None
        function = "root"

    class Execution:
        task = Task()

    class Claim:
        def __init__(self, attempt: int) -> None:
            self.attempt = attempt

        async def __aenter__(self) -> Execution:
            if self.attempt == 1:
                raise DeadlockDetectedError("forced lock cycle")
            return Execution()

        async def __aexit__(self, *_args: object) -> None:
            return None

    class Tasks:
        attempts = 0

        def claim(self, _topic: str) -> Claim:
            self.attempts += 1
            return Claim(self.attempts)

    class Store:
        tasks = Tasks()

    async def execute(_execution: Execution) -> bool:
        return True

    monkeypatch.setattr("bench.reaper_sql.execute_task", execute)

    with caplog.at_level(logging.WARNING, logger="bench.reaper_sql"):
        latencies = asyncio.run(
            run_workers(Store(), "topic", workers=1, expected=1)  # type: ignore[arg-type]
        )

    assert Store.tasks.attempts == 2
    assert set(latencies) == {"root"}
    assert "task transaction deadlocked; retrying" in caplog.text
    record = cast(Any, caplog.records[0])
    assert record.reaper_tags == {
        "topic": "topic",
        "sqlstate": "40P01",
    }
