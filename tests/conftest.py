"""Give each test process its own PostgreSQL database."""

import asyncio
import os
from collections.abc import Generator
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest
from asyncpg import connect


async def create_postgres_shard(admin_dsn: str, shard_dsn: str, name: str) -> None:
    """Create and load one test-owned database."""

    admin = await connect(admin_dsn)
    await admin.execute(f'CREATE DATABASE "{name}"')
    await admin.close()
    database = await connect(shard_dsn)
    schema = Path(__file__).parents[1].joinpath("reaper", "schema.sql").read_text()
    await database.execute(schema)
    await database.close()


async def drop_postgres_shard(admin_dsn: str, shard_dsn: str, name: str) -> None:
    """Collect rows before dropping one test database."""

    admin = await connect(admin_dsn)
    await admin.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
        name,
    )
    await admin.execute(f'DROP DATABASE "{name}"')
    await admin.close()


@pytest.fixture(scope="session", autouse=True)
def postgres_shard() -> Generator[None]:
    """Isolate each pytest process from stale durable work."""

    base_dsn = os.environ.get("REAPER_POSTGRES_DSN", "")
    if not base_dsn:
        yield
        return
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main").replace("-", "_")
    name = f"reaper_test_{os.getpid()}_{worker}"
    assert name.replace("_", "").isalnum()
    parts = urlsplit(base_dsn)
    admin_dsn = urlunsplit((parts.scheme, parts.netloc, "/postgres", parts.query, ""))
    shard_dsn = urlunsplit((parts.scheme, parts.netloc, f"/{name}", parts.query, ""))
    asyncio.run(create_postgres_shard(admin_dsn, shard_dsn, name))
    os.environ["REAPER_POSTGRES_DSN"] = shard_dsn
    yield
    asyncio.run(drop_postgres_shard(admin_dsn, shard_dsn, name))
    os.environ["REAPER_POSTGRES_DSN"] = base_dsn
