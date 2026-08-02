"""Wait on a durable timer processed by the maintenance pool."""

import asyncio

from reaper import ReaperClient, durable


@durable(execution_timeout=5.0)
async def delayed_greeting(name: str, delay_seconds: float) -> str:
    """Wait on a durable PostgreSQL timer before greeting someone."""

    await durable.sleep(delay_seconds)
    return f"Hello after {delay_seconds:g} seconds, {name}!"


async def main() -> None:
    """Submit a workflow that survives process and worker restarts."""

    async with ReaperClient.from_environment():
        result = await delayed_greeting("Grace", 2.0)
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
