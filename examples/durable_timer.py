"""Wait on a durable timer processed by the maintenance pool."""

import asyncio
from datetime import timedelta

from reaper import Reaper, durable


@durable(execution_timeout=5.0)
async def delayed_greeting(name: str, delay: timedelta) -> str:
    """Wait on a durable PostgreSQL timer before greeting someone."""

    await durable.sleep(delay)
    return f"Hello after {delay}, {name}!"


async def main() -> None:
    """Complete one timer and leave a much longer timer durable."""

    async with Reaper():
        greeting = await delayed_greeting("Grace", timedelta(seconds=3)).result(
            id="three-second-greeting"
        )
        long_wait = await delayed_greeting("Ada", timedelta(days=30)).submit(
            id="thirty-day-greeting"
        )
    print(greeting)
    print(f"30-day greeting submitted as {long_wait.id()}")


if __name__ == "__main__":
    asyncio.run(main())
