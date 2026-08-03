"""Run the smallest useful durable function."""

import asyncio

from reaper import Reaper, durable


@durable(execution_timeout=5.0)
async def hello(name: str) -> str:
    return f"Hello, {name}!"


async def main() -> None:
    async with Reaper():
        print(await hello("world"))


if __name__ == "__main__":
    asyncio.run(main())
