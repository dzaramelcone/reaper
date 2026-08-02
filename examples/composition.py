"""Compose one durable function from another."""

import asyncio

from reaper import ReaperClient, durable


@durable(execution_timeout=5.0)
async def capitalize(name: str) -> str:
    """Normalize a name in a reusable durable step."""

    return name.title()


@durable(execution_timeout=5.0)
async def welcome(name: str) -> str:
    """Await a child durable function inside a root workflow."""

    normalized = await capitalize(name)
    return f"Welcome, {normalized}!"


async def main() -> None:
    """Execute the composed workflow."""

    async with ReaperClient.from_environment():
        print(await welcome("ada lovelace"))


if __name__ == "__main__":
    asyncio.run(main())
