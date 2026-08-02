"""Submit a root under a caller-owned request ID."""

import asyncio

from reaper import ReaperClient, durable


@durable(execution_timeout=5.0)
async def greet(name: str) -> str:
    """Return one durable greeting."""

    return f"Hello, {name}!"


async def main() -> None:
    """Replay the same root without executing it twice."""

    async with ReaperClient.from_environment():
        result = await greet("Ada").result(id="request-123")
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
