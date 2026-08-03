"""Compose durable functions with fan-out and gather."""

import asyncio

from reaper import Reaper, durable


@durable(execution_timeout=5.0)
async def square(value: int) -> int:
    """Square one value."""

    return value * value


@durable(execution_timeout=5.0)
async def sum_squares(values: list[int]) -> int:
    """Run each square independently, then combine the results."""

    squares = await durable.fanout(square, values)
    return sum(promise.result() for promise in squares)


async def main() -> None:
    """Execute the composed workflow."""

    async with Reaper():
        print(await sum_squares([1, 2, 3, 4]))


if __name__ == "__main__":
    asyncio.run(main())
