"""Submit a durable root without waiting for its result."""

import asyncio

from reaper import Reaper, durable


@durable(execution_timeout=5.0)
async def create_report(account_id: str) -> str:
    """Create one durable report."""

    return f"report for {account_id}"


async def main() -> None:
    """Model the core of an HTTP endpoint returning 202 Accepted."""

    async with Reaper():
        promise = await create_report("account-42").submit(id="request-456")
    print(f"accepted promise {promise.id()}")


if __name__ == "__main__":
    asyncio.run(main())
