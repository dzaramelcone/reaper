"""Run one killable SkeletonPool tree for process tests."""

import asyncio
import os
import sys
from pathlib import Path

from reaper.pool import SkeletonPool
from tests.workers import living_tree


async def run(path: Path) -> None:
    """Keep one SkeletonPool and its descendants alive."""

    path.joinpath(f"reaper-{os.getpid()}.pid").touch()
    async with SkeletonPool(1, beat_rate=0.02) as reaper:
        await reaper.run_async(living_tree, path, 2, 2)


def main() -> None:
    asyncio.run(run(Path(sys.argv[1])))


if __name__ == "__main__":
    main()
