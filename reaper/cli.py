"""Run Reaper worker daemons."""

import asyncio
import logging
import signal
import sys
from collections.abc import Sequence

from pydantic_settings import CliApp

from reaper.log import configure_logging, write
from reaper.pool import SkeletonPool
from reaper.settings import ReaperSettings
from reaper.systemd import SystemdNotifier, SystemdState

log = logging.getLogger(__name__)


class ReaperCLI(ReaperSettings):
    """Validate daemon flags and environment data."""

    def cli_cmd(self) -> None:
        """Start the typed daemon config."""

        configure_logging(self.log_level)
        asyncio.run(serve(self))


async def serve(settings: ReaperSettings) -> None:
    """Supervise declared skeleton pools."""

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    notifier = SystemdNotifier.from_environment()
    watchdog_task: asyncio.Task[None] | None = None
    loop.add_signal_handler(signal.SIGINT, stop.set)
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    write(
        log,
        logging.INFO,
        "reaper daemon started",
        pools=len(settings.pools),
        skeletons=sum(pool.skeletons for pool in settings.pools),
    )
    try:
        async with SkeletonPool.from_settings(settings) as reaper:
            notifier.notify(SystemdState.READY)
            watchdog_task = loop.create_task(notifier.watchdog())
            try:
                stop_task = asyncio.create_task(stop.wait())
                failure_task = asyncio.create_task(reaper.wait_failure())
                done, pending = await asyncio.wait(
                    {stop_task, failure_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                if failure_task in done:
                    raise failure_task.result()
            finally:
                notifier.notify(SystemdState.STOPPING)
    finally:
        if watchdog_task is not None:
            watchdog_task.cancel()
            await asyncio.gather(watchdog_task, return_exceptions=True)
        notifier.close()
    write(log, logging.INFO, "reaper daemon stopped")


def main(args: Sequence[str] = ()) -> int:
    """Parse flags with Pydantic and run."""

    values = list(args or sys.argv[1:])
    CliApp.run(ReaperCLI, cli_args=values)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
