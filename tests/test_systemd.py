"""Check systemd readiness and watchdog notification behavior."""

import asyncio
import os
import socket
import tempfile
from pathlib import Path

from reaper.systemd import SystemdNotifier, SystemdState


def test_notifier_sends_readiness_and_watchdog_from_event_loop() -> None:
    async def scenario() -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            address = str(Path(directory) / "notify.sock")
            receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            receiver.bind(address)
            receiver.setblocking(False)
            notifier = SystemdNotifier.from_environment(
                {
                    "NOTIFY_SOCKET": address,
                    "WATCHDOG_USEC": "20000",
                    "WATCHDOG_PID": str(os.getpid()),
                }
            )
            try:
                assert notifier.watchdog_interval == 0.01
                assert notifier.notify(SystemdState.READY)
                loop = asyncio.get_running_loop()
                assert await loop.sock_recv(receiver, 64) == SystemdState.READY.value.encode()

                task = loop.create_task(notifier.watchdog())
                assert (
                    await asyncio.wait_for(loop.sock_recv(receiver, 64), 0.1)
                    == SystemdState.WATCHDOG.value.encode()
                )
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            finally:
                notifier.close()
                receiver.close()

    asyncio.run(scenario())


def test_watchdog_is_disabled_for_another_process() -> None:
    notifier = SystemdNotifier.from_environment(
        {
            "NOTIFY_SOCKET": "/tmp/not-used.sock",
            "WATCHDOG_USEC": "1000000",
            "WATCHDOG_PID": str(os.getpid() + 1),
        }
    )

    assert notifier.address == "/tmp/not-used.sock"
    assert notifier.watchdog_interval is None
    notifier.close()


def test_notifier_is_a_noop_outside_systemd() -> None:
    notifier = SystemdNotifier.from_environment({}, process_id=123)

    assert notifier.address is None
    assert notifier.watchdog_interval is None
    assert not notifier.notify(SystemdState.READY)
