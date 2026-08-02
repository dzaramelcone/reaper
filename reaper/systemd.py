"""Report daemon readiness and event-loop liveness to systemd."""

import asyncio
import os
import socket
from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class SystemdState(StrEnum):
    """Messages used by systemd's service notification protocol."""

    READY = "READY=1"
    STOPPING = "STOPPING=1"
    WATCHDOG = "WATCHDOG=1"


class SystemdNotifier(BaseModel):
    """Send optional service-manager notifications without a runtime dependency."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    address: str | bytes | None
    watchdog_interval: float | None
    process_id: int
    transport: Annotated[socket.socket | None, Field(exclude=True)] = None

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
        *,
        process_id: int | None = None,
    ) -> SystemdNotifier:
        """Build notification settings from systemd's environment contract."""

        values = os.environ if environment is None else environment
        pid = os.getpid() if process_id is None else process_id
        raw_address = values.get("NOTIFY_SOCKET", "")
        address: str | bytes | None = raw_address or None
        if raw_address.startswith("@"):
            address = b"\0" + os.fsencode(raw_address[1:])

        interval: float | None = None
        watchdog_process = values.get("WATCHDOG_PID", "")
        if not watchdog_process or watchdog_process == str(pid):
            try:
                watchdog_usec = int(values.get("WATCHDOG_USEC", "0"))
            except ValueError:
                watchdog_usec = 0
            if watchdog_usec > 0:
                interval = watchdog_usec / 2_000_000

        return cls(address=address, watchdog_interval=interval, process_id=pid)

    def notify(self, state: SystemdState) -> bool:
        """Send one datagram, returning false when notification is unavailable."""

        if self.address is None:
            return False
        if self.transport is None:
            self.transport = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            self.transport.set_inheritable(False)
            self.transport.setblocking(False)
        try:
            self.transport.sendto(state.encode(), self.address)
        except OSError:
            return False
        return True

    async def watchdog(self) -> None:
        """Tie systemd watchdog notifications directly to event-loop progress."""

        if self.watchdog_interval is None:
            return
        while True:
            await asyncio.sleep(self.watchdog_interval)
            self.notify(SystemdState.WATCHDOG)

    def close(self) -> None:
        """Release the notification socket."""

        if self.transport is not None:
            self.transport.close()
            self.transport = None
