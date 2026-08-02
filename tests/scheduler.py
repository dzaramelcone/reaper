"""Deterministically pause and release concurrent test actors."""

import asyncio
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field

from reaper.runtime import RuntimeCheckpoint


class SchedulePoint(BaseModel):
    """Describe one observable concurrency boundary."""

    model_config = ConfigDict(frozen=True, strict=True)

    actor: str
    operation: str
    phase: str
    details: Mapping[str, str] = Field(default_factory=dict)


class ScheduleTrace(BaseModel):
    """Store an exact release order for later replay."""

    model_config = ConfigDict(frozen=True, strict=True)

    version: int = 1
    decisions: tuple[SchedulePoint, ...]

    def save(self, path: Path) -> None:
        """Persist this trace as readable JSON."""

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=2) + "\n", encoding="utf8")

    @classmethod
    def load(cls, path: Path) -> Self:
        """Load one previously saved release trace."""

        return cls.model_validate_json(path.read_text(encoding="utf8"), strict=True)


class WaitingPoint(BaseModel):
    """Pair one checkpoint with the event that releases its actor."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    point: SchedulePoint
    released: asyncio.Event = Field(default_factory=asyncio.Event)


class DeterministicScheduler:
    """Expose coroutine interleavings as explicit test decisions."""

    def __init__(self) -> None:
        self.waiting: list[WaitingPoint] = []
        self.decisions: list[SchedulePoint] = []
        self.changed = asyncio.Condition()

    async def pause(
        self,
        actor: str,
        operation: str,
        phase: str,
        details: Mapping[str, str] | None = None,
    ) -> None:
        """Block an actor at a named boundary until the test releases it."""

        waiter = WaitingPoint(
            point=SchedulePoint(
                actor=actor,
                operation=operation,
                phase=phase,
                details=dict(details or {}),
            )
        )
        async with self.changed:
            self.waiting.append(waiter)
            self.changed.notify_all()
        await waiter.released.wait()

    async def wait_for(
        self,
        *,
        count: int = 1,
        operation: str = "",
        phase: str = "",
    ) -> tuple[SchedulePoint, ...]:
        """Wait until enough matching actors are paused."""

        if count <= 0:
            raise ValueError("checkpoint count must be positive")

        def matches() -> list[WaitingPoint]:
            return [
                waiter
                for waiter in self.waiting
                if (not operation or waiter.point.operation == operation)
                and (not phase or waiter.point.phase == phase)
            ]

        async with self.changed:
            await self.changed.wait_for(lambda: len(matches()) >= count)
            return tuple(waiter.point for waiter in matches())

    async def release(self, index: int = 0) -> SchedulePoint:
        """Release one currently paused actor and record the decision."""

        async with self.changed:
            if not self.waiting:
                raise RuntimeError("no actors are waiting")
            waiter = self.waiting.pop(index % len(self.waiting))
            self.decisions.append(waiter.point)
            waiter.released.set()
            self.changed.notify_all()
            return waiter.point

    async def release_matching(self, operation: str, phase: str = "") -> SchedulePoint:
        """Release the first actor at a particular operation boundary."""

        async with self.changed:
            for index, waiter in enumerate(self.waiting):
                if waiter.point.operation == operation and (
                    not phase or waiter.point.phase == phase
                ):
                    selected = self.waiting.pop(index)
                    self.decisions.append(selected.point)
                    selected.released.set()
                    self.changed.notify_all()
                    return selected.point
        raise RuntimeError(f"no actor is waiting at {operation}:{phase}")

    async def release_all(self) -> None:
        """Release every currently paused actor."""

        while self.waiting:
            await self.release()

    def choose(self, choices: Sequence[int], step: int) -> int:
        """Map generated integers onto the currently enabled actor set."""

        if not self.waiting:
            raise RuntimeError("no actors are available")
        if not choices:
            return 0
        return choices[step % len(choices)] % len(self.waiting)

    def trace(self) -> ScheduleTrace:
        """Return the exact decisions made so far."""

        return ScheduleTrace(decisions=tuple(self.decisions))


class CheckpointHooks:
    """Route selected production checkpoints through a scheduler."""

    def __init__(
        self,
        scheduler: DeterministicScheduler,
        operations: Sequence[RuntimeCheckpoint],
    ) -> None:
        self.scheduler = scheduler
        self.operations = frozenset(operations)

    async def checkpoint(
        self,
        operation: RuntimeCheckpoint,
        **details: object,
    ) -> None:
        """Pause only checkpoints selected by this campaign."""

        if operation not in self.operations:
            return
        await self.scheduler.pause(
            actor=str(details.get("actor", "runtime")),
            operation=operation,
            phase=str(details.get("phase", "reached")),
            details={
                key: str(value) for key, value in details.items() if key not in {"actor", "phase"}
            },
        )
