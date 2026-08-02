"""Deterministic model for the asynchronous pool-start contract."""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from reaper.control import SkeletonState


class StartupLife(StrEnum):
    """Describe the externally visible pool startup phase."""

    WAITING = "waiting"
    RETURNED = "returned"
    FAILED = "failed"


class StartupSlot(BaseModel):
    """Track one generated skeleton during startup."""

    model_config = ConfigDict(frozen=True, strict=True)

    generation: Annotated[int, Field(gt=0)]
    state: SkeletonState = SkeletonState.STARTING


class StartupModel:
    """Model readiness separately from the pure control reducer."""

    def __init__(self, target: int) -> None:
        if target <= 0:
            raise ValueError("startup target must be positive")
        self.target = target
        self.life = StartupLife.WAITING
        self.slots: dict[int, StartupSlot] = {}

    def spawn(self, generation: int) -> None:
        """Register one starting skeleton."""

        if self.life is not StartupLife.WAITING:
            return
        self.slots[generation] = StartupSlot(generation=generation)
        self.assert_invariants()

    def ready(self, generation: int) -> None:
        """Mark one live skeleton ready."""

        slot = self.slots.get(generation)
        if slot is None or self.life is not StartupLife.WAITING:
            return
        self.slots[generation] = slot.model_copy(update={"state": SkeletonState.IDLE})
        self.assert_invariants()

    def lose(self, generation: int) -> None:
        """Remove one skeleton that dies before startup returns."""

        if self.life is StartupLife.WAITING:
            self.slots.pop(generation, None)
        self.assert_invariants()

    def can_return(self) -> bool:
        """Require exact ready target capacity."""

        return len(self.slots) == self.target and all(
            slot.state is SkeletonState.IDLE for slot in self.slots.values()
        )

    def return_if_ready(self) -> bool:
        """Expose successful startup only when the readiness contract holds."""

        if not self.can_return():
            return False
        self.life = StartupLife.RETURNED
        self.assert_invariants()
        return True

    def fail(self) -> None:
        """Finish startup with an explicit failure."""

        if self.life is StartupLife.WAITING:
            self.life = StartupLife.FAILED
        self.assert_invariants()

    def assert_invariants(self) -> None:
        """Check safety after every generated transition."""

        assert len(self.slots) <= self.target
        assert all(generation > 0 for generation in self.slots)
        if self.life is StartupLife.RETURNED:
            assert self.can_return()
