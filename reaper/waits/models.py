"""Pydantic models for durable task waits."""

from typing import Annotated, Self

from pydantic import Field, JsonValue, model_validator

from reaper.database import QueryModel
from reaper.models import PromiseState

MAX_ID_BYTES = 1024


class WaitedPromise(QueryModel):
    id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)]
    state: PromiseState
    result: JsonValue | None
    error: JsonValue | None
    settled_at_ms: Annotated[int, Field(ge=0)] | None


class SuspendTask(QueryModel):
    id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)]
    awaited_ids: Annotated[tuple[str, ...], Field(min_length=1, max_length=1024)]

    @model_validator(mode="after")
    def validate_awaits(self) -> Self:
        if self.id in self.awaited_ids:
            raise ValueError("a task cannot await itself")
        if len(self.awaited_ids) != len(set(self.awaited_ids)):
            raise ValueError("awaited_ids must be unique")
        return self
