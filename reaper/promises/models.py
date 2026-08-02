"""Pydantic models for durable promise parameters and results."""

import uuid
from typing import Annotated, Self

from pydantic import AwareDatetime, Field, JsonValue, model_validator

from reaper.database import QueryModel, model_fingerprint
from reaper.models import PromiseState
from reaper.settings import DEFAULT_RETENTION_MS

MAX_ID_BYTES = 1024


def uuid4() -> str:
    return str(uuid.uuid4())


class PromiseRecord(QueryModel):
    id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)]
    idempotency_key: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    state: PromiseState
    root_id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)] | None
    result: JsonValue | None
    error: JsonValue | None
    due_at: AwareDatetime | None
    expires_at: AwareDatetime | None
    delete_after: AwareDatetime | None
    settled_at: AwareDatetime | None


class SubmitTimer(QueryModel):
    id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)] = Field(default_factory=uuid4)
    due_at: AwareDatetime
    retention_ms: Annotated[int, Field(ge=60_000, le=365 * 24 * 60 * 60 * 1_000)] = (
        DEFAULT_RETENTION_MS
    )
    root_id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)] | None = None

    @model_validator(mode="after")
    def validate_graph_and_retention(self) -> Self:
        if self.id == self.root_id:
            raise ValueError("a promise cannot be its own root")
        return self

    def fingerprint(self) -> str:
        excluded = frozenset({"retention_ms"}) if self.root_id is not None else frozenset()
        return model_fingerprint(self, exclude=excluded)

