"""Pydantic models for executable durable tasks."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Self

from pydantic import AwareDatetime, Field, JsonValue, model_validator

from reaper.database import QueryModel, model_fingerprint
from reaper.models import DEFAULT_TOPIC
from reaper.settings import DEFAULT_RETENTION_MS
from reaper.waits.models import WaitedPromise

MAX_PROMISE_LIFETIME_MS = 365 * 24 * 60 * 60 * 1_000
MAX_ID_BYTES = 1024
MAX_FUNCTION_BYTES = 1024
MAX_TOPIC_BYTES = 255
MAX_DEPTH = 256
SUBMISSION_TIME_FIELDS = frozenset({"available_at", "expires_at"})


def now() -> datetime:
    return datetime.now(UTC)


def expires() -> datetime:
    return now() + timedelta(days=365)


class ClaimedTask(QueryModel):
    id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)]
    root_id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)] | None
    function: Annotated[str, Field(min_length=1, max_length=MAX_FUNCTION_BYTES)]
    version: Annotated[int, Field(gt=0)]
    topic: Annotated[str, Field(min_length=1, max_length=MAX_TOPIC_BYTES)]
    input: JsonValue
    depth: Annotated[int, Field(ge=0, le=MAX_DEPTH)]
    max_failures: Annotated[int, Field(ge=1, le=100)]
    execution_timeout_ms: Annotated[int, Field(ge=1, le=86_400_000)]
    waits: tuple[WaitedPromise, ...]


class SubmitCall(QueryModel):
    id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)] = Field(
        default_factory=lambda: str(uuid.uuid4())
    )
    function: Annotated[str, Field(min_length=1, max_length=MAX_FUNCTION_BYTES)]
    input: JsonValue
    topic: Annotated[str, Field(min_length=1, max_length=MAX_TOPIC_BYTES)] = DEFAULT_TOPIC
    version: Annotated[int, Field(gt=0)] = 1
    depth: Annotated[int, Field(ge=0, le=MAX_DEPTH)] = 0
    priority: Annotated[int, Field(ge=-32768, le=32767)] = 0
    available_at: AwareDatetime = Field(default_factory=now)
    execution_timeout_ms: Annotated[int, Field(ge=1, le=86_400_000)] = 30_000
    expires_at: AwareDatetime = Field(default_factory=expires)
    retention_ms: Annotated[int, Field(ge=60_000, le=MAX_PROMISE_LIFETIME_MS)] = (
        DEFAULT_RETENTION_MS
    )
    max_failures: Annotated[int, Field(ge=1, le=100)] = 100
    root_id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)] | None = None

    @model_validator(mode="after")
    def validate_graph_and_retention(self) -> Self:
        if self.id == self.root_id:
            raise ValueError("a promise cannot be its own root")
        return self

    def fingerprint(self) -> str:
        """Identify work independently of when its first submission arrived."""

        excluded = SUBMISSION_TIME_FIELDS
        if self.root_id is not None:
            excluded |= {"retention_ms"}
        return model_fingerprint(self, exclude=excluded)


class FunctionVersion(QueryModel):
    function: Annotated[str, Field(min_length=1, max_length=MAX_FUNCTION_BYTES)]
    version: Annotated[int, Field(gt=0)]


class ClaimTask(QueryModel):
    topic: Annotated[str, Field(min_length=1, max_length=MAX_TOPIC_BYTES)] = DEFAULT_TOPIC
    excluded: tuple[FunctionVersion, ...] = ()


class CompleteTask(QueryModel):
    id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)]
    result: JsonValue


class RejectTask(QueryModel):
    id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)]
    error: JsonValue


class RetryTask(QueryModel):
    id: Annotated[str, Field(min_length=1, max_length=MAX_ID_BYTES)]
    error: JsonValue
    delay_ms: Annotated[int, Field(ge=0, le=MAX_PROMISE_LIFETIME_MS)] = 0


class RetryResult(QueryModel):
    rejected: bool
    failures: Annotated[int, Field(gt=0, le=100)]
