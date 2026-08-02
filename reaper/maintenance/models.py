"""Pydantic models for bounded timer and retention maintenance."""

from datetime import UTC, datetime
from typing import Annotated

from pydantic import AwareDatetime, Field

from reaper.database import QueryModel


class ProcessDue(QueryModel):
    limit: Annotated[int, Field(ge=1, le=10000)] = 500


class ProcessedDue(QueryModel):
    timers: Annotated[int, Field(ge=0)]
    timeouts: Annotated[int, Field(ge=0)]


class DeleteExpired(QueryModel):
    before: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    limit: Annotated[int, Field(ge=1, le=100000)] = 10000


class Deleted(QueryModel):
    roots: Annotated[int, Field(ge=0)]
