"""Shared domain models for Reaper."""

from enum import StrEnum

DEFAULT_TOPIC = "default"


class PromiseState(StrEnum):
    """Persisted promise states."""

    PENDING = "pending"
    RESOLVED = "resolved"
    REJECTED = "rejected"
    TIMED_OUT = "timed_out"


class ResultState(StrEnum):
    """Outcomes produced by one durable function attempt."""

    RESOLVED = "resolved"
    REJECTED = "rejected"
    RETRY = "retry"
    SUSPENDED = "suspended"
