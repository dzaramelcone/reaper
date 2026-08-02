"""Typed settings for Reaper clients and workers."""

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, PostgresDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_BEAT_RATE = 1.0
DEFAULT_POLL_RATE = 30.0
DEFAULT_MAINTENANCE_RATE = 1.0
DEFAULT_LISTENER_PROBE_RATE = 30.0
DEFAULT_LISTENER_RECYCLE_RATE = 300.0
DEFAULT_GC_RATE = 3600.0
DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1_000
DEFAULT_STARTUP_TIMEOUT = 30.0
DEFAULT_SERVICE_FAILURE_ROUNDS = 3
DEFAULT_SERVICE_RETRY_BASE = 0.25
DEFAULT_SERVICE_RETRY_MAX = 30.0
DEFAULT_MAX_QUEUED_JOBS = 10_000
DEFAULT_LOG_LEVEL = "INFO"


class PoolKind(StrEnum):
    TASK = "task"
    MAINTENANCE = "maintenance"


class PoolConfig(BaseModel):
    """Declare one resident skeleton pool."""

    model_config = ConfigDict(frozen=True, strict=True)

    kind: PoolKind = PoolKind.TASK
    skeletons: Annotated[int, Field(gt=0, le=64)]
    topic: Annotated[str, Field(min_length=1, max_length=255)] | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_flag(cls, value: object) -> object:
        """Read compact CLI pool forms."""

        if isinstance(value, int):
            return {"skeletons": value}
        if not isinstance(value, str):
            return value
        parts = value.split(":")
        match parts:
            case [count] if count.isdigit():
                return {"skeletons": int(count)}
            case [kind, count] if kind == PoolKind.MAINTENANCE and count.isdigit():
                return {
                    "kind": PoolKind.MAINTENANCE,
                    "skeletons": int(count),
                }
            case [topic, count] if topic and count.isdigit():
                return {
                    "kind": PoolKind.TASK,
                    "skeletons": int(count),
                    "topic": topic,
                }
            case _:
                raise ValueError(f"use TOPIC:COUNT, COUNT, or {PoolKind.MAINTENANCE}:COUNT")

    @model_validator(mode="after")
    def validate_topic(self) -> PoolConfig:
        if self.kind is PoolKind.MAINTENANCE and self.topic is not None:
            raise ValueError("a maintenance pool cannot have a task topic")
        return self


class ReaperSettings(BaseSettings):
    """Load the SQL link and pool shape."""

    model_config = SettingsConfigDict(
        env_prefix="REAPER_",
        extra="ignore",
        frozen=True,
        populate_by_name=True,
        cli_kebab_case=True,
    )

    postgres_dsn: PostgresDsn
    beat_rate: Annotated[float, Field(gt=0)] = DEFAULT_BEAT_RATE
    poll_rate: Annotated[float, Field(gt=0, le=3600)] = DEFAULT_POLL_RATE
    maintenance_rate: Annotated[float, Field(gt=0)] = DEFAULT_MAINTENANCE_RATE
    listener_probe_rate: Annotated[float, Field(gt=0, le=300)] = DEFAULT_LISTENER_PROBE_RATE
    listener_recycle_rate: Annotated[float, Field(gt=0, le=3600)] = DEFAULT_LISTENER_RECYCLE_RATE
    gc_rate: Annotated[float, Field(gt=0, le=86_400)] = DEFAULT_GC_RATE
    retention_ms: Annotated[
        int,
        Field(ge=60_000, le=365 * 24 * 60 * 60 * 1_000),
    ] = DEFAULT_RETENTION_MS
    startup_timeout: Annotated[float, Field(gt=0, le=300)] = DEFAULT_STARTUP_TIMEOUT
    service_failure_rounds: Annotated[int, Field(gt=0, le=10)] = DEFAULT_SERVICE_FAILURE_ROUNDS
    service_retry_base: Annotated[float, Field(gt=0, le=30)] = DEFAULT_SERVICE_RETRY_BASE
    service_retry_max: Annotated[float, Field(gt=0, le=300)] = DEFAULT_SERVICE_RETRY_MAX
    max_queued_jobs: Annotated[int, Field(gt=0, le=100_000)] = DEFAULT_MAX_QUEUED_JOBS
    log_level: str = DEFAULT_LOG_LEVEL
    pools: list[PoolConfig] = Field(default_factory=list, alias="pool")

    @model_validator(mode="after")
    def bound_horde(self) -> ReaperSettings:
        """Bound total local process use."""

        if len(self.pools) > 32:
            raise ValueError("a Reaper supports at most 32 pools")
        if sum(pool.skeletons for pool in self.pools) > 256:
            raise ValueError("a Reaper supports at most 256 skeletons")
        if self.service_retry_base > self.service_retry_max:
            raise ValueError("service retry base cannot exceed its max")
        if self.listener_probe_rate > self.listener_recycle_rate:
            raise ValueError("listener probe rate cannot exceed its recycle rate")
        return self
