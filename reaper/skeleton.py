"""Pure lifecycle rules for one skeleton process."""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from reaper.models import ResultState


class SkeletonPhase(StrEnum):
    """Describe the service work owned by one skeleton."""

    STARTING = "starting"
    CONNECTING = "connecting"
    IDLE = "idle"
    RUNNING = "running"
    SETTLING = "settling"
    BACKOFF = "backoff"
    STOPPING = "stopping"
    DEAD = "dead"


class ListenerPhase(StrEnum):
    """Describe the dedicated LISTEN connection."""

    CLOSED = "closed"
    LISTENING = "listening"


class LifecycleKind(StrEnum):
    """Events accepted by the skeleton lifecycle reducer."""

    START = "start"
    LINK_OPENED = "link_opened"
    READY = "ready"
    POLL = "poll"
    FALLBACK_POLL = "fallback_poll"
    LISTENER_OPENED = "listener_opened"
    LISTENER_NOTIFIED = "listener_notified"
    LISTENER_PROBED = "listener_probed"
    LISTENER_RECYCLED = "listener_recycled"
    LISTENER_CLOSED = "listener_closed"
    TASK_CLAIMED = "task_claimed"
    TASK_OUTCOME = "task_outcome"
    TASK_UNAVAILABLE = "task_unavailable"
    TASK_COMMITTED = "task_committed"
    TASK_RELEASED = "task_released"
    WORK_STARTED = "work_started"
    WORK_FINISHED = "work_finished"
    MAINTENANCE_POLL = "maintenance_poll"
    GC_FINISHED = "gc_finished"
    FAULT = "fault"
    STOP = "stop"
    STOPPED = "stopped"


class LifecycleLevel(StrEnum):
    """Portable logging levels sent over the control socket."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class WorkOutcome(StrEnum):
    """Describe the result of ad hoc process-pool work."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"


class TaskReleaseReason(StrEnum):
    """Explain why a claimed durable task was rolled back."""

    FUNCTION_UNAVAILABLE = "function_unavailable"


class LifecycleEvent(BaseModel):
    """Hold one ordered skeleton transition."""

    model_config = ConfigDict(frozen=True, strict=True)

    kind: LifecycleKind
    task_id: str = ""
    version: int = 0
    outcome: ResultState | WorkOutcome | None = None
    release_reason: TaskReleaseReason | None = None
    count: int | None = None
    detail: str = ""


class LifecycleEffect(BaseModel):
    """Request one structured record from the effect driver."""

    model_config = ConfigDict(frozen=True, strict=True)

    level: LifecycleLevel
    event: LifecycleKind
    phase: SkeletonPhase
    listener: ListenerPhase
    listener_generation: int
    task_id: str = ""
    version: int = 0
    outcome: ResultState | WorkOutcome | None = None
    release_reason: TaskReleaseReason | None = None
    count: int | None = None
    detail: str = ""


class SkeletonView(BaseModel):
    """Expose immutable skeleton lifecycle state."""

    model_config = ConfigDict(frozen=True, strict=True)

    phase: SkeletonPhase
    listener: ListenerPhase
    listener_generation: int
    task_id: str
    version: int


class SkeletonCore:
    """Reduce skeleton events into structured reporting effects."""

    def __init__(self) -> None:
        self.phase = SkeletonPhase.STARTING
        self.listener = ListenerPhase.CLOSED
        self.listener_generation = 0
        self.task_id = ""
        self.version = 0

    def apply(self, event: LifecycleEvent) -> tuple[LifecycleEffect, ...]:
        """Apply one transition and return its observable effect."""

        match event.kind:
            case LifecycleKind.START:
                self.phase = SkeletonPhase.CONNECTING
                self.clear_task()
            case LifecycleKind.READY:
                self.phase = SkeletonPhase.IDLE
            case LifecycleKind.LISTENER_OPENED:
                self.listener = ListenerPhase.LISTENING
                self.listener_generation += 1
            case LifecycleKind.LISTENER_RECYCLED:
                self.listener = ListenerPhase.LISTENING
                self.listener_generation += 1
            case LifecycleKind.LISTENER_CLOSED:
                self.listener = ListenerPhase.CLOSED
            case LifecycleKind.TASK_CLAIMED:
                self.require_phase(SkeletonPhase.IDLE)
                self.phase = SkeletonPhase.RUNNING
                self.set_task(event)
            case LifecycleKind.TASK_OUTCOME:
                self.require_phase(SkeletonPhase.RUNNING)
                self.phase = SkeletonPhase.SETTLING
            case LifecycleKind.TASK_UNAVAILABLE:
                self.require_phase(SkeletonPhase.RUNNING)
                self.phase = SkeletonPhase.SETTLING
            case LifecycleKind.TASK_COMMITTED:
                self.require_phase(SkeletonPhase.SETTLING)
                self.phase = SkeletonPhase.IDLE
                self.clear_task()
            case LifecycleKind.TASK_RELEASED:
                self.require_phase(SkeletonPhase.SETTLING)
                self.phase = SkeletonPhase.IDLE
                self.clear_task()
            case LifecycleKind.WORK_STARTED:
                self.require_phase(SkeletonPhase.IDLE)
                self.phase = SkeletonPhase.RUNNING
                self.set_task(event)
            case LifecycleKind.WORK_FINISHED:
                self.require_phase(SkeletonPhase.RUNNING)
                self.phase = SkeletonPhase.IDLE
                self.clear_task()
            case LifecycleKind.FAULT:
                self.phase = SkeletonPhase.BACKOFF
                self.listener = ListenerPhase.CLOSED
                self.clear_task()
            case LifecycleKind.STOP:
                self.phase = SkeletonPhase.STOPPING
                self.clear_task()
            case LifecycleKind.STOPPED:
                self.phase = SkeletonPhase.DEAD
                self.listener = ListenerPhase.CLOSED
                self.clear_task()
            case _:
                pass
        self.assert_invariants()
        return (
            LifecycleEffect(
                level=self.level_for(event.kind),
                event=event.kind,
                phase=self.phase,
                listener=self.listener,
                listener_generation=self.listener_generation,
                task_id=event.task_id or self.task_id,
                version=event.version or self.version,
                outcome=event.outcome,
                release_reason=event.release_reason,
                count=event.count,
                detail=event.detail,
            ),
        )

    def set_task(self, event: LifecycleEvent) -> None:
        if not event.task_id:
            raise ValueError(f"{event.kind} needs a task id")
        self.task_id = event.task_id
        self.version = event.version

    def clear_task(self) -> None:
        self.task_id = ""
        self.version = 0

    def require_phase(self, *phases: SkeletonPhase) -> None:
        if self.phase not in phases:
            expected = ", ".join(phase.value for phase in phases)
            raise RuntimeError(
                f"invalid skeleton transition from {self.phase}; expected {expected}"
            )

    def assert_invariants(self) -> None:
        owns_task = self.phase in {
            SkeletonPhase.RUNNING,
            SkeletonPhase.SETTLING,
        }
        assert bool(self.task_id) is owns_task
        assert not self.task_id or self.version >= 0
        assert self.listener_generation >= 0
        assert self.phase is not SkeletonPhase.DEAD or self.listener is ListenerPhase.CLOSED

    def view(self) -> SkeletonView:
        return SkeletonView(
            phase=self.phase,
            listener=self.listener,
            listener_generation=self.listener_generation,
            task_id=self.task_id,
            version=self.version,
        )

    @staticmethod
    def level_for(kind: LifecycleKind) -> LifecycleLevel:
        if kind in {
            LifecycleKind.START,
            LifecycleKind.LINK_OPENED,
            LifecycleKind.READY,
            LifecycleKind.POLL,
            LifecycleKind.FALLBACK_POLL,
            LifecycleKind.LISTENER_OPENED,
            LifecycleKind.LISTENER_NOTIFIED,
            LifecycleKind.LISTENER_PROBED,
            LifecycleKind.LISTENER_CLOSED,
            LifecycleKind.TASK_CLAIMED,
            LifecycleKind.TASK_OUTCOME,
            LifecycleKind.TASK_UNAVAILABLE,
            LifecycleKind.TASK_RELEASED,
            LifecycleKind.WORK_STARTED,
            LifecycleKind.WORK_FINISHED,
            LifecycleKind.MAINTENANCE_POLL,
            LifecycleKind.STOP,
            LifecycleKind.STOPPED,
            LifecycleKind.FAULT,
        }:
            return LifecycleLevel.DEBUG
        return LifecycleLevel.INFO
