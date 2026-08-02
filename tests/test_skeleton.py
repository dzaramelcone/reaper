"""Pure skeleton lifecycle transition checks."""

import pytest

from reaper.models import ResultState
from reaper.skeleton import (
    LifecycleEvent,
    LifecycleKind,
    LifecycleLevel,
    ListenerPhase,
    SkeletonCore,
    SkeletonPhase,
    TaskReleaseReason,
)


def deliver(
    core: SkeletonCore,
    kind: LifecycleKind,
    *,
    task_id: str = "",
    version: int = 0,
    outcome: ResultState | None = None,
    release_reason: TaskReleaseReason | None = None,
    detail: str = "",
) -> None:
    effects = core.apply(
        LifecycleEvent(
            kind=kind,
            task_id=task_id,
            version=version,
            outcome=outcome,
            release_reason=release_reason,
            detail=detail,
        )
    )
    assert len(effects) == 1
    assert effects[0].event is kind


def test_task_lifecycle_tracks_claim_outcome_and_commit() -> None:
    core = SkeletonCore()
    deliver(core, LifecycleKind.START)
    deliver(core, LifecycleKind.LISTENER_OPENED)
    deliver(core, LifecycleKind.READY)
    deliver(core, LifecycleKind.TASK_CLAIMED, task_id="promise", version=4)
    assert core.view().phase is SkeletonPhase.RUNNING
    deliver(
        core,
        LifecycleKind.TASK_OUTCOME,
        task_id="promise",
        version=4,
        outcome=ResultState.RESOLVED,
    )
    assert core.view().phase is SkeletonPhase.SETTLING
    deliver(core, LifecycleKind.TASK_COMMITTED, task_id="promise", version=4)
    assert core.view().phase is SkeletonPhase.IDLE
    assert core.view().task_id == ""


def test_listener_recycling_is_independent_of_running_task() -> None:
    core = SkeletonCore()
    deliver(core, LifecycleKind.START)
    deliver(core, LifecycleKind.LISTENER_OPENED)
    deliver(core, LifecycleKind.READY)
    deliver(core, LifecycleKind.TASK_CLAIMED, task_id="promise", version=2)
    deliver(core, LifecycleKind.LISTENER_RECYCLED)
    view = core.view()
    assert view.phase is SkeletonPhase.RUNNING
    assert view.listener is ListenerPhase.LISTENING
    assert view.listener_generation == 2


def test_fault_during_execution_clears_claim() -> None:
    core = SkeletonCore()
    deliver(core, LifecycleKind.START)
    deliver(core, LifecycleKind.READY)
    deliver(core, LifecycleKind.TASK_CLAIMED, task_id="promise", version=2)
    deliver(core, LifecycleKind.FAULT, task_id="promise", version=2)
    assert core.view().phase is SkeletonPhase.BACKOFF
    assert core.view().task_id == ""


def test_incompatible_task_release_returns_to_idle() -> None:
    core = SkeletonCore()
    deliver(core, LifecycleKind.START)
    deliver(core, LifecycleKind.READY)
    deliver(core, LifecycleKind.TASK_CLAIMED, task_id="promise", version=2)
    deliver(
        core,
        LifecycleKind.TASK_UNAVAILABLE,
        task_id="promise",
        version=2,
        release_reason=TaskReleaseReason.FUNCTION_UNAVAILABLE,
    )
    deliver(
        core,
        LifecycleKind.TASK_RELEASED,
        task_id="promise",
        version=2,
        release_reason=TaskReleaseReason.FUNCTION_UNAVAILABLE,
    )
    assert core.view().phase is SkeletonPhase.IDLE
    assert core.view().task_id == ""


def test_invalid_task_transition_is_rejected() -> None:
    core = SkeletonCore()
    deliver(core, LifecycleKind.START)
    deliver(core, LifecycleKind.READY)
    with pytest.raises(RuntimeError, match="invalid skeleton transition"):
        deliver(core, LifecycleKind.TASK_OUTCOME, task_id="promise", version=2)


def test_high_frequency_events_are_debug_level() -> None:
    core = SkeletonCore()
    effect = core.apply(LifecycleEvent(kind=LifecycleKind.POLL))[0]
    assert effect.level is LifecycleLevel.DEBUG
    fault = core.apply(LifecycleEvent(kind=LifecycleKind.FAULT, detail="lost db"))[0]
    assert fault.level is LifecycleLevel.DEBUG
