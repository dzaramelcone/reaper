"""Pure control rules shared by live and fake Reapers."""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class SkeletonState(StrEnum):
    STARTING = "starting"
    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"
    DEAD = "dead"


class SkeletonID(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    fd: int
    gen: int


class EventKind(StrEnum):
    START = "start"
    RECOVER = "recover"
    SPAWNED = "spawned"
    SPAWN_FAILED = "spawn_failed"
    READY = "ready"
    SUBMIT = "submit"
    RESULT = "result"
    EOF = "eof"
    SEND_FAILED = "send_failed"
    CLOSE = "close"
    DEADLINE = "deadline"


class EffectKind(StrEnum):
    SPAWN = "spawn"
    SEND_RUN = "send_run"
    SEND_STOP = "send_stop"
    CLOSE_DEATH = "close_death"
    RESOLVE = "resolve"
    FAIL = "fail"
    DROP = "drop"
    CLOSE_CONTROL = "close_control"
    REAP = "reap"
    KILL = "kill"


class FailureReason(StrEnum):
    """Explain why submitted process work could not finish."""

    WORKER_ERROR = "worker_error"
    WORKER_DIED = "worker_died"
    CLOSED = "closed"


class ControlEvent(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    kind: EventKind
    identity: SkeletonID | None = None
    gen: int | None = None
    job: str | None = None
    ok: bool = True


class ControlEffect(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    kind: EffectKind
    identity: SkeletonID | None = None
    gen: int | None = None
    job: str | None = None
    reason: FailureReason | None = None


class ControlSlot(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    identity: SkeletonID
    state: SkeletonState
    job: str | None = None


class ControlView(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    closing: bool
    slots: tuple[ControlSlot, ...]
    pending_spawns: tuple[int, ...]
    queue: tuple[str, ...]
    done: tuple[str, ...]
    failed: tuple[str, ...]


class ReaperCore:
    """Reduce ordered events into side-effect requests."""

    def __init__(self, target: int) -> None:
        if target <= 0:
            raise ValueError("target must be more than zero")
        self.target = target
        self.closing = False
        self.slots: dict[int, ControlSlot] = {}
        self.pending_spawns: set[int] = set()
        self.queue: list[str] = []
        self.done: list[str] = []
        self.failed: list[str] = []
        self.next_gen = 0

    def apply(self, event: ControlEvent) -> list[ControlEffect]:
        effects: list[ControlEffect] = []
        match event.kind:
            case EventKind.START:
                effects.extend(self.fill())
            case EventKind.RECOVER:
                effects.extend(self.fill())
            case EventKind.SPAWNED:
                effects.extend(self.spawned(event))
            case EventKind.SPAWN_FAILED:
                effects.extend(self.spawn_failed(event))
            case EventKind.READY:
                effects.extend(self.ready(event))
            case EventKind.SUBMIT:
                effects.extend(self.submit(event))
            case EventKind.RESULT:
                effects.extend(self.result(event))
            case EventKind.EOF | EventKind.SEND_FAILED:
                effects.extend(self.lost(event))
            case EventKind.CLOSE:
                effects.extend(self.close())
            case EventKind.DEADLINE:
                effects.extend(self.deadline())
        effects.extend(self.dispatch())
        self.assert_invariants()
        return effects

    def assert_invariants(self) -> None:
        """Check state facts after each event."""

        assert len(self.slots) + len(self.pending_spawns) <= self.target
        assert all(fd == slot.identity.fd for fd, slot in self.slots.items())
        assert all(
            bool(slot.job) is (slot.state is SkeletonState.RUNNING) for slot in self.slots.values()
        )
        jobs = list(self.queue)
        jobs.extend(slot.job for slot in self.slots.values() if slot.job)
        assert len(jobs) == len(set(jobs))
        gens = self.pending_spawns | {slot.identity.gen for slot in self.slots.values()}
        assert len(gens) == len(self.pending_spawns) + len(self.slots)
        assert not gens or self.next_gen >= max(gens)
        assert not self.closing or not self.pending_spawns

    def spawned(self, event: ControlEvent) -> list[ControlEffect]:
        if event.gen is None or event.identity is None:
            return []
        if event.gen not in self.pending_spawns:
            return self.discard(event.identity)
        self.pending_spawns.remove(event.gen)
        if self.closing:
            return self.discard(event.identity)
        self.slots[event.identity.fd] = ControlSlot(
            identity=event.identity,
            state=SkeletonState.STARTING,
        )
        return self.fill()

    def spawn_failed(self, event: ControlEvent) -> list[ControlEffect]:
        if event.gen in self.pending_spawns:
            self.pending_spawns.remove(event.gen)
        return []

    def ready(self, event: ControlEvent) -> list[ControlEffect]:
        slot = self.find(event.identity)
        if slot is None or slot.state != SkeletonState.STARTING:
            return []
        self.slots[slot.identity.fd] = slot.model_copy(update={"state": SkeletonState.IDLE})
        return []

    def submit(self, event: ControlEvent) -> list[ControlEffect]:
        if self.closing or event.job is None:
            return []
        known = set(self.queue)
        known.update(slot.job for slot in self.slots.values() if slot.job)
        if event.job not in known:
            self.queue.append(event.job)
        return []

    def result(self, event: ControlEvent) -> list[ControlEffect]:
        slot = self.find(event.identity)
        if slot is None or slot.state != SkeletonState.RUNNING:
            return []
        if slot.job != event.job or event.job is None:
            return []
        self.slots[slot.identity.fd] = slot.model_copy(
            update={"state": SkeletonState.IDLE, "job": None}
        )
        if event.ok:
            return [ControlEffect(kind=EffectKind.RESOLVE, job=event.job)]
        return [
            ControlEffect(
                kind=EffectKind.FAIL,
                job=event.job,
                reason=FailureReason.WORKER_ERROR,
            )
        ]

    def lost(self, event: ControlEvent) -> list[ControlEffect]:
        slot = self.find(event.identity)
        if slot is None:
            return []
        del self.slots[slot.identity.fd]
        effects = self.discard(slot.identity)
        if slot.job:
            effects.append(
                ControlEffect(
                    kind=EffectKind.FAIL,
                    job=slot.job,
                    reason=FailureReason.WORKER_DIED,
                )
            )
        return effects

    def discard(self, identity: SkeletonID) -> list[ControlEffect]:
        return [
            ControlEffect(kind=EffectKind.DROP, identity=identity),
            ControlEffect(kind=EffectKind.CLOSE_CONTROL, identity=identity),
            ControlEffect(kind=EffectKind.CLOSE_DEATH, identity=identity),
            ControlEffect(kind=EffectKind.REAP, identity=identity),
        ]

    def close(self) -> list[ControlEffect]:
        if self.closing:
            return []
        self.closing = True
        effects: list[ControlEffect] = []
        for job in self.queue:
            effects.append(
                ControlEffect(kind=EffectKind.FAIL, job=job, reason=FailureReason.CLOSED)
            )
        self.queue.clear()
        self.pending_spawns.clear()
        for fd, slot in tuple(self.slots.items()):
            if slot.job:
                effects.append(
                    ControlEffect(
                        kind=EffectKind.FAIL,
                        job=slot.job,
                        reason=FailureReason.CLOSED,
                    )
                )
            self.slots[fd] = slot.model_copy(update={"state": SkeletonState.STOPPING, "job": None})
            effects.append(ControlEffect(kind=EffectKind.SEND_STOP, identity=slot.identity))
        return effects

    def deadline(self) -> list[ControlEffect]:
        if not self.closing:
            return []
        return [
            ControlEffect(kind=EffectKind.KILL, identity=slot.identity)
            for slot in self.slots.values()
        ]

    def fill(self) -> list[ControlEffect]:
        effects: list[ControlEffect] = []
        total = len(self.slots) + len(self.pending_spawns)
        while not self.closing and total < self.target:
            self.next_gen += 1
            self.pending_spawns.add(self.next_gen)
            effects.append(ControlEffect(kind=EffectKind.SPAWN, gen=self.next_gen))
            total += 1
        return effects

    def dispatch(self) -> list[ControlEffect]:
        effects: list[ControlEffect] = []
        for fd, slot in tuple(self.slots.items()):
            if not self.queue:
                return effects
            if slot.state != SkeletonState.IDLE:
                continue
            job = self.queue.pop(0)
            self.slots[fd] = slot.model_copy(update={"state": SkeletonState.RUNNING, "job": job})
            effects.append(
                ControlEffect(
                    kind=EffectKind.SEND_RUN,
                    identity=slot.identity,
                    job=job,
                )
            )
        return effects

    def find(self, identity: SkeletonID | None) -> ControlSlot | None:
        if identity is None:
            return None
        slot = self.slots.get(identity.fd)
        if slot is None or slot.identity != identity:
            return None
        return slot

    def view(self) -> ControlView:
        slots = tuple(sorted(self.slots.values(), key=lambda slot: slot.identity.fd))
        return ControlView(
            closing=self.closing,
            slots=slots,
            pending_spawns=tuple(sorted(self.pending_spawns)),
            queue=tuple(self.queue),
            done=tuple(self.done),
            failed=tuple(self.failed),
        )
