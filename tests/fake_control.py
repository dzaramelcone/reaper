"""Deterministic control backends used only by tests."""

from pydantic import BaseModel, ConfigDict

from reaper.control import ControlEffect, ControlEvent, ControlView, EventKind, ReaperCore


class EffectCall(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    seq: int
    effect: ControlEffect


class TimedEvent(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    at: float
    seq: int
    event: ControlEvent


class FakeClock:
    """Release events when tests advance time."""

    def __init__(self, start: float = 0.0) -> None:
        self.time = start
        self.next_seq = 0
        self.events: list[TimedEvent] = []

    def now(self) -> float:
        return self.time

    def schedule(self, delay: float, event: ControlEvent) -> TimedEvent:
        if delay < 0:
            raise ValueError("delay must not be less than zero")
        self.next_seq += 1
        item = TimedEvent(
            at=self.time + delay,
            seq=self.next_seq,
            event=event,
        )
        self.events.append(item)
        self.events.sort(key=lambda value: (value.at, value.seq))
        return item

    def advance(self, amount: float) -> tuple[ControlEvent, ...]:
        if amount < 0:
            raise ValueError("time must not move back")
        self.time += amount
        due = [item for item in self.events if item.at <= self.time]
        self.events = [item for item in self.events if item.at > self.time]
        return tuple(item.event for item in due)


class DeterministicDriver:
    """Order every reducer effect and reply."""

    def __init__(self, target: int) -> None:
        self.core = ReaperCore(target)
        self.next_seq = 0
        self.effects: list[EffectCall] = []

    def deliver(self, event: ControlEvent) -> None:
        self.enqueue(self.core.apply(event))

    def enqueue(self, effects: list[ControlEffect]) -> None:
        for effect in effects:
            self.next_seq += 1
            self.effects.append(EffectCall(seq=self.next_seq, effect=effect))

    def take(self, index: int = 0) -> EffectCall:
        return self.effects.pop(index)

    def reply(self, call: EffectCall, event: ControlEvent) -> None:
        if event.kind == EventKind.SPAWNED and call.effect.gen != event.gen:
            raise ValueError("spawn reply has the wrong gen")
        self.deliver(event)

    def view(self) -> ControlView:
        return self.core.view()
