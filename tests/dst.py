"""Seeded state tests for Reaper control races."""

from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Protocol, Self

from pydantic import BaseModel, ConfigDict

from reaper.control import (
    ControlEffect,
    ControlEvent,
    ControlSlot,
    ControlView,
    EffectKind,
    EventKind,
    ReaperCore,
    SkeletonID,
    SkeletonState,
)


class Scenario(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    version: int = 1
    seed: int
    target: int
    events: tuple[ControlEvent, ...]


class Failure(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    index: int
    event: ControlEvent
    expected: ControlView
    actual: ControlView


class Trophy(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    scenario: Scenario
    failure: Failure

    def save(self, path: Path) -> None:
        """Save an exact replay case."""

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=2) + "\n", encoding="utf8")

    @classmethod
    def load(cls, path: Path) -> Self:
        """Load an exact replay case."""

        return cls.model_validate_json(path.read_text(encoding="utf8"), strict=True)


class RoutedEvent(BaseModel):
    """Send one control event to one tree node."""

    model_config = ConfigDict(frozen=True, strict=True)

    path: tuple[int, ...]
    owner: SkeletonID | None
    event: ControlEvent


class TreeScenario(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    version: int = 1
    seed: int
    target: int
    depth: int
    events: tuple[RoutedEvent, ...]


class TreeNodeView(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    path: tuple[int, ...]
    owner: SkeletonID | None
    reaper: ControlView


class TreeView(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    nodes: tuple[TreeNodeView, ...]


class TreeFailure(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    index: int
    routed: RoutedEvent
    expected: TreeView
    actual: TreeView


class TreeTrophy(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    scenario: TreeScenario
    failure: TreeFailure

    def save(self, path: Path) -> None:
        """Save an exact tree replay."""

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=2) + "\n", encoding="utf8")

    @classmethod
    def load(cls, path: Path) -> Self:
        """Load an exact tree replay."""

        return cls.model_validate_json(path.read_text(encoding="utf8"), strict=True)


class SeedStream(BaseModel):
    """Give stable bits for all Python builds."""

    model_config = ConfigDict(strict=True)

    state: int

    def next(self) -> int:
        self.state = (self.state + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
        value = self.state
        value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
        value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
        return value ^ (value >> 31)

    def pick(self, size: int) -> int:
        if size <= 0:
            raise ValueError("pick needs a nonempty set")
        return self.next() % size


class TransitionSystem(Protocol):
    def apply(self, event: ControlEvent) -> None: ...

    def view(self) -> ControlView: ...


class TreeTransitionSystem(Protocol):
    def apply(self, routed: RoutedEvent) -> None: ...

    def view(self) -> TreeView: ...


class ReaperModel:
    """Drive the real reducer with fake spawn effects."""

    def __init__(self, target: int) -> None:
        self.target = target
        self.core = self.make_core(target)
        self.retired: list[ControlSlot] = []
        self.spawn_results: list[bool] = []
        self.drive(self.core.apply(ControlEvent(kind=EventKind.START)))

    def make_core(self, target: int) -> ReaperCore:
        return ReaperCore(target)

    def apply(self, event: ControlEvent) -> None:
        if event.kind == EventKind.EOF:
            slot = self.core.find(event.identity)
            if slot:
                self.retired.append(slot)
        self.drive(self.core.apply(event))
        if event.kind in {
            EventKind.EOF,
            EventKind.SEND_FAILED,
            EventKind.SPAWN_FAILED,
        }:
            self.drive(self.core.apply(ControlEvent(kind=EventKind.RECOVER)))

    def drive(self, effects: list[ControlEffect]) -> None:
        pending = list(effects)
        while pending:
            effect = pending.pop(0)
            match effect.kind:
                case EffectKind.SPAWN if effect.gen is not None:
                    spawn_ok = self.spawn_results.pop(0) if self.spawn_results else True
                    if not spawn_ok:
                        pending.extend(
                            self.core.apply(
                                ControlEvent(
                                    kind=EventKind.SPAWN_FAILED,
                                    gen=effect.gen,
                                )
                            )
                        )
                        pending.extend(self.core.apply(ControlEvent(kind=EventKind.RECOVER)))
                        continue
                    used = set(self.core.slots)
                    fd = next(value for value in range(10, 10 + self.target) if value not in used)
                    identity = SkeletonID(fd=fd, gen=effect.gen)
                    event = ControlEvent(
                        kind=EventKind.SPAWNED,
                        identity=identity,
                        gen=effect.gen,
                    )
                    pending.extend(self.core.apply(event))
                case _:
                    continue

    def fail_next_spawn(self) -> None:
        """Fail one spawn before fair recovery."""

        self.spawn_results.append(False)

    @property
    def slots(self) -> dict[int, ControlSlot]:
        return self.core.slots

    def view(self) -> ControlView:
        return self.core.view()


class ReaperTreeModel:
    """Run real reducers as one owned Reaper tree."""

    def __init__(self, target: int, depth: int) -> None:
        if depth < 0:
            raise ValueError("depth must not be less than zero")
        self.target = target
        self.depth = depth
        self.nodes: dict[tuple[int, ...], ReaperModel] = {(): self.make_model(target)}
        self.owners: dict[tuple[int, ...], SkeletonID | None] = {(): None}
        self.reconcile()

    def make_model(self, target: int) -> ReaperModel:
        return ReaperModel(target)

    def owns(self, path: tuple[int, ...], identity: SkeletonID) -> bool:
        return self.owners.get(path) == identity

    def apply(self, routed: RoutedEvent) -> None:
        node = self.nodes.get(routed.path)
        if node is None or self.owners[routed.path] != routed.owner:
            return
        node.apply(routed.event)
        self.reconcile()

    def reconcile(self) -> None:
        live: dict[tuple[int, ...], SkeletonID | None] = {(): None}
        for level in range(self.depth):
            paths = sorted(path for path in self.nodes if len(path) == level)
            for path in paths:
                node = self.nodes.get(path)
                if node is None or path not in live:
                    continue
                for slot in node.slots.values():
                    child = (*path, slot.identity.fd)
                    live[child] = slot.identity
                    if child not in self.nodes or not self.owns(child, slot.identity):
                        self.nodes[child] = self.make_model(self.target)
                        self.owners[child] = slot.identity
        for path in tuple(self.nodes):
            if path not in live:
                del self.nodes[path]
                del self.owners[path]

    def view(self) -> TreeView:
        views = tuple(
            TreeNodeView(path=path, owner=self.owners[path], reaper=node.view())
            for path, node in sorted(self.nodes.items())
        )
        return TreeView(nodes=views)


class ScenarioCompiler:
    """Turn one seed into exact event order."""

    def compile(self, seed: int, *, target: int = 3, count: int = 100) -> Scenario:
        stream = SeedStream(state=seed & 0xFFFFFFFFFFFFFFFF)
        shadow = ReaperModel(target)
        events: list[ControlEvent] = []
        next_job = 0
        while len(events) < count:
            choice = stream.pick(100)
            starting = self.with_state(shadow, SkeletonState.STARTING)
            running = self.with_state(shadow, SkeletonState.RUNNING)
            active = list(shadow.slots.values())
            if choice < 20 or not active:
                event = ControlEvent(kind=EventKind.SUBMIT, job=f"job-{next_job}")
                next_job += 1
            elif choice < 35 and starting:
                event = ControlEvent(
                    kind=EventKind.READY,
                    identity=starting[stream.pick(len(starting))].identity,
                )
            elif choice < 52 and running:
                slot = running[stream.pick(len(running))]
                event = ControlEvent(
                    kind=EventKind.RESULT,
                    identity=slot.identity,
                    job=slot.job,
                )
            elif choice < 72:
                slot = active[stream.pick(len(active))]
                event = ControlEvent(
                    kind=EventKind.EOF,
                    identity=slot.identity,
                    job=slot.job,
                )
            elif choice < 86 and shadow.retired:
                slot = shadow.retired[stream.pick(len(shadow.retired))]
                event = ControlEvent(
                    kind=EventKind.EOF,
                    identity=slot.identity,
                    job=slot.job,
                )
            elif choice < 97 and shadow.retired:
                slot = shadow.retired[stream.pick(len(shadow.retired))]
                event = ControlEvent(
                    kind=EventKind.RESULT,
                    identity=slot.identity,
                    job=slot.job,
                )
            elif len(events) > count * 3 // 4:
                event = ControlEvent(kind=EventKind.CLOSE)
            else:
                event = ControlEvent(kind=EventKind.SUBMIT, job=f"job-{next_job}")
                next_job += 1
            events.append(event)
            shadow.apply(event)
        return Scenario(seed=seed, target=target, events=tuple(events))

    def with_state(
        self,
        model: ReaperModel,
        state: SkeletonState,
    ) -> list[ControlSlot]:
        return [slot for slot in model.slots.values() if slot.state == state]


class TreeScenarioCompiler:
    """Turn one seed into a nested event order."""

    def compile(
        self,
        seed: int,
        *,
        target: int = 2,
        depth: int = 2,
        count: int = 100,
    ) -> TreeScenario:
        stream = SeedStream(state=seed & 0xFFFFFFFFFFFFFFFF)
        shadow = ReaperTreeModel(target, depth)
        events: list[RoutedEvent] = []
        next_job = 0
        while len(events) < count:
            paths = sorted(shadow.nodes, key=lambda value: (-len(value), value))
            path = paths[stream.pick(len(paths))]
            node = shadow.nodes[path]
            choice = stream.pick(100)
            starting = self.with_state(node, SkeletonState.STARTING)
            running = self.with_state(node, SkeletonState.RUNNING)
            active = list(node.slots.values())
            match choice:
                case value if value < 18 or not active:
                    event = ControlEvent(kind=EventKind.SUBMIT, job=f"job-{next_job}")
                    next_job += 1
                case value if value < 34 and starting:
                    slot = starting[stream.pick(len(starting))]
                    event = ControlEvent(kind=EventKind.READY, identity=slot.identity)
                case value if value < 50 and running:
                    slot = running[stream.pick(len(running))]
                    event = ControlEvent(
                        kind=EventKind.RESULT,
                        identity=slot.identity,
                        job=slot.job,
                    )
                case value if value < 70:
                    slot = active[stream.pick(len(active))]
                    event = ControlEvent(
                        kind=EventKind.EOF,
                        identity=slot.identity,
                        job=slot.job,
                    )
                case value if value < 84 and node.retired:
                    slot = node.retired[stream.pick(len(node.retired))]
                    event = ControlEvent(
                        kind=EventKind.EOF,
                        identity=slot.identity,
                        job=slot.job,
                    )
                case value if value < 96 and node.retired:
                    slot = node.retired[stream.pick(len(node.retired))]
                    event = ControlEvent(
                        kind=EventKind.RESULT,
                        identity=slot.identity,
                        job=slot.job,
                    )
                case _:
                    event = ControlEvent(kind=EventKind.SUBMIT, job=f"job-{next_job}")
                    next_job += 1
            routed = RoutedEvent(
                path=path,
                owner=shadow.owners[path],
                event=event,
            )
            events.append(routed)
            shadow.apply(routed)
        return TreeScenario(
            seed=seed,
            target=target,
            depth=depth,
            events=tuple(events),
        )

    def with_state(
        self,
        model: ReaperModel,
        state: SkeletonState,
    ) -> list[ControlSlot]:
        return [slot for slot in model.slots.values() if slot.state == state]


class Fuzzer:
    """Find and save the first state split."""

    def __init__(self, compiler: ScenarioCompiler | None = None) -> None:
        self.compiler = compiler or ScenarioCompiler()

    def compare(
        self,
        scenario: Scenario,
        make_system: Callable[[int], TransitionSystem],
    ) -> Trophy | None:
        oracle = ReaperModel(scenario.target)
        system = make_system(scenario.target)
        for index, event in enumerate(scenario.events):
            oracle.apply(event)
            system.apply(event)
            expected = oracle.view()
            actual = system.view()
            if actual != expected:
                replay = scenario.model_copy(update={"events": scenario.events[: index + 1]})
                failure = Failure(
                    index=index,
                    event=event,
                    expected=expected,
                    actual=actual,
                )
                return Trophy(scenario=replay, failure=failure)
        return None

    def run(
        self,
        seeds: Iterable[int],
        make_system: Callable[[int], TransitionSystem],
        *,
        target: int = 3,
        count: int = 100,
    ) -> list[Trophy]:
        trophies: list[Trophy] = []
        for seed in seeds:
            scenario = self.compiler.compile(seed, target=target, count=count)
            trophy = self.compare(scenario, make_system)
            if trophy:
                trophies.append(trophy)
        return trophies

    def replay(
        self,
        trophy: Trophy,
        make_system: Callable[[int], TransitionSystem],
    ) -> Trophy | None:
        return self.compare(trophy.scenario, make_system)


class TreeFuzzer:
    """Find and save the first tree state split."""

    def __init__(self, compiler: TreeScenarioCompiler | None = None) -> None:
        self.compiler = compiler or TreeScenarioCompiler()

    def compare(
        self,
        scenario: TreeScenario,
        make_system: Callable[[int, int], TreeTransitionSystem],
    ) -> TreeTrophy | None:
        oracle = ReaperTreeModel(scenario.target, scenario.depth)
        system = make_system(scenario.target, scenario.depth)
        for index, routed in enumerate(scenario.events):
            oracle.apply(routed)
            system.apply(routed)
            expected = oracle.view()
            actual = system.view()
            if actual != expected:
                replay = scenario.model_copy(update={"events": scenario.events[: index + 1]})
                failure = TreeFailure(
                    index=index,
                    routed=routed,
                    expected=expected,
                    actual=actual,
                )
                return TreeTrophy(scenario=replay, failure=failure)
        return None

    def run(
        self,
        seeds: Iterable[int],
        make_system: Callable[[int, int], TreeTransitionSystem],
        *,
        target: int = 2,
        depth: int = 2,
        count: int = 100,
    ) -> list[TreeTrophy]:
        trophies: list[TreeTrophy] = []
        for seed in seeds:
            scenario = self.compiler.compile(seed, target=target, depth=depth, count=count)
            trophy = self.compare(scenario, make_system)
            if trophy:
                trophies.append(trophy)
        return trophies

    def replay(
        self,
        trophy: TreeTrophy,
        make_system: Callable[[int, int], TreeTransitionSystem],
    ) -> TreeTrophy | None:
        return self.compare(trophy.scenario, make_system)
