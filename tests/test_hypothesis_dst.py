"""Generate long control transition permutations."""

import os

from hypothesis import settings, strategies
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from reaper.control import ControlEvent, EventKind, SkeletonState
from tests.dst import ReaperModel, ReaperTreeModel, RoutedEvent
from tests.startup_model import StartupLife, StartupModel

EXAMPLES = int(os.environ.get("REAPER_FUZZ_EXAMPLES", "100"))
STEPS = int(os.environ.get("REAPER_FUZZ_STEPS", "100"))


def settle_fairly(system: ReaperModel) -> None:
    """Finish finite work under a fair scheduler."""

    if system.core.closing:
        for slot in tuple(system.slots.values()):
            system.apply(ControlEvent(kind=EventKind.EOF, identity=slot.identity))
        assert not system.slots
        assert not system.core.queue
        return
    limit = len(system.core.queue) + len(system.slots) + system.target + 1
    rounds = 0
    while system.core.queue or any(
        slot.state is not SkeletonState.IDLE for slot in system.slots.values()
    ):
        assert rounds < limit
        for slot in tuple(system.slots.values()):
            match slot.state:
                case SkeletonState.STARTING:
                    system.apply(ControlEvent(kind=EventKind.READY, identity=slot.identity))
                case SkeletonState.RUNNING:
                    system.apply(
                        ControlEvent(
                            kind=EventKind.RESULT,
                            identity=slot.identity,
                            job=slot.job,
                        )
                    )
                case _:
                    continue
        rounds += 1
    assert len(system.slots) == system.target
    assert all(slot.state is SkeletonState.IDLE for slot in system.slots.values())
    assert not system.core.queue


def settle_tree_fairly(system: ReaperTreeModel) -> None:
    """Restore a finite Reaper tree under fair replies."""

    queued = sum(len(node.core.queue) for node in system.nodes.values())
    limit = queued + len(system.nodes) * (system.target + 2) + 1
    rounds = 0
    while any(
        node.core.queue or any(slot.state is not SkeletonState.IDLE for slot in node.slots.values())
        for node in system.nodes.values()
    ):
        assert rounds < limit
        for path in tuple(sorted(system.nodes)):
            node = system.nodes.get(path)
            if not node:
                continue
            owner = system.owners[path]
            for slot in tuple(node.slots.values()):
                match slot.state:
                    case SkeletonState.STARTING:
                        event = ControlEvent(
                            kind=EventKind.READY,
                            identity=slot.identity,
                        )
                    case SkeletonState.RUNNING:
                        event = ControlEvent(
                            kind=EventKind.RESULT,
                            identity=slot.identity,
                            job=slot.job,
                        )
                    case _:
                        continue
                system.apply(RoutedEvent(path=path, owner=owner, event=event))
        rounds += 1
    expected_nodes = sum(system.target**level for level in range(system.depth + 1))
    assert len(system.nodes) == expected_nodes
    assert all(len(node.slots) == system.target for node in system.nodes.values())
    assert all(
        slot.state is SkeletonState.IDLE
        for node in system.nodes.values()
        for slot in node.slots.values()
    )


class ControlPermutationMachine(RuleBasedStateMachine):
    """Permute flat Reaper state transitions."""

    def __init__(self) -> None:
        super().__init__()
        self.system = ReaperModel(1)

    @initialize(pool_size=strategies.integers(min_value=1, max_value=12))
    def configure(self, pool_size: int) -> None:
        self.system = ReaperModel(pool_size)

    @rule(job=strategies.integers(min_value=0, max_value=40))
    def submit(self, job: int) -> None:
        self.system.apply(ControlEvent(kind=EventKind.SUBMIT, job=f"job-{job}"))

    @rule(index=strategies.integers(min_value=0, max_value=100))
    def ready(self, index: int) -> None:
        slots = [
            slot for slot in self.system.slots.values() if slot.state is SkeletonState.STARTING
        ]
        if slots:
            slot = slots[index % len(slots)]
            self.system.apply(ControlEvent(kind=EventKind.READY, identity=slot.identity))

    @rule(
        index=strategies.integers(min_value=0, max_value=100),
        ok=strategies.booleans(),
    )
    def result(self, index: int, ok: bool) -> None:
        slots = [slot for slot in self.system.slots.values() if slot.state is SkeletonState.RUNNING]
        if slots:
            slot = slots[index % len(slots)]
            self.system.apply(
                ControlEvent(
                    kind=EventKind.RESULT,
                    identity=slot.identity,
                    job=slot.job,
                    ok=ok,
                )
            )

    @rule(index=strategies.integers(min_value=0, max_value=100))
    def eof(self, index: int) -> None:
        slots = list(self.system.slots.values())
        if slots:
            slot = slots[index % len(slots)]
            self.system.apply(ControlEvent(kind=EventKind.EOF, identity=slot.identity))

    @rule(index=strategies.integers(min_value=0, max_value=100))
    def stale_eof(self, index: int) -> None:
        if self.system.retired:
            slot = self.system.retired[index % len(self.system.retired)]
            self.system.apply(ControlEvent(kind=EventKind.EOF, identity=slot.identity))

    @rule()
    def close(self) -> None:
        self.system.apply(ControlEvent(kind=EventKind.CLOSE))

    @invariant()
    def state_ranges_hold(self) -> None:
        view = self.system.view()
        assert len(view.slots) <= self.system.target
        assert len({slot.identity for slot in view.slots}) == len(view.slots)
        assert all(slot.identity.fd >= 10 for slot in view.slots)

    def teardown(self) -> None:
        settle_fairly(self.system)


class TreePermutationMachine(RuleBasedStateMachine):
    """Permute nested Reaper tree transitions."""

    def __init__(self) -> None:
        super().__init__()
        self.system = ReaperTreeModel(target=1, depth=0)
        self.next_job = 0

    @initialize(
        width=strategies.integers(min_value=1, max_value=4),
        depth=strategies.integers(min_value=0, max_value=3),
    )
    def configure(self, width: int, depth: int) -> None:
        self.system = ReaperTreeModel(target=width, depth=depth)

    @rule(
        path_index=strategies.integers(min_value=0, max_value=100),
        slot_index=strategies.integers(min_value=0, max_value=100),
        kind=strategies.sampled_from(
            (
                EventKind.SUBMIT,
                EventKind.READY,
                EventKind.RESULT,
                EventKind.EOF,
            )
        ),
    )
    def transition(self, path_index: int, slot_index: int, kind: EventKind) -> None:
        paths = sorted(self.system.nodes)
        path = paths[path_index % len(paths)]
        node = self.system.nodes[path]
        slots = list(node.slots.values())
        match kind:
            case EventKind.SUBMIT:
                event = ControlEvent(kind=kind, job=f"job-{self.next_job}")
                self.next_job += 1
            case EventKind.READY:
                ready = [slot for slot in slots if slot.state is SkeletonState.STARTING]
                if not ready:
                    return
                event = ControlEvent(kind=kind, identity=ready[slot_index % len(ready)].identity)
            case EventKind.RESULT:
                running = [slot for slot in slots if slot.state is SkeletonState.RUNNING]
                if not running:
                    return
                slot = running[slot_index % len(running)]
                event = ControlEvent(kind=kind, identity=slot.identity, job=slot.job)
            case EventKind.EOF:
                if not slots:
                    return
                event = ControlEvent(kind=kind, identity=slots[slot_index % len(slots)].identity)
            case _:
                raise AssertionError(f"unhandled generated event {kind}")
        self.system.apply(
            RoutedEvent(
                path=path,
                owner=self.system.owners[path],
                event=event,
            )
        )

    @invariant()
    def tree_ranges_hold(self) -> None:
        view = self.system.view()
        expected = sum(self.system.target**level for level in range(self.system.depth + 1))
        assert 1 <= len(view.nodes) <= expected
        assert view.nodes[0].path == ()
        assert all(len(node.reaper.slots) <= self.system.target for node in view.nodes)
        assert all(len(node.path) <= self.system.depth for node in view.nodes)

    def teardown(self) -> None:
        settle_tree_fairly(self.system)


class HordePermutationMachine(RuleBasedStateMachine):
    """Permute one Reaper with many typed pools."""

    def __init__(self) -> None:
        super().__init__()
        self.systems: list[ReaperModel] = []
        self.roles: list[str] = []
        self.topics: list[str] = []
        self.next_job = 0

    @initialize(
        specs=strategies.lists(
            strategies.tuples(
                strategies.sampled_from(("general", "task", "maintenance")),
                strategies.sampled_from(("", "math", "cat", "workflow")),
                strategies.integers(min_value=1, max_value=12),
            ),
            min_size=1,
            max_size=8,
        )
    )
    def configure(self, specs: list[tuple[str, str, int]]) -> None:
        self.roles = [role for role, _, _ in specs]
        self.topics = [topic for _, topic, _ in specs]
        self.systems = [ReaperModel(target) for _, _, target in specs]

    @rule(
        pool_index=strategies.integers(min_value=0, max_value=100),
        slot_index=strategies.integers(min_value=0, max_value=100),
        kind=strategies.sampled_from(
            (EventKind.SUBMIT, EventKind.READY, EventKind.RESULT, EventKind.EOF)
        ),
    )
    def transition(self, pool_index: int, slot_index: int, kind: EventKind) -> None:
        system = self.systems[pool_index % len(self.systems)]
        slots = list(system.slots.values())
        match kind:
            case EventKind.SUBMIT:
                if self.roles[pool_index % len(self.roles)] != "general":
                    return
                event = ControlEvent(kind=kind, job=f"job-{self.next_job}")
                self.next_job += 1
            case EventKind.READY:
                ready = [slot for slot in slots if slot.state is SkeletonState.STARTING]
                if not ready:
                    return
                event = ControlEvent(kind=kind, identity=ready[slot_index % len(ready)].identity)
            case EventKind.RESULT:
                running = [slot for slot in slots if slot.state is SkeletonState.RUNNING]
                if not running:
                    return
                slot = running[slot_index % len(running)]
                event = ControlEvent(kind=kind, identity=slot.identity, job=slot.job)
            case EventKind.EOF:
                if not slots:
                    return
                event = ControlEvent(kind=kind, identity=slots[slot_index % len(slots)].identity)
            case _:
                raise AssertionError(f"unhandled horde event {kind}")
        system.apply(event)

    @rule()
    def close(self) -> None:
        for system in self.systems:
            system.apply(ControlEvent(kind=EventKind.CLOSE))

    @invariant()
    def horde_ranges_hold(self) -> None:
        assert 1 <= len(self.systems) <= 8
        assert len(self.systems) == len(self.roles) == len(self.topics)
        assert all(len(system.slots) <= system.target for system in self.systems)

    def teardown(self) -> None:
        for system in self.systems:
            settle_fairly(system)


class StartupPermutationMachine(RuleBasedStateMachine):
    """Permute spawn, readiness, loss, and public startup return."""

    def __init__(self) -> None:
        super().__init__()
        self.system = StartupModel(target=1)
        self.next_generation = 1

    @initialize(pool_size=strategies.integers(min_value=1, max_value=12))
    def configure(self, pool_size: int) -> None:
        self.system = StartupModel(pool_size)
        self.next_generation = 1

    @rule()
    def spawn(self) -> None:
        if self.system.life is StartupLife.WAITING and len(self.system.slots) < self.system.target:
            self.system.spawn(self.next_generation)
            self.next_generation += 1

    @rule(index=strategies.integers(min_value=0, max_value=100))
    def ready(self, index: int) -> None:
        generations = tuple(self.system.slots)
        if generations:
            self.system.ready(generations[index % len(generations)])

    @rule(index=strategies.integers(min_value=0, max_value=100))
    def lose(self, index: int) -> None:
        generations = tuple(self.system.slots)
        if generations:
            self.system.lose(generations[index % len(generations)])

    @rule()
    def return_if_ready(self) -> None:
        self.system.return_if_ready()

    @invariant()
    def successful_start_has_exact_ready_capacity(self) -> None:
        if self.system.life is StartupLife.RETURNED:
            assert self.system.can_return()


ControlPermutationTest = ControlPermutationMachine.TestCase
ControlPermutationTest.settings = settings(
    max_examples=EXAMPLES,
    stateful_step_count=STEPS,
    deadline=None,
    print_blob=True,
)

TreePermutationTest = TreePermutationMachine.TestCase
TreePermutationTest.settings = settings(
    max_examples=EXAMPLES,
    stateful_step_count=STEPS,
    deadline=None,
    print_blob=True,
)

HordePermutationTest = HordePermutationMachine.TestCase
HordePermutationTest.settings = settings(
    max_examples=EXAMPLES,
    stateful_step_count=STEPS,
    deadline=None,
    print_blob=True,
)

StartupPermutationTest = StartupPermutationMachine.TestCase
StartupPermutationTest.settings = settings(
    max_examples=EXAMPLES,
    stateful_step_count=STEPS,
    deadline=None,
    print_blob=True,
)
