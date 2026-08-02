"""Check seeded race plans and trophy replay."""

from pathlib import Path

from reaper.control import ControlEvent, ControlSlot, EventKind, ReaperCore, SkeletonID
from tests.dst import (
    Fuzzer,
    ReaperModel,
    ReaperTreeModel,
    RoutedEvent,
    ScenarioCompiler,
    TreeFuzzer,
    TreeScenarioCompiler,
    TreeTrophy,
    Trophy,
)


class FdOnlyCore(ReaperCore):
    """Model a stale FD reuse bug in the live reducer."""

    def find(self, identity: SkeletonID | None) -> ControlSlot | None:
        if identity is None:
            return None
        return self.slots.get(identity.fd)


class FdOnlyModel(ReaperModel):
    def make_core(self, target: int) -> ReaperCore:
        return FdOnlyCore(target)


class FdOnlyTree(ReaperTreeModel):
    """Model child reuse after its owner dies."""

    def owns(self, path: tuple[int, ...], identity: SkeletonID) -> bool:
        owner = self.owners.get(path)
        return owner is not None and owner.fd == identity.fd


def make_good(target: int) -> ReaperModel:
    return ReaperModel(target)


def make_bad(target: int) -> FdOnlyModel:
    return FdOnlyModel(target)


def make_good_tree(target: int, depth: int) -> ReaperTreeModel:
    return ReaperTreeModel(target, depth)


def make_bad_tree(target: int, depth: int) -> FdOnlyTree:
    return FdOnlyTree(target, depth)


def test_seed_compiles_to_one_fixed_plan() -> None:
    compiler = ScenarioCompiler()
    first = compiler.compile(42, target=3, count=200)
    second = compiler.compile(42, target=3, count=200)
    other = compiler.compile(43, target=3, count=200)
    assert first == second
    assert first != other
    assert any(event.kind == EventKind.EOF for event in first.events)
    assert len(first.events) == 200


def test_good_model_matches_all_seed_plans() -> None:
    trophies = Fuzzer().run(range(100), make_good, target=4, count=250)
    assert trophies == []


def test_fd_reuse_bug_makes_a_trophy(tmp_path: Path) -> None:
    fuzzer = Fuzzer()
    trophies = fuzzer.run(range(100), make_bad, target=2, count=100)
    assert trophies
    trophy = trophies[0]
    assert trophy.failure.event.kind in (EventKind.EOF, EventKind.RESULT)
    path = tmp_path / f"seed-{trophy.scenario.seed}.json"
    trophy.save(path)
    loaded = Trophy.load(path)
    assert loaded == trophy
    assert fuzzer.replay(loaded, make_bad) is not None
    assert fuzzer.replay(loaded, make_good) is None


def test_saved_trophies_replay_on_fixed_model() -> None:
    paths = sorted((Path(__file__).parent / "trophies").glob("fd-*.json"))
    assert paths
    for path in paths:
        trophy = Trophy.load(path)
        plan = ScenarioCompiler().compile(
            trophy.scenario.seed,
            target=trophy.scenario.target,
            count=len(trophy.scenario.events),
        )
        assert plan.events == trophy.scenario.events
        assert Fuzzer().replay(trophy, make_good) is None


def test_seed_compiles_to_nested_routes() -> None:
    compiler = TreeScenarioCompiler()
    first = compiler.compile(42, target=2, depth=3, count=200)
    second = compiler.compile(42, target=2, depth=3, count=200)
    other = compiler.compile(43, target=2, depth=3, count=200)
    assert first == second
    assert first != other
    assert any(len(routed.path) == 3 for routed in first.events)
    assert any(routed.event.kind == EventKind.EOF for routed in first.events)


def test_good_tree_matches_nested_seed_plans() -> None:
    trophies = TreeFuzzer().run(
        range(100),
        make_good_tree,
        target=2,
        depth=3,
        count=250,
    )
    assert trophies == []


def test_parent_reuse_drops_stale_child_events() -> None:
    tree = ReaperTreeModel(target=1, depth=2)
    first = SkeletonID(fd=10, gen=1)
    stale = RoutedEvent(
        path=(10,),
        owner=first,
        event=ControlEvent(kind=EventKind.SUBMIT, job="stale"),
    )
    tree.apply(
        RoutedEvent(
            path=(),
            owner=None,
            event=ControlEvent(kind=EventKind.EOF, identity=first),
        )
    )
    tree.apply(stale)
    child = tree.nodes[(10,)]
    assert tree.owners[(10,)] == SkeletonID(fd=10, gen=2)
    assert child.view().queue == ()


def test_owner_reuse_bug_makes_a_tree_trophy(tmp_path: Path) -> None:
    fuzzer = TreeFuzzer()
    trophies = fuzzer.run(
        range(100),
        make_bad_tree,
        target=1,
        depth=2,
        count=30,
    )
    assert trophies
    trophy = trophies[0]
    assert trophy.failure.routed.event.kind == EventKind.EOF
    assert trophy.failure.routed.path != ()
    path = tmp_path / f"tree-seed-{trophy.scenario.seed}.json"
    trophy.save(path)
    loaded = TreeTrophy.load(path)
    assert loaded == trophy
    assert fuzzer.replay(loaded, make_bad_tree) is not None
    assert fuzzer.replay(loaded, make_good_tree) is None


def test_saved_tree_trophies_replay_on_fixed_model() -> None:
    paths = sorted((Path(__file__).parent / "trophies").glob("tree-*.json"))
    assert paths
    for path in paths:
        trophy = TreeTrophy.load(path)
        plan = TreeScenarioCompiler().compile(
            trophy.scenario.seed,
            target=trophy.scenario.target,
            depth=trophy.scenario.depth,
            count=len(trophy.scenario.events),
        )
        assert plan.events == trophy.scenario.events
        assert TreeFuzzer().replay(trophy, make_good_tree) is None
