"""Check typed task acts."""

import asyncio
import inspect
import sys
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from importlib.machinery import ModuleSpec
from typing import Annotated, assert_type, cast

import pytest
from pydantic import BaseModel, ConfigDict, JsonValue

from reaper.models import PromiseState, ResultState
from reaper.pool import SkeletonPool
from reaper.promise import (
    MAX_CHILD_PROMISES,
    MAX_PROMISE_BYTES,
    Context,
    ContextDep,
    DurableCall,
    DurableFunction,
    Injected,
    Promise,
    ReaperClient,
    ReaperError,
    RetryableError,
    current_context,
    durable,
    function_name,
    set_default_store,
)
from reaper.promises.models import PromiseRecord, SubmitTimer
from reaper.tasks.models import SubmitCall
from tests.promise_graphs import (
    dag,
    diamond,
    dynamic_dag,
    fanout,
    first,
    gather,
    join,
    mapreduce,
)
from tests.workers import run_promise_task


class FakeStore:
    """Store each fake SQL call."""

    def __init__(self, value: object = 0) -> None:
        """Make an empty call list."""

        self.value = value
        self.calls: list[SubmitCall] = []
        self.timer_calls: list[tuple[str, float]] = []
        self.timers: dict[str, PromiseRecord] = {}
        self.retention_ms = 604_800_000

    async def submit_call(self, params: SubmitCall) -> PromiseRecord:
        """Save and echo one call."""

        self.calls.append(params)
        return done_row(params.id, self.value)

    async def submit_timer(self, params: SubmitTimer) -> PromiseRecord:
        delay = (params.due_at - datetime.now(UTC)).total_seconds()
        self.timer_calls.append((params.id, delay))
        promise = pending_row(params.id, due_at=params.due_at)
        self.timers[params.id] = promise
        return promise

    async def read_promise(self, promise_id: str) -> PromiseRecord:
        return self.timers[promise_id]


class AmbiguousCommitStore(FakeStore):
    """Commit the first root invocation but lose its response."""

    def __init__(self) -> None:
        super().__init__(7)
        self.created_ids: list[str] = []

    async def submit_call(self, params: SubmitCall) -> PromiseRecord:
        promise_id = params.id
        self.created_ids.append(promise_id)
        if len(self.created_ids) == 1:
            raise ConnectionError("response lost after commit")
        return done_row(promise_id, self.value)


def pending_row(
    promise_id: str,
    *,
    due_at: datetime | None = None,
) -> PromiseRecord:
    now = datetime.now(UTC)
    expires_at = now + timedelta(days=365) if due_at is None else None
    return PromiseRecord(
        id=promise_id,
        idempotency_key="0" * 64,
        state=PromiseState.PENDING,
        root_id=None,
        result=None,
        error=None,
        due_at=due_at,
        expires_at=expires_at,
        delete_after=(expires_at or due_at or now) + timedelta(days=7),
        settled_at=None,
    )


def done_row(promise_id: str, value: object) -> PromiseRecord:
    """Make one resolved promise row."""

    now = datetime.now(UTC)
    return PromiseRecord(
        id=promise_id,
        idempotency_key="0" * 64,
        state=PromiseState.RESOLVED,
        root_id=None,
        result=cast(JsonValue, value),
        error=None,
        due_at=None,
        expires_at=now + timedelta(days=365),
        delete_after=now + timedelta(days=372),
        settled_at=now,
    )


class PoolStore:
    """Run durable calls in warm SkeletonPool slots."""

    def __init__(self, pool: SkeletonPool) -> None:
        self.pool = pool
        self.calls: list[tuple[str, Mapping[str, object]]] = []
        self.retention_ms = 604_800_000

    async def submit_call(self, params: SubmitCall) -> PromiseRecord:
        args = cast(Mapping[str, object], params.input)
        self.calls.append((params.function, args))
        value = await self.pool.run_async(run_promise_task, params.function, args)
        return done_row(params.id, value)

    async def read_promise(self, promise_id: str) -> PromiseRecord:
        raise RuntimeError(f"promise {promise_id!r} was not retained")

    async def submit_timer(self, params: SubmitTimer) -> PromiseRecord:
        raise RuntimeError(f"timer {params.id!r} has no route")


class BuiltinModel(BaseModel):
    """Hold nested typed wire data."""

    model_config = ConfigDict(frozen=True, strict=True)

    name: str
    scores: tuple[int, ...]
    flags: frozenset[str]


@durable(execution_timeout=1.0, topic="math")
async def add(
    left: int,
    ctx: ContextDep,
    right: int = 1,
) -> int:
    """Add two strict ints."""

    assert ctx.task_id == "task"
    return left + right


@durable(execution_timeout=1.0)
async def wrong_value(value: int) -> int:
    """Give a bad result type."""

    return cast(int, str(value))


@durable(execution_timeout=1.0)
async def retry_task(value: int) -> int:
    """Ask for one more run."""

    raise RetryableError(str(value))


@durable(execution_timeout=1.0)
async def reject_task(value: int) -> int:
    """Raise a final task fault."""

    raise ValueError(str(value))


@durable(execution_timeout=0.001)
async def thirty_day_task(value: int) -> int:
    await asyncio.sleep(timedelta(days=30).total_seconds())
    return value


@durable(execution_timeout=1.0)
async def durable_thirty_day_wait() -> str:
    await durable.sleep(timedelta(days=30))
    return "awake"


@durable(execution_timeout=1.0)
async def sum_all(base: int, *items: int, **named: int) -> int:
    """Sum all arg forms."""

    return base + sum(items) + sum(named.values())


@durable(execution_timeout=1.0)
async def builtin_none(value: None) -> None:
    return value


@durable(execution_timeout=1.0)
async def builtin_bool(value: bool) -> bool:
    return value


@durable(execution_timeout=1.0)
async def builtin_int(value: int) -> int:
    return value


@durable(execution_timeout=1.0)
async def builtin_float(value: float) -> float:
    return value


@durable(execution_timeout=1.0)
async def builtin_complex(value: complex) -> complex:
    return value


@durable(execution_timeout=1.0)
async def builtin_str(value: str) -> str:
    return value


@durable(execution_timeout=1.0)
async def builtin_bytes(value: bytes) -> bytes:
    return value


@durable(execution_timeout=1.0)
async def builtin_list(value: list[int]) -> list[int]:
    return value


@durable(execution_timeout=1.0)
async def builtin_dict(value: dict[str, int]) -> dict[str, int]:
    return value


@durable(execution_timeout=1.0)
async def builtin_fixed_tuple(value: tuple[int, str, bool]) -> tuple[int, str, bool]:
    return value


@durable(execution_timeout=1.0)
async def builtin_tuple(value: tuple[int, ...]) -> tuple[int, ...]:
    return value


@durable(execution_timeout=1.0)
async def builtin_set(value: set[int]) -> set[int]:
    return value


@durable(execution_timeout=1.0)
async def builtin_frozenset(value: frozenset[int]) -> frozenset[int]:
    return value


@durable(execution_timeout=1.0)
async def builtin_union(value: int | str) -> int | str:
    return value


@durable(execution_timeout=1.0)
async def builtin_model(value: BuiltinModel) -> BuiltinModel:
    return value


@durable(execution_timeout=1.0)
async def wrong_tuple() -> tuple[int, ...]:
    return cast(tuple[int, ...], [1, 2])


@durable(execution_timeout=1.0)
async def resource_echo(value: str) -> str:
    return value


@durable(execution_timeout=1.0)
async def resource_large_result(size: int) -> str:
    return "x" * size


async def round_trip[ValueT](
    task: DurableFunction[..., ValueT],
    payload: Mapping[str, object],
    context: Context,
) -> ValueT:
    """Run through task and wire gates."""

    result = await task.execute(payload, context)
    assert result.state is ResultState.RESOLVED
    promise: Promise[ValueT] = Promise(
        stored=done_row(task.name, result.value),
        value_adapter=task.value_adapter,
    )
    return promise.result()


def test_call_returns_a_typed_value() -> None:
    """Check the direct root call."""

    async def check() -> None:
        """Run this async check."""

        fake = FakeStore(5)
        token = set_default_store(fake)
        result = await add(2, right=3)
        assert result == 5
        assert fake.calls[0].input == {"left": 2, "right": 3}
        assert fake.calls[0].execution_timeout_ms == 1_000
        from reaper.promise import default_store

        default_store.reset(token)

    asyncio.run(check())


def test_root_retry_after_an_ambiguous_commit_reuses_the_execution_id() -> None:
    """A lost create response must not turn one logical submission into two workflows."""

    async def check() -> None:
        fake = AmbiguousCommitStore()
        token = set_default_store(fake)
        try:
            with pytest.raises(ConnectionError, match="response lost after commit"):
                await add(2, right=5).result(id="http-request-one")
            assert await add(2, right=5).result(id="http-request-one") == 7
        finally:
            from reaper.promise import default_store

            default_store.reset(token)
        assert len(fake.created_ids) == 2
        assert fake.created_ids[0] == fake.created_ids[1]

    asyncio.run(check())


def test_args_and_values_are_strict() -> None:
    """Check all strict type gates."""

    async def check() -> None:
        """Run this async check."""

        fake = FakeStore()
        context = Context(store=fake, task_id="task")
        bad_arg = await add.execute({"left": "2", "right": 3}, context)
        bad_value = await wrong_value.execute({"value": 2}, context)
        assert bad_arg.state is ResultState.REJECTED
        assert bad_arg.error is not None
        assert "ValidationError" in bad_arg.error.type
        assert bad_value.state is ResultState.REJECTED

    asyncio.run(check())


def test_retry_and_reject_stay_split() -> None:
    """Check retry and fail paths."""

    async def check() -> None:
        """Run this async check."""

        fake = FakeStore()
        context = Context(store=fake, task_id="task")
        retry = await retry_task.execute({"value": 7}, context)
        reject = await reject_task.execute({"value": 8}, context)
        assert retry.state is ResultState.RETRY
        assert retry.error is not None
        assert reject.state is ResultState.REJECTED

    asyncio.run(check())


def test_timedelta_sleep_obeys_local_execution_timeout() -> None:
    """Cancel a thirty day local sleep."""

    async def check() -> None:
        context = Context(store=FakeStore(), task_id="timed")
        result = await thirty_day_task.execute({"value": 7}, context)
        assert result.state is ResultState.RETRY
        assert result.error is not None
        assert result.error.type == "builtins.TimeoutError"

    asyncio.run(check())


def test_durable_timedelta_sleep_suspends_and_replays() -> None:
    """Replay one thirty day SQL timer."""

    async def check() -> None:
        fake = FakeStore()
        task_id = "thirty-day"
        timer_id = f"{task_id}:1:durable.sleep"
        first = Context(store=fake, task_id=task_id)
        suspended = await durable_thirty_day_wait.execute({}, first)
        assert suspended.state is ResultState.SUSPENDED
        assert suspended.awaited == (timer_id,)
        assert fake.timer_calls[0][0] == timer_id
        assert fake.timer_calls[0][1] == pytest.approx(timedelta(days=30).total_seconds(), abs=0.1)

        replay = Context(
            store=fake,
            task_id=task_id,
            preload=(done_row(timer_id, None),),
        )
        resolved = await durable_thirty_day_wait.execute({}, replay)
        assert resolved.state is ResultState.RESOLVED
        assert resolved.value == "awake"
        assert len(fake.timer_calls) == 1

    asyncio.run(check())


def test_preload_skips_a_sql_call() -> None:
    """Check one call preload hit."""

    async def check() -> None:
        """Run this async check."""

        fake = FakeStore()
        promise_id = f"task:1:{add.name}"
        context = Context(
            store=fake,
            task_id="task",
            preload=(done_row(promise_id, 5),),
        )
        token = current_context.set(context)
        result = await add(2, 3)
        current_context.reset(token)
        assert result == 5
        assert fake.calls == []

    asyncio.run(check())


def test_context_marker_is_not_a_field() -> None:
    """Check ctx stays off wire."""

    assert set(add.params_model.model_fields) == {"left", "right"}
    assert tuple(inspect.signature(add).parameters) == ("left", "right")


def test_decorator_preserves_the_public_static_return_type() -> None:
    """Keep IDE call hints and the durable result parameter."""

    call = assert_type(resource_echo("hello"), DurableCall[str])
    assert isinstance(call, DurableCall)
    assert "self" not in inspect.signature(durable).parameters


def test_client_loads_environment_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the getting-started client constructor concise and typed."""

    monkeypatch.setenv("REAPER_POSTGRES_DSN", "postgresql://user:pass@localhost/example")
    client = ReaperClient.from_environment()
    assert str(client.postgres_dsn) == "postgresql://user:pass@localhost/example"


def test_module_entrypoint_function_name_is_importable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonicalize `python -m` definitions for remote skeleton imports."""

    async def entrypoint(value: int) -> int:
        return value

    entrypoint.__module__ = "__main__"
    entrypoint.__qualname__ = "entrypoint"
    monkeypatch.setattr(sys.modules["__main__"], "__spec__", ModuleSpec("app.jobs", loader=None))
    assert function_name(entrypoint) == "app.jobs.entrypoint"


def test_star_args_and_kw_args() -> None:
    """Check typed star arg forms."""

    async def check() -> None:
        """Run this async check."""

        fake = FakeStore(10)
        token = set_default_store(fake)
        result = await sum_all(1, 2, 3, four=4)
        from reaper.promise import default_store

        default_store.reset(token)
        assert result == 10
        assert fake.calls[0].input == {
            "base": 1,
            "items": [2, 3],
            "named": {"four": 4},
        }
        context = Context(store=fake, task_id="task")
        executed = await sum_all.execute(
            {"base": 1, "items": (2, 3), "named": {"four": 4}},
            context,
        )
        assert executed.value == 10
        bad = await sum_all.execute(
            {"base": 1, "items": ("2",), "named": {}},
            context,
        )
        assert bad.state is ResultState.REJECTED

    asyncio.run(check())


def test_promise_read_guards() -> None:
    """Check bad promise value data."""

    pending = add.value_adapter
    open_promise: Promise[int] = Promise(
        stored=pending_row("p"),
        value_adapter=pending,
    )
    empty_promise: Promise[int] = Promise(
        stored=done_row("p", None),
        value_adapter=pending,
    )
    with pytest.raises(ReaperError):
        open_promise.result()
    with pytest.raises(ReaperError):
        empty_promise.result()


def test_decorator_type_guards() -> None:
    """Check bad task forms fail."""

    def sync(value: int) -> int:
        """Give this int back."""

        return value

    async def bad_ctx(value: Annotated[int, Injected()]) -> int:
        """Use a bad ctx type."""

        return value

    async def bound(self: object, value: int) -> int:
        return value

    async def nested(value: int) -> int:
        return value

    scope: dict[str, object] = {}
    exec("async def no_arg_type(value) -> int:\n return value", scope)
    exec("async def no_end_type(value: int):\n return value", scope)
    no_arg_type = cast(Callable[..., Awaitable[int]], scope["no_arg_type"])
    no_end_type = cast(Callable[..., Awaitable[int]], scope["no_end_type"])
    with pytest.raises(TypeError):
        durable(execution_timeout=1.0)(cast(Callable[..., Awaitable[int]], sync))
    with pytest.raises(ValueError):
        durable(execution_timeout=0.0)(add.fn)
    with pytest.raises(TypeError):
        durable(execution_timeout=1)(add.fn)
    with pytest.raises(ValueError):
        durable(execution_timeout=1.0, promise_duration=0.0)(add.fn)
    with pytest.raises(TypeError):
        durable(execution_timeout=1.0, promise_duration=10)(add.fn)
    with pytest.raises(ValueError):
        durable(
            execution_timeout=1.0,
            promise_duration=365 * 24 * 60 * 60.0 + 1,
        )(add.fn)
    with pytest.raises(ValueError):
        durable(execution_timeout=1.0, max_attempts=101)(add.fn)
    with pytest.raises(TypeError):
        durable(execution_timeout=1.0)(no_arg_type)
    with pytest.raises(TypeError):
        durable(execution_timeout=1.0)(no_end_type)
    with pytest.raises(TypeError):
        durable(execution_timeout=1.0)(bad_ctx)
    with pytest.raises(TypeError, match="bound method"):
        durable(execution_timeout=1.0)(bound)
    with pytest.raises(TypeError, match="module-level"):
        durable(execution_timeout=1.0)(nested)


def test_promise_resource_limits_are_enforced() -> None:
    async def check() -> None:
        fake = FakeStore()
        token = set_default_store(fake)
        oversized = await asyncio.gather(
            resource_echo("x" * (MAX_PROMISE_BYTES + 1)),
            return_exceptions=True,
        )
        assert isinstance(oversized[0], ValueError)
        from reaper.promise import default_store

        default_store.reset(token)

        context = Context(store=fake, task_id="root")
        result = await resource_large_result.execute(
            {"size": MAX_PROMISE_BYTES + 1},
            context,
        )
        assert result.state is ResultState.REJECTED
        assert result.error
        assert result.error.type == "reaper.promise.ResourceLimitError"

        for _ in range(MAX_CHILD_PROMISES):
            context.next_id("child")
        with pytest.raises(RuntimeError, match="too many child"):
            context.next_id("child")

    asyncio.run(check())


def test_builtin_result_types_cross_the_wire() -> None:
    async def check() -> None:
        context = Context(store=FakeStore(), task_id="types")
        model = BuiltinModel(
            name="Ada",
            scores=(3, 5, 8),
            flags=frozenset({"new", "safe"}),
        )
        await round_trip(builtin_none, {"value": None}, context)
        assert await round_trip(builtin_bool, {"value": True}, context) is True
        assert await round_trip(builtin_int, {"value": 7}, context) == 7
        assert await round_trip(builtin_float, {"value": 1.25}, context) == 1.25
        assert await round_trip(builtin_complex, {"value": 2 + 3j}, context) == 2 + 3j
        assert await round_trip(builtin_str, {"value": "hello"}, context) == "hello"
        assert await round_trip(builtin_bytes, {"value": b"\x00\xff"}, context) == b"\x00\xff"
        assert await round_trip(builtin_list, {"value": [1, 2]}, context) == [1, 2]
        assert await round_trip(builtin_dict, {"value": {"one": 1}}, context) == {"one": 1}
        assert await round_trip(
            builtin_fixed_tuple,
            {"value": (1, "two", True)},
            context,
        ) == (1, "two", True)
        assert await round_trip(builtin_tuple, {"value": ()}, context) == ()
        assert await round_trip(builtin_tuple, {"value": (1, 2, 3)}, context) == (1, 2, 3)
        assert await round_trip(builtin_set, {"value": {1, 2}}, context) == {1, 2}
        frozen = await round_trip(
            builtin_frozenset,
            {"value": frozenset({1, 2})},
            context,
        )
        assert frozen == frozenset({1, 2})
        assert await round_trip(builtin_union, {"value": 4}, context) == 4
        assert await round_trip(builtin_union, {"value": "four"}, context) == "four"
        assert await round_trip(builtin_model, {"value": model}, context) == model

    asyncio.run(check())


def test_builtin_results_stay_strict() -> None:
    async def check() -> None:
        context = Context(store=FakeStore(), task_id="types")
        bad_int = await builtin_int.execute({"value": "7"}, context)
        bad_tuple = await wrong_tuple.execute({}, context)
        assert bad_int.state is ResultState.REJECTED
        assert bad_tuple.state is ResultState.REJECTED

    asyncio.run(check())


def test_diamond_promises_run_on_reaper() -> None:
    async def check() -> None:
        async with SkeletonPool(4) as pool:
            link = PoolStore(pool)
            await diamond(link)

    asyncio.run(check())


def test_gather_promises_run_on_reaper() -> None:
    async def check() -> None:
        async with SkeletonPool(4) as pool:
            link = PoolStore(pool)
            await gather(link)

    asyncio.run(check())


def test_fanout_promises_run_on_reaper() -> None:
    async def check() -> None:
        async with SkeletonPool(4) as pool:
            link = PoolStore(pool)
            await fanout(link)

    asyncio.run(check())


def test_join_promises_run_on_reaper() -> None:
    async def check() -> None:
        async with SkeletonPool(3) as pool:
            link = PoolStore(pool)
            await join(link)

    asyncio.run(check())


def test_first_promise_runs_on_reaper() -> None:
    async def check() -> None:
        async with SkeletonPool(3) as pool:
            link = PoolStore(pool)
            await first(link)

    asyncio.run(check())


def test_mapreduce_promises_run_on_reaper() -> None:
    async def check() -> None:
        async with SkeletonPool(4) as pool:
            link = PoolStore(pool)
            await mapreduce(link)

    asyncio.run(check())


def test_dag_promises_run_on_reaper() -> None:
    async def check() -> None:
        async with SkeletonPool(4) as pool:
            link = PoolStore(pool)
            await dag(link)

    asyncio.run(check())


def test_dynamic_dag_promises_run_on_reaper() -> None:
    async def check() -> None:
        async with SkeletonPool(4) as pool:
            link = PoolStore(pool)
            await dynamic_dag(link)

    asyncio.run(check())
