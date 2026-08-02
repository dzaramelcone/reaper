"""Typed durable functions backed by Reaper's PostgreSQL store."""

import asyncio
import contextvars
import functools
import inspect
import json
import logging
import math
import sys
import types
import uuid
from collections.abc import Awaitable, Callable, Generator, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import (
    Annotated,
    Protocol,
    Self,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    PostgresDsn,
    SkipValidation,
    TypeAdapter,
    ValidationError,
    create_model,
)

from reaper.api import Reaper
from reaper.database import JSON_ADAPTER, task_channel
from reaper.log import write
from reaper.maintenance.models import DeleteExpired
from reaper.models import DEFAULT_TOPIC, PromiseState, ResultState
from reaper.postgres import PostgresListener, PostgresPool
from reaper.promises.models import PromiseRecord, SubmitTimer
from reaper.settings import DEFAULT_RETENTION_MS, ReaperSettings
from reaper.tasks import TaskExecution
from reaper.tasks.models import SubmitCall
from reaper.waits.models import WaitedPromise

log = logging.getLogger(__name__)

MAX_PROMISE_BYTES = 1024 * 1024
MAX_CHILD_PROMISES = 1024
MAX_PROMISE_DEPTH = 256
MAX_PROMISE_DURATION = 365 * 24 * 60 * 60.0
MAX_ATTEMPTS = 100


class Injected:
    """Mark data added by the task host."""


class Error(BaseModel):
    """Hold safe fault data."""

    model_config = ConfigDict(frozen=True, strict=True)

    type: str
    text: str

    @classmethod
    def from_exception(cls, error: BaseException) -> Self:
        return cls(
            type=f"{type(error).__module__}.{type(error).__qualname__}",
            text=str(error),
        )


class Result(BaseModel):
    """Hold one task result."""

    model_config = ConfigDict(arbitrary_types_allowed=True, strict=True)

    state: ResultState
    value: JsonValue = None
    error: Error | None = None
    awaited: tuple[str, ...] = ()


class RetryableError(Exception):
    """Ask the pool to retry."""


class ResourceLimitError(RuntimeError):
    """Reject durable data that exceeds a configured storage bound."""


class SuspendTask(Exception):
    """Pause until child promises settle."""

    def __init__(self, awaited: Iterable[str]) -> None:
        self.awaited = tuple(dict.fromkeys(awaited))
        super().__init__(", ".join(self.awaited))


class ReaperError(RuntimeError):
    """Show a durable-store operation fault."""


class PromiseStore(Protocol):
    """Native durable-store operations used by task composition."""

    async def submit_call(self, params: SubmitCall) -> PromiseRecord: ...
    async def read_promise(self, promise_id: str) -> PromiseRecord: ...
    async def submit_timer(self, params: SubmitTimer) -> PromiseRecord: ...

    @property
    def retention_ms(self) -> int: ...


default_store: contextvars.ContextVar[PromiseStore | None] = contextvars.ContextVar(
    "reaper.promise.store",
    default=None,
)


def set_default_store(
    store: PromiseStore | None,
) -> contextvars.Token[PromiseStore | None]:
    return default_store.set(store)


class ReaperClient(BaseModel):
    """Bind durable functions to the native Reaper SQL domains."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    postgres_dsn: PostgresDsn | None = None
    retention_ms: Annotated[int, Field(ge=60_000)] = DEFAULT_RETENTION_MS
    database: Annotated[PostgresPool | None, Field(exclude=True)] = None
    store: Annotated[Reaper | None, Field(exclude=True)] = None
    token: Annotated[
        contextvars.Token[PromiseStore | None] | None,
        Field(exclude=True),
    ] = None

    @classmethod
    def from_settings(cls, settings: ReaperSettings) -> Self:
        """Build a client from typed settings."""

        return cls(postgres_dsn=settings.postgres_dsn, retention_ms=settings.retention_ms)

    @classmethod
    def from_environment(cls) -> Self:
        """Build a client from `REAPER_` environment settings."""

        return cls.from_settings(ReaperSettings())

    async def __aenter__(self) -> Self:
        if self.database is not None:
            return self
        if self.postgres_dsn is None:
            raise RuntimeError("set a Postgres DSN first")
        self.database = PostgresPool(self.postgres_dsn)
        await self.database.connect()
        pool = self.database.get_pool()
        self.store = Reaper(pool)
        self.token = set_default_store(self)
        write(log, logging.DEBUG, "promise link opened")
        return self

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        value: BaseException | None,
        trace: types.TracebackType | None,
    ) -> None:
        try:
            if self.database is not None:
                await self.database.close()
        finally:
            try:
                if self.token is not None:
                    default_store.reset(self.token)
                    self.token = None
            finally:
                self.database = None
                self.store = None
                write(log, logging.DEBUG, "promise link closed")

    def get_store(self) -> Reaper:
        if self.store is None:
            raise RuntimeError("open the Reaper client first")
        return self.store

    async def submit_call(self, params: SubmitCall) -> PromiseRecord:
        """Submit a validated durable call."""

        return await self.get_store().tasks.submit(params)

    async def listen(
        self,
        address: str,
        *,
        recycle_rate: float,
        probe_rate: float,
    ) -> PostgresListener:
        """Open one dedicated listener for a task topic."""

        if self.postgres_dsn is None:
            raise RuntimeError("a listener needs a Postgres DSN")
        if not address:
            raise ValueError("a listener needs a task topic")
        channel = task_channel(address)
        listener = PostgresListener(
            self.postgres_dsn,
            channel,
            recycle_rate=recycle_rate,
            probe_rate=probe_rate,
        )
        await listener.connect()
        return listener

    async def read_promise(self, promise_id: str) -> PromiseRecord:
        """Load one promise row."""

        promise = await self.get_store().promises.get(promise_id)
        if promise is None:
            raise ReaperError(f"promise {promise_id!r} does not exist")
        return promise

    async def process_due(self) -> None:
        """Advance due timers and expired calls."""

        await self.get_store().maintenance.process_due()

    async def gc(self, limit: int = 10_000) -> int:
        """Delete one bounded batch of retained terminal promises."""

        removed = await self.get_store().maintenance.delete_expired(DeleteExpired(limit=limit))
        return removed.roots

    async def submit_timer(self, params: SubmitTimer) -> PromiseRecord:
        """Submit a validated durable timer."""

        return await self.get_store().promises.timer(params)


class Context(BaseModel):
    """Hold data for one task."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    store: Annotated[PromiseStore, SkipValidation(), Field(exclude=True)]
    execution: Annotated[TaskExecution | None, Field(exclude=True)] = None
    task_id: Annotated[str, Field(min_length=1)]
    preload: tuple[PromiseRecord | WaitedPromise, ...] = ()
    memo: dict[str, PromiseRecord | WaitedPromise] = Field(default_factory=dict)
    call_index: int = 0
    depth: Annotated[int, Field(ge=0, le=MAX_PROMISE_DEPTH)] = 0

    def model_post_init(self, context: object) -> None:
        for promise in self.preload:
            self.memo[promise.id] = promise

    def next_id(self, func: str) -> str:
        self.call_index += 1
        assert self.call_index > 0
        if self.call_index > MAX_CHILD_PROMISES:
            raise RuntimeError("task created too many child promises")
        promise_id = f"{self.task_id}:{self.call_index}:{func}"
        assert promise_id
        return promise_id

    def find(self, promise_id: str) -> PromiseRecord | WaitedPromise | None:
        return self.memo.get(promise_id)

    def save(self, promise: PromiseRecord | WaitedPromise) -> None:
        self.memo[promise.id] = promise


ContextDep = Annotated[Context, Injected()]


current_context: contextvars.ContextVar[Context | None] = contextvars.ContextVar(
    "reaper.promise.context",
    default=None,
)


class Promise[ValueT](BaseModel):
    """Hold a typed promise row."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    stored: PromiseRecord | WaitedPromise
    value_adapter: TypeAdapter[ValueT]
    reader: Annotated[PromiseStore | None, SkipValidation(), Field(exclude=True)] = None

    def id(self) -> str:
        return self.stored.id

    def state(self) -> PromiseState:
        return self.stored.state

    def settled_order(self) -> int:
        if isinstance(self.stored, WaitedPromise):
            return self.stored.settled_at_ms or 2**63
        if self.stored.settled_at is None:
            return 2**63
        return round(self.stored.settled_at.timestamp() * 1_000)

    def result(self) -> ValueT:
        """Decode and check the done value."""

        if self.state() is not PromiseState.RESOLVED:
            raise ReaperError(f"promise {self.id()} is {self.state()}")
        encoded = JSON_ADAPTER.dump_json(self.stored.result)
        if len(encoded) > MAX_PROMISE_BYTES:
            raise ReaperError("promise result is too large")
        try:
            return self.value_adapter.validate_json(encoded, strict=True)
        except ValidationError as error:
            raise ReaperError(f"promise {self.id()} contains an invalid result") from error


class DurableCall[ValueT]:
    """Hold one lazy durable call."""

    def __init__(
        self,
        task: DurableFunction[..., ValueT],
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> None:
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.root_id = uuid.uuid4().hex

    def __await__(self) -> Generator[object, None, ValueT]:
        return self.result().__await__()

    async def submit(self, *, id: str = "") -> Promise[ValueT]:
        """Create this root call, optionally under a durable idempotency ID."""

        return await self.task.invoke(
            self.args,
            self.kwargs,
            root_id=id or self.root_id,
        )

    async def result(self, *, id: str = "", poll_rate: float = 0.25) -> ValueT:
        """Resolve this root or child call."""

        if poll_rate <= 0:
            raise ValueError("result poll rate must be more than zero")
        promise = await self.submit(id=id)
        if current_context.get() is not None:
            if promise.state() is PromiseState.PENDING:
                raise SuspendTask((promise.id(),))
            return promise.result()
        while promise.state() is PromiseState.PENDING:
            if not promise.reader:
                raise RuntimeError("this durable call has no result reader")
            await asyncio.sleep(poll_rate)
            stored = await promise.reader.read_promise(promise.id())
            promise = Promise(
                stored=stored,
                value_adapter=promise.value_adapter,
                reader=promise.reader,
            )
        return promise.result()


class DurableFunction[**ParamsT, ValueT]:
    """Make and run one task type."""

    def __init__(
        self,
        fn: Callable[ParamsT, Awaitable[ValueT]],
        *,
        execution_timeout: float,
        promise_duration: float,
        topic: str,
        version: int,
        max_attempts: int,
    ) -> None:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("durable needs an async def")
        if not isinstance(execution_timeout, float):
            raise TypeError("execution_timeout must be a float")
        if execution_timeout <= 0:
            raise ValueError("execution_timeout must be more than zero")
        if not isinstance(promise_duration, float):
            raise TypeError("promise_duration must be a float")
        if promise_duration <= 0:
            raise ValueError("promise_duration must be more than zero")
        if promise_duration > MAX_PROMISE_DURATION:
            raise ValueError("promise_duration cannot exceed one year")
        if not isinstance(max_attempts, int):
            raise TypeError("max_attempts must be an int")
        if max_attempts <= 0 or max_attempts > MAX_ATTEMPTS:
            raise ValueError("max_attempts must be from 1 through 100")
        self.fn: Callable[..., Awaitable[ValueT]] = fn
        self.execution_timeout = execution_timeout
        self.promise_duration = promise_duration
        self.topic = topic
        self.version = version
        self.max_attempts = max_attempts
        self.name = function_name(fn)
        self.signature = inspect.signature(fn)
        self.hints = get_type_hints(fn, include_extras=True)
        self.context_arg = find_context_arg(self.signature, self.hints)
        self.public_signature = public_signature(self.signature, self.context_arg)
        self.__signature__ = self.public_signature
        first = next(iter(self.signature.parameters.values()), None)
        if first is not None and first.name in ("self", "cls"):
            raise TypeError("durable requires an importable function, not a bound method")
        if "." in fn.__qualname__:
            raise TypeError("durable requires a module-level importable function")
        self.params_model = make_params_model(
            fn,
            self.signature,
            self.hints,
            self.context_arg,
        )
        result_type = self.hints.get("return")
        if result_type is None:
            raise TypeError(f"{self.name} needs a return type")
        self.value_adapter: TypeAdapter[ValueT] = make_value_adapter(result_type)
        functools.update_wrapper(self, fn)

    @overload
    def __call__(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> DurableCall[ValueT]: ...

    @overload
    def __call__(self, *args: object, **kwargs: object) -> DurableCall[ValueT]: ...

    def __call__(self, *args: object, **kwargs: object) -> DurableCall[ValueT]:
        return DurableCall(self, args, kwargs)

    async def invoke(
        self,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
        *,
        root_id: str = "",
    ) -> Promise[ValueT]:
        """Reuse task memo data before SQL."""

        bound = self.public_signature.bind(*args, **kwargs)
        bound.apply_defaults()
        model = self.params_model.model_validate(dict(bound.arguments), strict=True)
        params = cast(dict[str, object], model.model_dump(mode="json"))
        param_bytes = len(json.dumps(params, separators=(",", ":")).encode())
        if param_bytes > MAX_PROMISE_BYTES:
            raise ValueError("promise input is too large")
        context = current_context.get()
        depth = context.depth + 1 if context else 0
        if depth > MAX_PROMISE_DEPTH:
            raise RuntimeError("promise ancestry is too deep")
        store = context.store if context else default_store.get()
        if store is None:
            raise RuntimeError("set a Reaper link first")
        if context:
            if root_id:
                root_id = ""
            promise_id = context.next_id(self.name)
        else:
            promise_id = root_id or uuid.uuid4().hex
        cached = context.find(promise_id) if context else None
        stored = cached
        if stored is None:
            write(
                log,
                logging.INFO,
                "promise created",
                id=promise_id,
                function=self.name,
                topic=self.topic,
            )
            if context is not None:
                execution = context.execution
                if execution is None:
                    raise RuntimeError("child calls require an active task execution")
                parent = execution.task
                root = parent.root_id or parent.id
                now = datetime.now(UTC)
                child = execution.submit(
                    SubmitCall(
                        id=promise_id,
                        root_id=root,
                        function=self.name,
                        input=cast(JsonValue, params),
                        topic=self.topic,
                        version=self.version,
                        depth=depth,
                        execution_timeout_ms=round(self.execution_timeout * 1_000),
                        expires_at=now + timedelta(seconds=self.promise_duration),
                        retention_ms=store.retention_ms,
                        max_failures=self.max_attempts,
                    )
                )
                item = await child
            else:
                now = datetime.now(UTC)
                item = await store.submit_call(
                    SubmitCall(
                        id=promise_id,
                        function=self.name,
                        input=cast(JsonValue, params),
                        topic=self.topic,
                        version=self.version,
                        depth=depth,
                        execution_timeout_ms=round(self.execution_timeout * 1_000),
                        expires_at=now + timedelta(seconds=self.promise_duration),
                        retention_ms=store.retention_ms,
                        max_failures=self.max_attempts,
                    )
                )
            stored = item
        else:
            write(
                log,
                logging.DEBUG,
                "promise replayed",
                id=promise_id,
                function=self.name,
            )
        if context:
            context.save(stored)
        assert stored.id == promise_id
        return Promise(
            stored=stored,
            value_adapter=self.value_adapter,
            reader=store,
        )

    async def call(
        self,
        payload: Mapping[str, object],
        context: Context,
        wire: bool = False,
    ) -> tuple[dict[str, object], ValueT]:
        if wire:
            model = self.params_model.model_validate_json(
                json.dumps(payload, separators=(",", ":")),
                strict=True,
            )
        else:
            model = self.params_model.model_validate(payload, strict=True)
        values = cast(dict[str, object], model.model_dump())
        call_values = {name: getattr(model, name) for name in self.params_model.model_fields}
        call_args: list[object] = []
        call_kwargs: dict[str, object] = {}
        for name, parameter in self.signature.parameters.items():
            value = context if name == self.context_arg else call_values[name]
            if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
                call_args.extend(cast(tuple[object, ...], value))
            elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
                call_kwargs.update(cast(Mapping[str, object], value))
            elif parameter.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                call_args.append(value)
            else:
                call_kwargs[name] = value
        value = await self.fn(*call_args, **call_kwargs)
        checked = self.value_adapter.validate_python(value, strict=True)
        return values, checked

    async def execute(
        self,
        payload: Mapping[str, object],
        context: Context,
        *,
        wire: bool = False,
    ) -> Result:
        """Map task faults to retry or reject."""

        token = current_context.set(context)
        try:
            try:
                item: tuple[dict[str, object], ValueT] | BaseException = await self.call_once(
                    payload, context, wire
                )
            except BaseException as caught:
                item = caught
        finally:
            current_context.reset(token)
        if isinstance(item, TimeoutError):
            timeout = TimeoutError("local task run timed out")
            write(
                log,
                logging.INFO,
                "promise retry scheduled",
                id=context.task_id,
                fault=type(timeout).__qualname__,
            )
            return Result(state=ResultState.RETRY, error=Error.from_exception(timeout))
        if isinstance(item, asyncio.CancelledError):
            raise item
        if isinstance(item, RetryableError):
            return Result(state=ResultState.RETRY, error=Error.from_exception(item))
        if isinstance(item, SuspendTask):
            write(
                log,
                logging.DEBUG,
                "promise suspended",
                id=context.task_id,
                awaited=item.awaited,
            )
            return Result(
                state=ResultState.SUSPENDED,
                awaited=item.awaited,
            )
        if isinstance(item, BaseException):
            write(
                log,
                logging.WARNING,
                "promise rejected",
                id=context.task_id,
                fault=type(item).__qualname__,
            )
            return Result(state=ResultState.REJECTED, error=Error.from_exception(item))
        _values, value = item
        write(log, logging.DEBUG, "promise resolved", id=context.task_id)
        encoded_value = self.value_adapter.dump_json(value)
        if len(encoded_value) > MAX_PROMISE_BYTES:
            error = Error.from_exception(ResourceLimitError("promise result is too large"))
            return Result(
                state=ResultState.REJECTED,
                error=error,
            )
        return Result(
            state=ResultState.RESOLVED,
            value=JSON_ADAPTER.validate_json(encoded_value, strict=True),
        )

    async def call_once(
        self,
        payload: Mapping[str, object],
        context: Context,
        wire: bool,
    ) -> tuple[dict[str, object], ValueT]:
        """Bound one local execution attempt."""

        async with asyncio.timeout(self.execution_timeout):
            return await self.call(payload, context, wire)


class Durable:
    """Build tasks and compose their promises."""

    @staticmethod
    def __call__[**ParamsT, ValueT](
        *,
        execution_timeout: float,
        promise_duration: float = 365 * 24 * 60 * 60.0,
        topic: str = DEFAULT_TOPIC,
        version: int = 1,
        max_attempts: int = MAX_ATTEMPTS,
    ) -> Callable[
        [Callable[ParamsT, Awaitable[ValueT]]],
        DurableFunction[ParamsT, ValueT],
    ]:
        def wrap(
            fn: Callable[ParamsT, Awaitable[ValueT]],
        ) -> DurableFunction[ParamsT, ValueT]:
            return DurableFunction(
                fn,
                execution_timeout=execution_timeout,
                promise_duration=promise_duration,
                topic=topic,
                version=version,
                max_attempts=max_attempts,
            )

        return wrap

    async def gather[ValueT](
        self,
        *calls: DurableCall[ValueT],
    ) -> tuple[Promise[ValueT], ...]:
        promises = tuple(await asyncio.gather(*(call.submit() for call in calls)))
        assert len(promises) == len(calls)
        pending = [promise.id() for promise in promises if promise.state() is PromiseState.PENDING]
        if pending:
            raise SuspendTask(pending)
        return promises

    async def join[ValueT, JoinedT](
        self,
        task: Callable[[list[ValueT]], DurableCall[JoinedT]],
        *calls: DurableCall[ValueT],
    ) -> Promise[JoinedT]:
        """Run one task after all inputs."""

        promises = await self.gather(*calls)
        joined = await self.gather(
            task([promise.result() for promise in promises]),
        )
        return joined[0]

    async def first[ValueT](
        self,
        *calls: DurableCall[ValueT],
    ) -> Promise[ValueT]:
        """Give the first completed promise."""

        if not calls:
            raise ValueError("first needs at least one promise")
        futures = {asyncio.create_task(call.task.invoke(call.args, call.kwargs)) for call in calls}
        promises: list[Promise[ValueT]] = []
        while futures:
            done, futures = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
            for future in done:
                promise = future.result()
                promises.append(promise)
        terminal = [promise for promise in promises if promise.state() is not PromiseState.PENDING]
        if terminal:
            return min(
                terminal,
                key=lambda promise: promise.settled_order(),
            )
        raise SuspendTask(promise.id() for promise in promises)

    async def fanout[InputT, ValueT](
        self,
        task: Callable[[InputT], DurableCall[ValueT]],
        values: Iterable[InputT],
    ) -> tuple[Promise[ValueT], ...]:
        return await self.gather(*(task(value) for value in values))

    async def mapreduce[InputT, ValueT, ReducedT](
        self,
        mapper: Callable[[InputT], DurableCall[ValueT]],
        reducer: Callable[[list[ValueT]], DurableCall[ReducedT]],
        values: Iterable[InputT],
    ) -> Promise[ReducedT]:
        mapped = await self.fanout(mapper, values)
        reduced = await self.gather(
            reducer([promise.result() for promise in mapped]),
        )
        return reduced[0]

    @overload
    async def sleep(self, delay: float) -> None: ...

    @overload
    async def sleep(self, delay: timedelta) -> None: ...

    async def sleep(self, delay: object) -> None:
        """Suspend on one durable SQL timer."""

        match delay:
            case timedelta():
                seconds = delay.total_seconds()
            case float() | int():
                seconds = float(delay)
            case _:
                raise TypeError("sleep delay must be seconds or timedelta")
        if math.isnan(seconds):
            raise ValueError("sleep delay cannot be nan")
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        assert seconds > 0
        context = current_context.get()
        store = context.store if context else default_store.get()
        if store is None:
            raise RuntimeError("durable sleep needs a timer link")
        promise_id = context.next_id("durable.sleep") if context else uuid.uuid4().hex
        promise = context.find(promise_id) if context else None
        if promise is None:
            due_at = datetime.now(UTC) + timedelta(seconds=seconds)
            write(
                log,
                logging.INFO,
                "promise timer created",
                id=promise_id,
                delay=seconds,
            )
            if context is not None and context.execution is not None:
                execution = context.execution
                assert execution is not None
                root = execution.task.root_id or execution.task.id
                timer = await execution.timer(
                    SubmitTimer(
                        id=promise_id,
                        root_id=root,
                        due_at=due_at,
                        retention_ms=store.retention_ms,
                    )
                )
                promise = timer
            else:
                due_at = datetime.now(UTC) + timedelta(seconds=seconds)
                promise = await store.submit_timer(
                    SubmitTimer(
                        id=promise_id,
                        due_at=due_at,
                        retention_ms=store.retention_ms,
                    )
                )
        assert promise.id == promise_id
        if context:
            context.save(promise)
        state = promise.state
        if state is PromiseState.PENDING and context:
            raise SuspendTask((promise_id,))
        while state is PromiseState.PENDING:
            await asyncio.sleep(0.01)
            promise = await store.read_promise(promise_id)
            state = promise.state
        if state is not PromiseState.RESOLVED:
            raise ReaperError(f"timer {promise_id} is {state}")


durable = Durable()


def function_name(fn: Callable[..., object]) -> str:
    """Return the module path another skeleton can import."""

    module_name = fn.__module__
    if module_name == "__main__":
        module = sys.modules.get(module_name)
        spec = getattr(module, "__spec__", None)
        if spec is not None and spec.name:
            module_name = spec.name
    return f"{module_name}.{fn.__qualname__}"


def make_value_adapter[ValueT](result_type: object) -> TypeAdapter[ValueT]:
    """Keep byte values safe on JSON wires."""

    if isinstance(result_type, type) and issubclass(result_type, BaseModel):
        return TypeAdapter(result_type)
    config = ConfigDict(
        strict=True,
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )
    return TypeAdapter(result_type, config=config)


def make_params_model(
    fn: Callable[..., object],
    signature: inspect.Signature,
    hints: Mapping[str, object],
    context_arg: str | None,
) -> type[BaseModel]:
    """Build one strict model from the call shape."""

    fields: dict[str, tuple[object, object]] = {}
    for name, parameter in signature.parameters.items():
        if name == context_arg:
            continue
        annotation = hints.get(name)
        if annotation is None:
            raise TypeError(f"{fn.__qualname__}.{name} needs a type")
        default: object
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            annotation = types.GenericAlias(tuple, (annotation, Ellipsis))
            default = ()
        elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
            annotation = Annotated[
                types.GenericAlias(dict, (str, annotation)),
                Field(default_factory=dict),
            ]
            default = ...
        else:
            default = parameter.default
            if default is inspect.Parameter.empty:
                default = ...
        fields[name] = (annotation, default)
    model_name = f"{fn.__name__.title().replace('_', '')}Params"
    builder = cast(Callable[..., type[BaseModel]], create_model)
    return builder(
        model_name,
        __config__=ConfigDict(
            strict=True,
            extra="forbid",
            ser_json_bytes="base64",
            val_json_bytes="base64",
        ),
        **fields,
    )


def find_context_arg(
    signature: inspect.Signature,
    hints: Mapping[str, object],
) -> str | None:
    found: list[str] = []
    for name in signature.parameters:
        annotation = hints.get(name)
        if get_origin(annotation) is not Annotated:
            continue
        base, *meta = get_args(annotation)
        marked = any(isinstance(item, Injected) for item in meta)
        if marked and base is not Context:
            raise TypeError("ContextDep can mark just Context")
        if marked:
            found.append(name)
    if len(found) > 1:
        raise TypeError("a task can have one Context")
    return found[0] if found else None


def public_signature(
    signature: inspect.Signature,
    context_arg: str | None,
) -> inspect.Signature:
    params = [parameter for name, parameter in signature.parameters.items() if name != context_arg]
    return signature.replace(parameters=params)
