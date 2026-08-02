"""Create and inspect durable promises."""

from collections.abc import Mapping

from reaper.database import (
    ConnectionPool,
    DatabaseExecutor,
    IdempotencyConflictError,
    PromiseNotFoundError,
    connection,
    decode_optional_json,
)
from reaper.models import PromiseState
from reaper.promises.models import PromiseRecord, SubmitTimer
from reaper.promises.queries import GET_PROMISES, SUBMIT_TIMER


def promise_from_row(row: Mapping[str, object]) -> PromiseRecord:
    values = dict(row)
    values["state"] = PromiseState(str(values["state"]))
    values["result"] = decode_optional_json(values.pop("result_json"))
    values["error"] = decode_optional_json(values.pop("error_json"))
    return PromiseRecord.model_validate(
        {
            key: values[key]
            for key in (
                "id",
                "idempotency_key",
                "state",
                "root_id",
                "result",
                "error",
                "due_at",
                "expires_at",
                "delete_after",
                "settled_at",
            )
        }
    )


async def get_promises(
    executor: DatabaseExecutor,
    promise_ids: tuple[str, ...],
) -> tuple[PromiseRecord, ...]:
    if not promise_ids:
        return ()
    rows = await executor.fetch(GET_PROMISES, list(promise_ids))
    return tuple(promise_from_row(row) for row in rows)


async def get_promise(executor: DatabaseExecutor, promise_id: str) -> PromiseRecord | None:
    promises = await get_promises(executor, (promise_id,))
    return promises[0] if promises else None


async def submit_timer(executor: DatabaseExecutor, params: SubmitTimer) -> PromiseRecord:
    fingerprint = params.fingerprint()
    row = await executor.fetchrow(
        SUBMIT_TIMER,
        params.id,
        fingerprint,
        params.root_id,
        params.due_at,
        params.retention_ms,
    )
    promise = promise_from_row(row) if row is not None else await get_promise(executor, params.id)
    if promise is None:
        raise PromiseNotFoundError(params.id)
    if promise.idempotency_key != fingerprint:
        raise IdempotencyConflictError(params.id)
    return promise


class Promises:
    """High-level durable promise API bound to a connection pool."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    async def get(self, promise_id: str) -> PromiseRecord | None:
        async with connection(self.pool) as executor:
            return await get_promise(executor, promise_id)

    async def get_many(self, promise_ids: tuple[str, ...]) -> tuple[PromiseRecord, ...]:
        async with connection(self.pool) as executor:
            return await get_promises(executor, promise_ids)

    async def timer(self, params: SubmitTimer) -> PromiseRecord:
        async with connection(self.pool) as executor:
            return await submit_timer(executor, params)
