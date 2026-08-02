"""Suspend tasks and resume them after precise promise dependencies settle."""

from reaper.database import (
    CrossGraphWaitError,
    PromiseNotFoundError,
    TaskNotFoundError,
    TransactionExecutor,
    require_transaction,
)
from reaper.waits.models import SuspendTask
from reaper.waits.queries import SUSPEND_TASK


async def suspend_task(executor: TransactionExecutor, params: SuspendTask) -> None:
    require_transaction(executor)
    row = await executor.fetchrow(SUSPEND_TASK, params.id, list(params.awaited_ids))
    if row is None or not bool(row["waiter_exists"]):
        raise TaskNotFoundError(params.id)
    missing = tuple(str(item) for item in row["missing"])
    if missing:
        raise PromiseNotFoundError(missing[0])
    if not bool(row["same_root"]):
        raise CrossGraphWaitError(params.id, params.awaited_ids)
