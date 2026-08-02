"""Advance deadlines and enforce bounded durable-state retention."""

from reaper.database import (
    ConnectionPool,
    DatabaseExecutor,
    connection,
)
from reaper.maintenance.models import Deleted, DeleteExpired, ProcessDue, ProcessedDue
from reaper.maintenance.queries import DELETE_EXPIRED, PROCESS_DUE


async def process_due(
    executor: DatabaseExecutor,
    params: ProcessDue | None = None,
) -> ProcessedDue:
    params = ProcessDue() if params is None else params
    row = await executor.fetchrow(PROCESS_DUE, params.limit)
    return ProcessedDue.model_validate(dict(row), extra="ignore")


async def delete_expired(
    executor: DatabaseExecutor, params: DeleteExpired | None = None
) -> Deleted:
    params = DeleteExpired() if params is None else params
    rows = await executor.fetch(DELETE_EXPIRED, params.before, params.limit)
    return Deleted(roots=len(rows))


class Maintenance:
    """High-level maintenance API bound to a connection pool."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    async def process_due(self, params: ProcessDue | None = None) -> ProcessedDue:
        async with connection(self.pool) as executor:
            return await process_due(executor, params)

    async def delete_expired(self, params: DeleteExpired | None = None) -> Deleted:
        async with connection(self.pool) as executor:
            return await delete_expired(executor, params)
