"""High-level durable-store domain facade."""

from reaper.database import ConnectionPool
from reaper.maintenance import Maintenance
from reaper.promises import Promises
from reaper.tasks import Tasks


class Store:
    """Bind all durable-promise domains to one async connection pool."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool
        self.promises = Promises(pool)
        self.tasks = Tasks(pool)
        self.maintenance = Maintenance(pool)
