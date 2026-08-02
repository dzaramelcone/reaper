"""Load durable-wait SQL constants."""

from importlib.resources import files

SUSPEND_TASK = files(__package__).joinpath("suspend_task.sql").read_text()
