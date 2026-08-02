"""SQL constants for transport-level PostgreSQL operations."""

from importlib.resources import files

PROBE = files(__package__).joinpath("probe.sql").read_text()
