"""Load maintenance SQL constants."""

from importlib.resources import files

DELETE_EXPIRED = files(__package__).joinpath("delete_expired.sql").read_text()
PROCESS_DUE = files(__package__).joinpath("process_due.sql").read_text()
