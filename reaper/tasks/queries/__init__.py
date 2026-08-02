"""Load task SQL constants."""

from importlib.resources import files

CLAIM = files(__package__).joinpath("claim.sql").read_text()
LOCK_PROMISE = files(__package__).joinpath("lock_promise.sql").read_text()
RETRY = files(__package__).joinpath("retry.sql").read_text()
SETTLE = files(__package__).joinpath("settle.sql").read_text()
SUBMIT_CALLS = files(__package__).joinpath("submit_calls.sql").read_text()
