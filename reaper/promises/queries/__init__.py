"""Load promise SQL constants."""

from importlib.resources import files

GET_PROMISES = files(__package__).joinpath("get_promises.sql").read_text()
SUBMIT_TIMER = files(__package__).joinpath("submit_timer.sql").read_text()
