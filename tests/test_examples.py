"""Verify the one-file examples remain remotely importable."""

from examples.composition import capitalize, welcome
from examples.durable_timer import delayed_greeting
from examples.fanout import square, sum_squares
from examples.hello_world import hello
from examples.idempotent_root import greet
from examples.submit_root import create_report
from reaper import durable


def test_example_functions_have_importable_names() -> None:
    """Persist module names that a skeleton can load in another process."""

    functions = (
        hello,
        capitalize,
        welcome,
        square,
        sum_squares,
        greet,
        create_report,
        delayed_greeting,
    )
    assert all(task.name.startswith("examples.") for task in functions)
    assert all("<locals>" not in task.name for task in functions)
    assert callable(durable)
