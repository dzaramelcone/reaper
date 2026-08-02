"""Structured human-readable logging checks."""

import logging
import re
from typing import cast

import pytest

from reaper.log import ReaperFormatter, write


class TaggedLogRecord(logging.LogRecord):
    """Add the structured attribute installed by reaper.log.write."""

    reaper_tags: dict[str, object]


def test_formatter_renders_record_and_quoted_tags() -> None:
    record = TaggedLogRecord(
        name="reaper.worker",
        level=logging.INFO,
        pathname="worker.py",
        lineno=42,
        msg="task acquired",
        args=(),
        exc_info=None,
    )
    record.created = 0
    record.msecs = 0
    record.reaper_tags = {
        "id": "root-1",
        "version": 2,
        "healthy": True,
        "detail": "two words\nand a line",
        "owner": "skeleton-0123456789abcdef0123456789abcdef0123456789abcdef",
        "empty": "",
    }

    rendered = ReaperFormatter().format(record)

    assert re.match(r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.000\]", rendered)
    assert "[INFO] [reaper.worker:42]: task acquired\n" in rendered
    lines = rendered.splitlines()
    assert lines[1].startswith("    tags: id=root-1")
    assert lines[2].startswith("          owner=skeleton-")
    assert 'detail="two words\\nand a line"' in rendered
    assert "empty=" not in rendered


def test_write_preserves_call_site_and_structured_tags(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("reaper.test")
    caplog.set_level(logging.INFO, logger="reaper.test")

    write(logger, logging.INFO, "hello", answer=42)

    record = cast(TaggedLogRecord, caplog.records[-1])
    assert record.message == "hello"
    assert record.reaper_tags == {"answer": 42}
    assert re.fullmatch(r"test_logging\.py", record.filename)
    assert record.funcName == "test_write_preserves_call_site_and_structured_tags"
