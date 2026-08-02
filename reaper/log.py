"""Readable structured logging shared by Reaper and its skeletons."""

import json
import logging
import re
import sys
from collections.abc import Mapping

TAG_ATTRIBUTE = "reaper_tags"
SIMPLE_VALUE = re.compile(r"^[A-Za-z0-9_./:@+\-]*$")
DEFAULT_LINE_WIDTH = 100
TAG_PREFIX = "    tags: "
TAG_CONTINUATION = " " * len(TAG_PREFIX)


class ReaperFormatter(logging.Formatter):
    """Render a compact record followed by an optional indented tag line."""

    def __init__(self, *, line_width: int = DEFAULT_LINE_WIDTH) -> None:
        super().__init__()
        if line_width <= len(TAG_PREFIX):
            raise ValueError("log line width is too small for tag indentation")
        self.line_width = line_width

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        timestamp = f"{timestamp}.{int(record.msecs):03d}"
        message = record.getMessage()
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        rendered = f"[{timestamp}] [{record.levelname}] [{record.name}:{record.lineno}]: {message}"
        tags = getattr(record, TAG_ATTRIBUTE, None)
        if not isinstance(tags, Mapping) or not tags:
            return rendered
        values = tuple(
            f"{key}={format_tag_value(value)}"
            for key, value in tags.items()
            if value is not None and value != ""
        )
        return rendered if not values else f"{rendered}\n{wrap_tags(values, self.line_width)}"


def format_tag_value(value: object) -> str:
    """Quote tag values only when plain token rendering would be ambiguous."""

    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if SIMPLE_VALUE.fullmatch(text):
        return text
    return json.dumps(text, ensure_ascii=False)


def wrap_tags(values: tuple[str, ...], width: int) -> str:
    """Wrap complete tag tokens and align continuation values."""

    lines: list[str] = []
    current = TAG_PREFIX
    for value in values:
        separator = "" if current in {TAG_PREFIX, TAG_CONTINUATION} else " "
        if separator and len(current) + len(separator) + len(value) > width:
            lines.append(current)
            current = TAG_CONTINUATION + value
        else:
            current += separator + value
    lines.append(current)
    return "\n".join(lines)


def configure_logging(level: str) -> None:
    """Configure one process with Reaper's readable structured format."""

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(ReaperFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler])


def write(
    logger: logging.Logger,
    level: int,
    message: str,
    **tags: object,
) -> None:
    """Write a record with structured tags and the caller's source location."""

    logger.log(
        level,
        message,
        extra={TAG_ATTRIBUTE: tags},
        stacklevel=2,
    )
