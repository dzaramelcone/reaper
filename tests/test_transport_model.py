"""Generate partial-write schedules against the production frame writer."""

import asyncio

from hypothesis import given, settings, strategies

from reaper.pool import FramedWriter
from tests.transport_model import ChunkingTransport, decode_frames


@settings(max_examples=100, deadline=None)
@given(
    chunks=strategies.lists(
        strategies.integers(min_value=1, max_value=32),
        min_size=1,
        max_size=8,
    ),
    payload_size=strategies.integers(min_value=0, max_value=2_048),
)
def test_framed_writer_serializes_generated_partial_writes(
    chunks: list[int],
    payload_size: int,
) -> None:
    async def check() -> None:
        transport = ChunkingTransport(chunks)
        writer = FramedWriter(transport=transport)
        first = {"actor": "first", "payload": "a" * payload_size}
        second = {"actor": "second", "payload": "b" * payload_size}
        await asyncio.gather(writer.send(first), writer.send(second))
        assert decode_frames(transport.wire) == (first, second)

    asyncio.run(check())
