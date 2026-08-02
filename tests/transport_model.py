"""Byte-stream transport model for framed IPC tests."""

import asyncio
import pickle
import struct
from collections.abc import Mapping, Sequence

from reaper.pool import HEADER, MAX_MSG


class ChunkingTransport:
    """Split every send across generated chunk boundaries."""

    def __init__(self, chunks: Sequence[int]) -> None:
        self.chunks = tuple(max(1, amount) for amount in chunks) or (1,)
        self.wire = bytearray()

    async def sendall(self, data: bytes) -> None:
        """Write all bytes while yielding between modeled short writes."""

        offset = 0
        index = 0
        while offset < len(data):
            amount = self.chunks[index % len(self.chunks)]
            self.wire.extend(data[offset : offset + amount])
            offset += amount
            index += 1
            await asyncio.sleep(0)


def decode_frames(data: bytes | bytearray) -> tuple[Mapping[str, object], ...]:
    """Decode a complete modeled byte stream or raise on corruption."""

    messages: list[Mapping[str, object]] = []
    offset = 0
    while offset < len(data):
        if len(data) - offset < HEADER.size:
            raise ValueError("trailing partial IPC header")
        size = HEADER.unpack(data[offset : offset + HEADER.size])[0]
        offset += HEADER.size
        if size > MAX_MSG or len(data) - offset < size:
            raise ValueError("trailing partial IPC payload")
        try:
            value = pickle.loads(bytes(data[offset : offset + size]))
        except (EOFError, pickle.UnpicklingError, struct.error) as error:
            raise ValueError("corrupt IPC payload") from error
        if not isinstance(value, Mapping):
            raise ValueError("IPC payload is not a mapping")
        messages.append(value)
        offset += size
    return tuple(messages)
