from __future__ import annotations

from dataclasses import dataclass
from typing import Any, BinaryIO, Iterable

import struct

import msgpack


@dataclass(frozen=True)
class Message:
    type: str
    payload: dict[str, Any]


def _read_exact(stream: BinaryIO, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError("unexpected EOF")
        buf.extend(chunk)
    return bytes(buf)


def iter_messages(stream: BinaryIO) -> Iterable[Message]:
    # Length-prefixed MessagePack frames: [u32_be length][msgpack bytes]
    while True:
        try:
            header = _read_exact(stream, 4)
        except EOFError:
            return
        (size,) = struct.unpack(">I", header)
        data = _read_exact(stream, size)
        obj = msgpack.unpackb(data, raw=False)
        if not isinstance(obj, dict) or "type" not in obj:
            continue
        payload = obj.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {"value": payload}
        yield Message(type=str(obj["type"]), payload=payload)


def send_message(stream: BinaryIO, msg_type: str, payload: dict[str, Any]) -> None:
    data = msgpack.packb({"type": msg_type, "payload": payload}, use_bin_type=True)
    stream.write(struct.pack(">I", len(data)))
    stream.write(data)
    stream.flush()
