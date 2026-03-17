from __future__ import annotations

from dataclasses import dataclass
from multiprocessing import shared_memory
from typing import Final


DEFAULT_THRESHOLD_BYTES: Final[int] = 16 * 1024


@dataclass(frozen=True)
class ShmTextRef:
    kind: str  # "shm"
    name: str
    size: int
    encoding: str = "utf-8"


def write_text_to_shm(text: str, *, encoding: str = "utf-8") -> ShmTextRef:
    data = text.encode(encoding, errors="replace")
    shm = shared_memory.SharedMemory(create=True, size=len(data))
    try:
        shm.buf[: len(data)] = data
        return ShmTextRef(kind="shm", name=shm.name, size=len(data), encoding=encoding)
    finally:
        # Close our handle; the creator keeps the segment alive until unlink.
        shm.close()


def read_text_from_shm(ref: ShmTextRef) -> str:
    shm = shared_memory.SharedMemory(name=ref.name, create=False)
    try:
        data = bytes(shm.buf[: ref.size])
        return data.decode(ref.encoding, errors="replace")
    finally:
        shm.close()


def cleanup_shm(ref: ShmTextRef) -> None:
    try:
        shm = shared_memory.SharedMemory(name=ref.name, create=False)
    except FileNotFoundError:
        return
    try:
        shm.unlink()
    finally:
        shm.close()

