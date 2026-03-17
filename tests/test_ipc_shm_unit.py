from __future__ import annotations

import pytest

from pytest_rxdist.shm import ShmTextRef, cleanup_shm, read_text_from_shm, write_text_to_shm


def test_shm_roundtrip_and_cleanup():
    ref = write_text_to_shm("hello shm")
    assert isinstance(ref, ShmTextRef)
    assert ref.kind == "shm"
    assert ref.size > 0
    assert read_text_from_shm(ref) == "hello shm"

    cleanup_shm(ref)

    # After unlink, attaching should fail (platform dependent error type).
    with pytest.raises(Exception):
        read_text_from_shm(ref)


def test_shm_large_payload_roundtrip():
    big = "A" * 200000
    ref = write_text_to_shm(big)
    try:
        out = read_text_from_shm(ref)
        assert out == big
    finally:
        cleanup_shm(ref)

