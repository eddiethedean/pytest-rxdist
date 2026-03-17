from __future__ import annotations

import random
import time


def test_mixed_fast_0():
    assert True


def test_mixed_fast_1():
    assert True


def test_mixed_fast_2():
    assert True


def test_mixed_fast_3():
    assert True


def test_mixed_fast_4():
    assert True


def test_mixed_fast_5():
    assert True


def test_mixed_fast_6():
    assert True


def test_mixed_fast_7():
    assert True


def test_mixed_fast_8():
    assert True


def test_mixed_fast_9():
    assert True


def test_mixed_slow_0():
    time.sleep(0.15 + random.random() * 0.05)
    assert True


def test_mixed_slow_1():
    time.sleep(0.15 + random.random() * 0.05)
    assert True


def test_mixed_slow_2():
    time.sleep(0.15 + random.random() * 0.05)
    assert True


def test_mixed_slow_3():
    time.sleep(0.15 + random.random() * 0.05)
    assert True


