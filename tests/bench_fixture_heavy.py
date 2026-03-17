from __future__ import annotations

import time

import pytest


@pytest.fixture(scope="session")
def heavy_session():
    time.sleep(0.2)
    return 123


def test_fx_0(heavy_session):
    assert heavy_session == 123


def test_fx_1(heavy_session):
    assert heavy_session == 123


def test_fx_2(heavy_session):
    assert heavy_session == 123


def test_fx_3(heavy_session):
    assert heavy_session == 123


def test_fx_4(heavy_session):
    assert heavy_session == 123


def test_fx_5(heavy_session):
    assert heavy_session == 123

