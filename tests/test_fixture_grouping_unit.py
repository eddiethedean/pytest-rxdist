from __future__ import annotations

from dataclasses import dataclass

from pytest_rxdist.fixture_grouping import build_session_fixture_units, session_fixture_key


@dataclass
class _FakeFixtureDef:
    scope: str


@dataclass
class _FakeFixtureInfo:
    names_closure: list[str]
    name2fixturedefs: dict[str, list[_FakeFixtureDef]]


@dataclass
class _FakeItem:
    nodeid: str
    _fixtureinfo: _FakeFixtureInfo | None


def test_session_fixture_key_none_when_no_fixtureinfo():
    item = _FakeItem(nodeid="a::test_1", _fixtureinfo=None)
    assert session_fixture_key(item) is None


def test_session_fixture_key_filters_session_scope_and_sorts():
    finfo = _FakeFixtureInfo(
        names_closure=["db", "tmp_path", "cache"],
        name2fixturedefs={
            "db": [_FakeFixtureDef(scope="session")],
            "tmp_path": [_FakeFixtureDef(scope="function")],
            "cache": [_FakeFixtureDef(scope="session")],
        },
    )
    item = _FakeItem(nodeid="a::test_1", _fixtureinfo=finfo)
    assert session_fixture_key(item) == ("cache", "db")


def test_build_units_chunks_and_preserves_order():
    items = []
    for i in range(7):
        finfo = _FakeFixtureInfo(
            names_closure=["db"],
            name2fixturedefs={"db": [_FakeFixtureDef(scope="session")]},
        )
        items.append(_FakeItem(nodeid=f"suite::test_{i}", _fixtureinfo=finfo))

    units = build_session_fixture_units(items, max_cohort_size=3)
    assert units == [
        ["suite::test_0", "suite::test_1", "suite::test_2"],
        ["suite::test_3", "suite::test_4", "suite::test_5"],
        ["suite::test_6"],
    ]


def test_build_units_keeps_ungrouped_as_singletons_and_after_grouped():
    grouped = _FakeFixtureInfo(names_closure=["db"], name2fixturedefs={"db": [_FakeFixtureDef(scope="session")]})
    ungrouped = _FakeFixtureInfo(names_closure=["tmp_path"], name2fixturedefs={"tmp_path": [_FakeFixtureDef(scope="function")]})
    items = [
        _FakeItem(nodeid="x::test_group_1", _fixtureinfo=grouped),
        _FakeItem(nodeid="x::test_ungroup_1", _fixtureinfo=ungrouped),
        _FakeItem(nodeid="x::test_group_2", _fixtureinfo=grouped),
        _FakeItem(nodeid="x::test_ungroup_2", _fixtureinfo=None),
    ]
    units = build_session_fixture_units(items, max_cohort_size=50)
    assert units == [["x::test_group_1", "x::test_group_2"], ["x::test_ungroup_1"], ["x::test_ungroup_2"]]

