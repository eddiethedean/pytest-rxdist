from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class FixtureGroupingStats:
    cohorts: int
    grouped_tests: int
    ungrouped_tests: int
    max_cohort_size: int


def _chunk(seq: Sequence[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    return [list(seq[i : i + size]) for i in range(0, len(seq), size)]


def session_fixture_key(item) -> tuple[str, ...] | None:
    """
    Best-effort: return a stable key describing the session-scoped fixtures used by this item.

    Returns:
      - tuple[str,...] if any session-scoped fixtures are discovered
      - None if none are discovered or fixture information is unavailable
    """
    try:
        finfo = getattr(item, "_fixtureinfo", None)
        if finfo is None:
            return None

        closure = list(getattr(finfo, "names_closure", []) or [])
        name2defs = getattr(finfo, "name2fixturedefs", None)
        if not isinstance(name2defs, dict):
            return None

        session_names: list[str] = []
        for name in closure:
            defs = name2defs.get(name)
            if not defs:
                continue
            # Use the last definition (closest) if multiple exist.
            last_def = defs[-1]
            scope = getattr(last_def, "scope", None)
            if scope == "session":
                session_names.append(str(name))

        if not session_names:
            return None
        return tuple(sorted(set(session_names)))
    except Exception:
        return None


def build_session_fixture_units(items: Iterable, *, max_cohort_size: int) -> list[list[str]]:
    """
    Build execution units from items based on shared session-scoped fixtures.

    - Items with no session-scoped fixtures become singleton units.
    - Each cohort is split into chunks of at most max_cohort_size (deterministic).
    - Ordering is stable: cohorts appear in order of first appearance in collection,
      and within each cohort we preserve collection order.
    """
    cap = max(1, int(max_cohort_size))

    # Ordered by first time each cohort key appears.
    cohorts: "OrderedDict[tuple[str, ...], list[str]]" = OrderedDict()
    ungrouped: list[str] = []

    for item in items:
        nodeid = str(getattr(item, "nodeid"))
        key = session_fixture_key(item)
        if key is None:
            ungrouped.append(nodeid)
        else:
            cohorts.setdefault(key, []).append(nodeid)

    units: list[list[str]] = []
    for _key, nodeids in cohorts.items():
        units.extend(_chunk(nodeids, cap))

    # Keep ungrouped as singletons to avoid surprising coupling.
    units.extend([[nid] for nid in ungrouped])
    return units


def stats_for_units(units: list[list[str]], *, max_cohort_size: int) -> FixtureGroupingStats:
    # We don’t retain cohort keys here; approximate “cohorts” by counting non-singleton units.
    grouped = sum(len(u) for u in units if len(u) > 1)
    ungrouped = sum(len(u) for u in units if len(u) == 1)
    cohorts = sum(1 for u in units if len(u) > 1)
    return FixtureGroupingStats(
        cohorts=cohorts,
        grouped_tests=grouped,
        ungrouped_tests=ungrouped,
        max_cohort_size=max(1, int(max_cohort_size)),
    )

