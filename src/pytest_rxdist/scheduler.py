from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class SmartSchedule:
    per_worker: list[list[str]]
    known_count: int
    unknown_count: int
    estimated_totals_s: list[float]

    @property
    def estimated_makespan_s(self) -> float:
        return max(self.estimated_totals_s, default=0.0)


def smart_schedule(
    nodeids: Iterable[str],
    *,
    num_workers: int,
    avg_durations_s: dict[str, float],
) -> SmartSchedule:
    n = max(1, int(num_workers))
    nodeids_list = [str(x) for x in nodeids]

    known: list[tuple[str, float]] = []
    unknown: list[str] = []
    for nid in nodeids_list:
        d = avg_durations_s.get(nid)
        if d is None:
            unknown.append(nid)
        else:
            known.append((nid, float(d)))

    known.sort(key=lambda t: t[1], reverse=True)

    per_worker: list[list[str]] = [[] for _ in range(n)]
    totals: list[float] = [0.0 for _ in range(n)]

    # LPT: assign longest-known tests first to the currently-lightest worker.
    for nid, dur in known:
        idx = min(range(n), key=lambda i: totals[i])
        per_worker[idx].append(nid)
        totals[idx] += max(0.0, dur)

    # Interleave unknowns round-robin after known assignment.
    for i, nid in enumerate(unknown):
        per_worker[i % n].append(nid)

    return SmartSchedule(
        per_worker=per_worker,
        known_count=len(known),
        unknown_count=len(unknown),
        estimated_totals_s=totals,
    )


def smart_schedule_units(
    units: Iterable[list[str]],
    *,
    num_workers: int,
    avg_durations_s: dict[str, float],
) -> SmartSchedule:
    """
    Schedule units (lists of nodeids) as atomic groups.

    Returns a SmartSchedule with per_worker flattened to nodeids, but units will not
    be split across workers.
    """
    n = max(1, int(num_workers))
    units_list = [list(u) for u in units]

    known_units: list[tuple[int, float]] = []
    unknown_units: list[int] = []
    known_count = 0
    unknown_count = 0

    for idx, unit in enumerate(units_list):
        est = 0.0
        unit_any_known = False
        for nid in unit:
            d = avg_durations_s.get(str(nid))
            if d is None:
                unknown_count += 1
            else:
                unit_any_known = True
                known_count += 1
                est += max(0.0, float(d))
        if unit_any_known:
            known_units.append((idx, est))
        else:
            unknown_units.append(idx)

    known_units.sort(key=lambda t: t[1], reverse=True)

    per_worker: list[list[str]] = [[] for _ in range(n)]
    totals: list[float] = [0.0 for _ in range(n)]

    # LPT by unit estimated total.
    for unit_idx, est in known_units:
        widx = min(range(n), key=lambda i: totals[i])
        per_worker[widx].extend([str(x) for x in units_list[unit_idx]])
        totals[widx] += est

    # Round-robin unknown units after known assignment.
    for i, unit_idx in enumerate(unknown_units):
        per_worker[i % n].extend([str(x) for x in units_list[unit_idx]])

    return SmartSchedule(
        per_worker=per_worker,
        known_count=known_count,
        unknown_count=unknown_count,
        estimated_totals_s=totals,
    )

