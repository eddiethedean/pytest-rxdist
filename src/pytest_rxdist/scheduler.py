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

