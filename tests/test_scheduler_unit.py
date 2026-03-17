from __future__ import annotations

from pytest_rxdist.scheduler import smart_schedule


def test_smart_schedule_contains_all_nodeids_once():
    nodeids = ["a", "b", "c", "d", "e"]
    durations = {"a": 5.0, "b": 4.0, "c": 3.0}
    s = smart_schedule(nodeids, num_workers=2, avg_durations_s=durations)

    flat = [x for w in s.per_worker for x in w]
    assert sorted(flat) == sorted(nodeids)
    assert len(flat) == len(set(flat))
    assert s.known_count == 3
    assert s.unknown_count == 2


def test_smart_schedule_balances_known_work_reasonably():
    nodeids = ["t1", "t2", "t3", "t4"]
    durations = {"t1": 10.0, "t2": 9.0, "t3": 1.0, "t4": 1.0}
    s = smart_schedule(nodeids, num_workers=2, avg_durations_s=durations)
    # With LPT, totals should be close: (10+1) vs (9+1)
    totals = sorted(s.estimated_totals_s)
    assert abs(totals[1] - totals[0]) <= 2.0

