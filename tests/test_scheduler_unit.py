from __future__ import annotations

from pytest_rxdist.scheduler import smart_schedule
from pytest_rxdist.timing_store import TimingStore
from pytest_rxdist.timing_store import env_fingerprint

import time
from pathlib import Path


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


def test_timing_store_avg_durations_aggregates(tmp_path):
    db = Path(tmp_path) / "timings.sqlite3"
    store = TimingStore.open(db)
    try:
        store.write_run(
            started_at=time.time(),
            env_fp=env_fingerprint(),
            rxdist_version="0.0.0",
            results=[
                {"nodeid": "a", "duration_s": 1.0, "outcome": "passed"},
                {"nodeid": "b", "duration_s": 2.0, "outcome": "passed"},
            ],
        )
        store.write_run(
            started_at=time.time() + 1,
            env_fp=env_fingerprint(),
            rxdist_version="0.0.0",
            results=[
                {"nodeid": "a", "duration_s": 3.0, "outcome": "passed"},
            ],
        )

        avg = store.avg_durations(["a", "b", "c"])
        assert avg["a"] == 2.0
        assert avg["b"] == 2.0
        assert "c" not in avg
    finally:
        store.close()

