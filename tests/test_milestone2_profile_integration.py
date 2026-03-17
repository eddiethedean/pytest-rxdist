from __future__ import annotations

import sqlite3
import subprocess
import sys
import time
from pathlib import Path


def _run_pytest(args: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "pytest", *args],
        capture_output=True,
        text=True,
        env=env,
    )


def _merged_env(extra: dict[str, str]) -> dict[str, str]:
    import os

    merged = dict(os.environ)
    merged.update(extra)
    return merged


def _db_rows(db: Path, sql: str, params: tuple = ()) -> list[tuple]:
    conn = sqlite3.connect(str(db))
    try:
        cur = conn.execute(sql, params)
        return list(cur.fetchall())
    finally:
        conn.close()


def test_profile_serial_writes_db_and_contains_expected_columns(tmp_path: Path):
    db = tmp_path / "timings.sqlite3"
    env = _merged_env({"PYTEST_RXDIST_TIMINGS_PATH": str(db)})

    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--rxdist-profile", "tests/test_smoke.py"], env=env)
    assert p.returncode == 0, p.stdout + "\n" + p.stderr
    assert db.exists()

    runs = _db_rows(db, "SELECT env_fingerprint, rxdist_version FROM runs")
    assert runs
    (env_fp, rxdist_version) = runs[-1]
    assert isinstance(env_fp, str) and "python=" in env_fp and "platform=" in env_fp
    assert isinstance(rxdist_version, str) and rxdist_version

    results = _db_rows(db, "SELECT nodeid, duration_s, outcome FROM test_results")
    assert results
    nodeids = {row[0] for row in results}
    assert "tests/test_smoke.py::test_smoke" in nodeids
    assert "tests/test_smoke.py::test_rust_binding_hello" in nodeids
    for _nodeid, duration_s, outcome in results:
        assert float(duration_s) >= 0.0
        assert outcome in {"passed", "failed", "skipped", "xfailed", "xpassed", "unknown"}


def test_profile_parallel_writes_db_and_does_not_spam_summary(tmp_path: Path):
    db = tmp_path / "timings.sqlite3"
    env = _merged_env({"PYTEST_RXDIST_TIMINGS_PATH": str(db)})

    p1 = _run_pytest(
        ["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-profile", "tests/test_smoke.py"],
        env=env,
    )
    assert p1.returncode == 0, p1.stdout + "\n" + p1.stderr
    assert db.exists()

    p2 = _run_pytest(
        ["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-profile", "tests/test_smoke.py"],
        env=env,
    )
    assert p2.returncode == 0, p2.stdout + "\n" + p2.stderr
    combined = p2.stdout + "\n" + p2.stderr
    # Summary should print once per controller run (workers disable the plugin).
    assert combined.count("pytest-rxdist: timings loaded") == 1

    results = _db_rows(db, "SELECT nodeid, duration_s, outcome FROM test_results")
    assert results


def test_summary_is_ordered_and_limited(tmp_path: Path):
    db = tmp_path / "timings.sqlite3"
    env = _merged_env({"PYTEST_RXDIST_TIMINGS_PATH": str(db)})
    testfile = tmp_path / "tmp_sleep_suite.py"

    testfile.write_text(
        "import time\n\n"
        "def test_fast():\n"
        "    time.sleep(0.01)\n"
        "    assert True\n\n"
        "def test_slow():\n"
        "    time.sleep(0.05)\n"
        "    assert True\n",
        encoding="utf-8",
    )

    # First run: write data.
    p1 = _run_pytest(["-p", "pytest_rxdist", "-q", "--rxdist-profile", str(testfile)], env=env)
    assert p1.returncode == 0, p1.stdout + "\n" + p1.stderr

    # Second run: should load and print slowest list (limit=5 in implementation).
    p2 = _run_pytest(["-p", "pytest_rxdist", "-q", "--rxdist-profile", str(testfile)], env=env)
    assert p2.returncode == 0, p2.stdout + "\n" + p2.stderr
    combined = p2.stdout + "\n" + p2.stderr
    slow_lines = [ln for ln in combined.splitlines() if ln.strip().startswith("slow:") or "  slow:" in ln]
    assert slow_lines, combined

    # Expect test_slow to appear before test_fast in the printed slowest section.
    idx_slow = combined.find("test_slow")
    idx_fast = combined.find("test_fast")
    assert idx_slow != -1 and idx_fast != -1
    assert idx_slow < idx_fast


def test_invalid_numprocesses_values_fail_fast():
    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "-1", "tests/test_smoke.py"])
    assert p.returncode != 0
    assert "numprocesses must be >= 0" in (p.stdout + "\n" + p.stderr)

    p2 = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "wat", "tests/test_smoke.py"])
    assert p2.returncode != 0
    assert "invalid" in (p2.stdout + "\n" + p2.stderr).lower()


def test_profile_perf_sanity_not_extreme_overhead(tmp_path: Path):
    # Very loose check: profiling shouldn't be orders of magnitude slower.
    # Use the same tiny suite twice to minimize noise.
    db = tmp_path / "timings.sqlite3"
    env = _merged_env({"PYTEST_RXDIST_TIMINGS_PATH": str(db)})

    start = time.perf_counter()
    p1 = _run_pytest(["-p", "pytest_rxdist", "-q", "tests/test_smoke.py"], env=env)
    t_no = time.perf_counter() - start
    assert p1.returncode == 0, p1.stdout + "\n" + p1.stderr

    start = time.perf_counter()
    p2 = _run_pytest(["-p", "pytest_rxdist", "-q", "--rxdist-profile", "tests/test_smoke.py"], env=env)
    t_prof = time.perf_counter() - start
    assert p2.returncode == 0, p2.stdout + "\n" + p2.stderr

    # Allow a lot of slack for first-time sqlite init, CI noise, etc.
    assert t_prof < max(2.0, t_no * 5.0), f"profiling too slow: no={t_no:.3f}s prof={t_prof:.3f}s"


def test_smart_scheduler_uses_timings_and_reports_stats(tmp_path: Path):
    db = tmp_path / "timings.sqlite3"
    env = _merged_env({"PYTEST_RXDIST_TIMINGS_PATH": str(db)})
    # Write the suite under repo-local tests/ so worker subprocesses can resolve
    # nodeids consistently from the project root.
    testfile = Path("tests/_rxdist_tmp_imbalanced_suite.py")

    testfile.write_text(
        "import time\n\n"
        "def test_slow():\n"
        "    time.sleep(0.05)\n"
        "    assert True\n\n"
        "def test_fast1():\n"
        "    time.sleep(0.005)\n"
        "    assert True\n\n"
        "def test_fast2():\n"
        "    time.sleep(0.005)\n"
        "    assert True\n",
        encoding="utf-8",
    )

    # Seed timings.
    try:
        seed = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--rxdist-profile", str(testfile)],
            env=env,
        )
        assert seed.returncode == 0, seed.stdout + "\n" + seed.stderr

        # Run with smart scheduler and debug on; expect scheduler stats line.
        run = _run_pytest(
            [
                "-p",
                "pytest_rxdist",
                "-q",
                "--numprocesses",
                "2",
                "--rxdist-scheduler",
                "smart",
                "--rxdist-debug",
                str(testfile),
            ],
            env=env,
        )
        assert run.returncode == 0, run.stdout + "\n" + run.stderr
        combined = run.stdout + "\n" + run.stderr
        assert "pytest-rxdist: smart_schedule" in combined
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass

