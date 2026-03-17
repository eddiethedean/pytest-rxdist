from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path


def _run_pytest(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "pytest", *args],
        capture_output=True,
        text=True,
    )


def _run_pytest_env(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    merged = dict(os.environ)
    merged.update(env)
    return subprocess.run(
        [sys.executable, "-m", "pytest", *args],
        capture_output=True,
        text=True,
        env=merged,
    )


def test_parallel_n2_passes():
    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-reuse", "safe", "tests/test_smoke.py"])
    assert p.returncode == 0, p.stdout + "\n" + p.stderr
    assert "2 passed" in (p.stdout + p.stderr)


def test_parallel_auto_passes():
    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "auto", "--rxdist-reuse", "safe", "tests/test_smoke.py"])
    assert p.returncode == 0, p.stdout + "\n" + p.stderr
    assert "2 passed" in (p.stdout + p.stderr)


def test_failure_attribution_has_nodeid():
    # Create a failing assertion by selecting an inline test via -k doesn't help,
    # so point at a known failing nodeid in a small temporary file.
    testfile = Path("tests/_rxdist_tmp_fail.py")
    testfile.write_text(
        "def test_ok():\n"
        "    assert True\n\n"
        "def test_fail():\n"
        "    assert False\n",
        encoding="utf-8",
    )

    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-reuse", "safe", str(testfile)])
    try:
        assert p.returncode != 0
        combined = p.stdout + "\n" + p.stderr
        assert f"{testfile}::test_fail" in combined
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass


def test_failure_includes_print_output():
    testfile = Path("tests/_rxdist_tmp_output.py")
    testfile.write_text(
        "def test_fail_with_output():\n"
        "    print('HELLO_FROM_TEST')\n"
        "    assert False\n",
        encoding="utf-8",
    )

    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-reuse", "safe", str(testfile)])
    try:
        assert p.returncode != 0
        combined = p.stdout + "\n" + p.stderr
        assert f"{testfile}::test_fail_with_output" in combined
        assert "HELLO_FROM_TEST" in combined
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass


def test_concurrency_wall_clock_smoke():
    # Two tests that each sleep ~0.4s. With 2 workers we expect wall time
    # to be noticeably less than ~0.8s. Keep threshold generous to avoid flakes.
    testfile = Path("tests/_rxdist_tmp_sleep.py")
    testfile.write_text(
        "import time\n\n"
        "def test_sleep_a():\n"
        "    time.sleep(0.4)\n"
        "    assert True\n\n"
        "def test_sleep_b():\n"
        "    time.sleep(0.4)\n"
        "    assert True\n",
        encoding="utf-8",
    )

    start = time.perf_counter()
    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-reuse", "safe", str(testfile)])
    elapsed = time.perf_counter() - start

    try:
        assert p.returncode == 0, p.stdout + "\n" + p.stderr
        # If totally serial, we'd expect ~0.8s + overhead. In parallel, ~0.4s + overhead.
        assert elapsed < 0.95, f"expected concurrency; elapsed={elapsed:.3f}s\n{p.stdout}\n{p.stderr}"
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass


def test_numprocesses_0_is_serial_fallback():
    # Our plugin gates parallel mode on a truthy worker count.
    # `0` should behave like serial pytest and still pass.
    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "0", "tests/test_smoke.py"])
    assert p.returncode == 0, p.stdout + "\n" + p.stderr
    assert "2 passed" in (p.stdout + p.stderr)


def test_workers_do_not_recurse():
    # Ensure worker runs disable the pytest_rxdist plugin to prevent recursion.
    # We verify by asserting worker env var isn't leaked to the controller run
    # and the suite still completes.
    assert os.environ.get("PYTEST_RXDIST_WORKER") != "1"
    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-reuse", "safe", "tests/test_smoke.py"])
    assert p.returncode == 0, p.stdout + "\n" + p.stderr


def test_profile_creates_sqlite_and_loads_summary_on_second_run(tmp_path):
    db = tmp_path / "timings.sqlite3"
    env = {"PYTEST_RXDIST_TIMINGS_PATH": str(db)}

    p1 = _run_pytest_env(["-p", "pytest_rxdist", "-q", "--rxdist-profile", "tests/test_smoke.py"], env=env)
    assert p1.returncode == 0, p1.stdout + "\n" + p1.stderr
    assert db.exists()

    p2 = _run_pytest_env(["-p", "pytest_rxdist", "-q", "--rxdist-profile", "tests/test_smoke.py"], env=env)
    assert p2.returncode == 0, p2.stdout + "\n" + p2.stderr
    combined = p2.stdout + "\n" + p2.stderr
    assert "pytest-rxdist: timings loaded" in combined


def test_profile_corruption_recovery(tmp_path):
    db = tmp_path / "timings.sqlite3"
    db.write_bytes(b"not a sqlite database")
    env = {"PYTEST_RXDIST_TIMINGS_PATH": str(db)}

    p = _run_pytest_env(["-p", "pytest_rxdist", "-q", "--rxdist-profile", "tests/test_smoke.py"], env=env)
    assert p.returncode == 0, p.stdout + "\n" + p.stderr
    assert db.exists()
    # A rotated corrupt file should exist alongside the fresh DB.
    rotated = list(tmp_path.glob("timings.sqlite3.corrupt.*"))
    assert rotated
