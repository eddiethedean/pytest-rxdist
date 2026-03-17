from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path


def _run_pytest(args: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged = dict(os.environ)
    if env:
        merged.update(env)
    return subprocess.run([sys.executable, "-m", "pytest", *args], capture_output=True, text=True, env=merged)


def test_worker_crash_is_handled_and_reports_failure(tmp_path: Path):
    # Create a suite with multiple tests so some work remains after a crash.
    testfile = Path("tests/_rxdist_tmp_crash_suite.py")
    testfile.write_text(
        "def test_a():\n"
        "    assert True\n\n"
        "def test_b():\n"
        "    assert True\n\n"
        "def test_c():\n"
        "    assert True\n",
        encoding="utf-8",
    )

    try:
        p = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "2", str(testfile)],
            env={"PYTEST_RXDIST_WORKER_CRASH_AFTER": "1"},
        )
        assert p.returncode != 0
        combined = p.stdout + "\n" + p.stderr
        assert "worker died" in combined.lower()
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass


def test_smart_scheduler_beats_baseline_on_ordered_imbalanced_suite(tmp_path: Path):
    # Construct a suite ordered as: many short tests then a long test LAST.
    # Baseline shared-queue will often start the long test late; smart should start it early.
    db = tmp_path / "timings.sqlite3"
    env = {"PYTEST_RXDIST_TIMINGS_PATH": str(db)}

    testfile = Path("tests/_rxdist_tmp_makespan_suite.py")
    short = "\n".join(
        [f"def test_short_{i}():\n    import time; time.sleep(0.05); assert True\n" for i in range(8)]
    )
    testfile.write_text(
        "import time\n\n"
        + short
        + "\n"
        "def test_long_last():\n"
        "    time.sleep(1.0)\n"
        "    assert True\n",
        encoding="utf-8",
    )

    try:
        # Seed timings from a serial profiling run.
        seed = _run_pytest(["-p", "pytest_rxdist", "-q", "--rxdist-profile", str(testfile)], env=env)
        assert seed.returncode == 0, seed.stdout + "\n" + seed.stderr

        start = time.perf_counter()
        base = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-scheduler", "baseline", str(testfile)],
            env=env,
        )
        t_base = time.perf_counter() - start
        assert base.returncode == 0, base.stdout + "\n" + base.stderr

        start = time.perf_counter()
        smart = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-scheduler", "smart", str(testfile)],
            env=env,
        )
        t_smart = time.perf_counter() - start
        assert smart.returncode == 0, smart.stdout + "\n" + smart.stderr

        # Expect meaningful improvement; keep margin generous.
        assert t_smart < t_base - 0.15, f"expected smart faster: baseline={t_base:.3f}s smart={t_smart:.3f}s"
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass
