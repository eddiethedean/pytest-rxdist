from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _run_pytest(args: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged = dict(os.environ)
    if env:
        merged.update(env)
    return subprocess.run([sys.executable, "-m", "pytest", *args], capture_output=True, text=True, env=merged)


def test_fixture_grouping_session_debug_output_and_cap(tmp_path: Path):
    testfile = Path("tests/_rxdist_tmp_fixture_grouping_suite.py")
    testfile.write_text(
        "import time\n"
        "import pytest\n\n"
        "@pytest.fixture(scope='session')\n"
        "def expensive():\n"
        "    time.sleep(0.15)\n"
        "    return 123\n\n"
        + "\n".join(
            [
                f"def test_a_{i}(expensive):\n    assert expensive == 123\n"
                for i in range(7)
            ]
        )
        + "\n"
        "def test_b_uses_no_session_fixture(tmp_path):\n"
        "    assert tmp_path\n",
        encoding="utf-8",
    )

    try:
        p = _run_pytest(
            [
                "-p",
                "pytest_rxdist",
                "-q",
                "--numprocesses",
                "2",
                "--rxdist-reuse",
                "safe",
                "--rxdist-fixture-grouping",
                "session",
                "--rxdist-fixture-grouping-max-cohort-size",
                "3",
                "--rxdist-debug",
                str(testfile),
            ]
        )
        assert p.returncode == 0, p.stdout + "\n" + p.stderr
        combined = p.stdout + "\n" + p.stderr
        assert "fixture_grouping=session" in combined
        assert "max_cohort_size=3" in combined
        assert "8 passed" in combined
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass


def test_fixture_grouping_is_opt_in(tmp_path: Path):
    testfile = Path("tests/_rxdist_tmp_fixture_grouping_optin.py")
    testfile.write_text(
        "import pytest\n\n"
        "@pytest.fixture(scope='session')\n"
        "def expensive():\n"
        "    return 1\n\n"
        "def test_1(expensive):\n"
        "    assert expensive == 1\n",
        encoding="utf-8",
    )

    try:
        p = _run_pytest(
            [
                "-p",
                "pytest_rxdist",
                "-q",
                "--numprocesses",
                "2",
                "--rxdist-debug",
                str(testfile),
            ]
        )
        assert p.returncode == 0, p.stdout + "\n" + p.stderr
        combined = p.stdout + "\n" + p.stderr
        assert "fixture_grouping=session" not in combined
        assert "1 passed" in combined
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass


def test_fixture_grouping_multiple_cohorts_and_smart_scheduler(tmp_path: Path):
    # Create two distinct session fixtures to form two cohorts + one ungrouped test.
    db = tmp_path / "timings.sqlite3"
    env = {"PYTEST_RXDIST_TIMINGS_PATH": str(db)}

    testfile = Path("tests/_rxdist_tmp_fixture_grouping_multi.py")
    testfile.write_text(
        "import time\n"
        "import pytest\n\n"
        "@pytest.fixture(scope='session')\n"
        "def fx_a():\n"
        "    time.sleep(0.05)\n"
        "    return 'a'\n\n"
        "@pytest.fixture(scope='session')\n"
        "def fx_b():\n"
        "    time.sleep(0.05)\n"
        "    return 'b'\n\n"
        + "\n".join([f"def test_a_{i}(fx_a):\n    assert fx_a == 'a'\n" for i in range(4)])
        + "\n"
        + "\n".join([f"def test_b_{i}(fx_b):\n    assert fx_b == 'b'\n" for i in range(4)])
        + "\n"
        + "def test_ungrouped(tmp_path):\n    assert tmp_path\n",
        encoding="utf-8",
    )

    try:
        seed = _run_pytest(["-p", "pytest_rxdist", "-q", "--rxdist-profile", str(testfile)], env=env)
        assert seed.returncode == 0, seed.stdout + "\n" + seed.stderr

        p = _run_pytest(
            [
                "-p",
                "pytest_rxdist",
                "-q",
                "--numprocesses",
                "2",
                "--rxdist-scheduler",
                "smart",
                "--rxdist-reuse",
                "safe",
                "--rxdist-fixture-grouping",
                "session",
                "--rxdist-fixture-grouping-max-cohort-size",
                "10",
                "--rxdist-debug",
                str(testfile),
            ],
            env=env,
        )
        assert p.returncode == 0, p.stdout + "\n" + p.stderr
        combined = p.stdout + "\n" + p.stderr
        # Stats line proves grouping ran; should have grouped tests and at least one cohort.
        assert "fixture_grouping=session" in combined
        assert "grouped=" in combined and "cohorts=" in combined
        assert "9 passed" in combined
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass

