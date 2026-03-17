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

