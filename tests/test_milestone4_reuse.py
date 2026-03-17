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


def test_reuse_safe_passes_existing_suite():
    p = _run_pytest(["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-reuse", "safe", "tests/test_smoke.py"])
    assert p.returncode == 0, p.stdout + "\n" + p.stderr


def test_reuse_safe_is_faster_than_off_for_heavy_import(tmp_path: Path):
    # Heavy import simulated by sleeping at import time.
    testfile = Path("tests/_rxdist_tmp_heavy_import_suite.py")
    testfile.write_text(
        "import time\n"
        "time.sleep(0.25)\n\n"
        "def test_one():\n"
        "    assert True\n\n"
        "def test_two():\n"
        "    assert True\n\n"
        "def test_three():\n"
        "    assert True\n",
        encoding="utf-8",
    )

    try:
        start = time.perf_counter()
        off = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "1", "--rxdist-reuse", "off", str(testfile)]
        )
        t_off = time.perf_counter() - start
        assert off.returncode == 0, off.stdout + "\n" + off.stderr

        start = time.perf_counter()
        safe = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "1", "--rxdist-reuse", "safe", str(testfile)]
        )
        t_safe = time.perf_counter() - start
        assert safe.returncode == 0, safe.stdout + "\n" + safe.stderr

        # Off: import sleep repeats per test invocation; safe: import sleep paid once per worker.
        assert t_safe < t_off - 0.2, f"expected reuse faster: off={t_off:.3f}s safe={t_safe:.3f}s"
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass

