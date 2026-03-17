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


def test_rust_engine_matches_python_engine_on_simple_suite(tmp_path: Path):
    testfile = Path("tests/_rxdist_tmp_rust_parity.py")
    testfile.write_text(
        "def test_ok():\n"
        "    assert True\n\n"
        "def test_fail():\n"
        "    assert False\n",
        encoding="utf-8",
    )

    try:
        base = ["-p", "pytest_rxdist", "-q", "--numprocesses", "2", str(testfile)]
        py = _run_pytest([*base, "--rxdist-engine", "python"])
        rs = _run_pytest([*base, "--rxdist-engine", "rust"])
        assert py.returncode == rs.returncode == 1
        assert "1 failed" in (py.stdout + "\n" + py.stderr)
        assert "1 failed" in (rs.stdout + "\n" + rs.stderr)
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass


def test_rust_engine_shm_batching_smoke(tmp_path: Path):
    testfile = Path("tests/_rxdist_tmp_rust_shm_batch.py")
    big = "Z" * 50000
    testfile.write_text(
        "import sys\n\n"
        "def test_ok():\n"
        f"    print({big!r})\n"
        "    print('err', file=sys.stderr)\n"
        "    assert True\n",
        encoding="utf-8",
    )
    try:
        env = dict(os.environ)
        env["PYTEST_RXDIST_SHM_THRESHOLD_BYTES"] = "1024"
        p = _run_pytest(
            [
                "-p",
                "pytest_rxdist",
                "-q",
                "--numprocesses",
                "1",
                "--rxdist-engine",
                "rust",
                "--rxdist-ipc",
                "shm",
                "--rxdist-ipc-batch-size",
                "4",
                str(testfile),
            ],
            env=env,
        )
        assert p.returncode == 0, p.stdout + "\n" + p.stderr
        assert "1 passed" in (p.stdout + "\n" + p.stderr)
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass


def test_rust_worker_harness_smoke(tmp_path: Path):
    testfile = Path("tests/_rxdist_tmp_rust_worker_smoke.py")
    testfile.write_text(
        "def test_ok1():\n"
        "    assert True\n\n"
        "def test_ok2():\n"
        "    assert True\n",
        encoding="utf-8",
    )
    try:
        p = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-worker", "rust", str(testfile)]
        )
        assert p.returncode == 0, p.stdout + "\n" + p.stderr
        assert "2 passed" in (p.stdout + "\n" + p.stderr)
    finally:
        try:
            testfile.unlink()
        except OSError:
            pass

