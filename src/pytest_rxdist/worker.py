from __future__ import annotations

import contextlib
import gc
import io
import os
import subprocess
import sys
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerResult:
    nodeid: str
    outcome: str  # passed|failed|skipped|xfailed|xpassed|unknown
    duration_s: float
    returncode: int
    stdout: str
    stderr: str


def _classify(stdout: str, stderr: str, returncode: int) -> str:
    text = (stdout + "\n" + stderr).upper()
    if returncode == 0:
        # Could still be xpass/xfail/skip; best-effort heuristics.
        if "XPASS" in text:
            return "xpassed"
        if "XFAIL" in text:
            return "xfailed"
        if "SKIPPED" in text:
            return "skipped"
        return "passed"
    if "XFAIL" in text:
        return "xfailed"
    if "SKIPPED" in text:
        return "skipped"
    return "failed"


def _reuse_mode() -> str:
    # off|safe|aggressive
    mode = (os.environ.get("PYTEST_RXDIST_REUSE") or "safe").strip().lower()
    return mode if mode in {"off", "safe", "aggressive"} else "safe"


def _cleanup_after_test() -> None:
    # Conservative cleanup to reduce obvious cross-test leakage.
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    gc.collect()


def _run_one_subprocess(nodeid: str) -> WorkerResult:
    start = time.perf_counter()
    env = dict(os.environ)
    env["PYTEST_RXDIST_WORKER"] = "1"

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        nodeid,
        "-p",
        "no:pytest_rxdist",
        "--maxfail=1",
        "--disable-warnings",
        "-rA",
    ]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    dur = time.perf_counter() - start
    outcome = _classify(proc.stdout, proc.stderr, proc.returncode)
    return WorkerResult(
        nodeid=nodeid,
        outcome=outcome,
        duration_s=dur,
        returncode=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
    )


def _run_one_inprocess(nodeid: str) -> WorkerResult:
    # In-process execution enables warm worker reuse (imports are naturally cached).
    import pytest

    start = time.perf_counter()
    out_io = io.StringIO()
    err_io = io.StringIO()

    # Prevent recursion into our own plugin for the inner pytest run.
    args = [
        nodeid,
        "-p",
        "no:pytest_rxdist",
        "--maxfail=1",
        "--disable-warnings",
        "-rA",
    ]

    try:
        with contextlib.redirect_stdout(out_io), contextlib.redirect_stderr(err_io):
            rc = int(pytest.main(args))
    except SystemExit as e:
        try:
            rc = int(getattr(e, "code", 1) or 1)
        except Exception:
            rc = 1
    except Exception as e:  # pragma: no cover
        rc = 1
        err_io.write(f"pytest-rxdist worker in-process exception: {e!r}\n")

    dur = time.perf_counter() - start
    stdout = out_io.getvalue()
    stderr = err_io.getvalue()
    outcome = _classify(stdout, stderr, rc)
    _cleanup_after_test()
    return WorkerResult(
        nodeid=nodeid,
        outcome=outcome,
        duration_s=dur,
        returncode=rc,
        stdout=stdout,
        stderr=stderr,
    )


def run_one(nodeid: str) -> WorkerResult:
    mode = _reuse_mode()
    if mode == "off":
        r = _run_one_subprocess(nodeid)
        _cleanup_after_test()
        return r
    # safe/aggressive currently both run in-process.
    return _run_one_inprocess(nodeid)

