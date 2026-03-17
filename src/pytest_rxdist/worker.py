from __future__ import annotations

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


def run_one(nodeid: str) -> WorkerResult:
    start = time.perf_counter()
    env = dict(os.environ)
    env["PYTEST_RXDIST_WORKER"] = "1"

    # Run pytest for a single nodeid, ensuring we don't recurse into rxdist.
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

