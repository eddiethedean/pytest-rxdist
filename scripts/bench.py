from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RunResult:
    label: str
    returncode: int
    elapsed_s: float


def _run_pytest(args: list[str], *, env: dict[str, str] | None = None) -> RunResult:
    merged = dict(os.environ)
    if env:
        merged.update(env)
    start = time.perf_counter()
    p = subprocess.run([sys.executable, "-m", "pytest", *args], env=merged, capture_output=True, text=True)
    elapsed = time.perf_counter() - start
    label = " ".join(args)
    if p.returncode != 0:
        sys.stderr.write(p.stdout + "\n" + p.stderr + "\n")
    return RunResult(label=label, returncode=p.returncode, elapsed_s=elapsed)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", choices=["tiny", "fixture"], default="tiny")
    ap.add_argument("--numprocesses", default="auto")
    ap.add_argument("--engine", choices=["python", "rust"], default="python")
    ap.add_argument("--runs", type=int, default=3)
    ns = ap.parse_args(argv)

    repo = Path(__file__).resolve().parents[1]
    if ns.suite == "tiny":
        testpath = repo / "tests" / "bench_many_tiny.py"
    else:
        testpath = repo / "tests" / "bench_fixture_heavy.py"

    base = [
        "-p",
        "pytest_rxdist",
        "-q",
        "--numprocesses",
        str(ns.numprocesses),
        "--rxdist-engine",
        ns.engine,
        str(testpath),
    ]

    results: list[RunResult] = []
    for _ in range(ns.runs):
        r = _run_pytest(base)
        results.append(r)
        if r.returncode != 0:
            return r.returncode

    best = min(r.elapsed_s for r in results)
    avg = sum(r.elapsed_s for r in results) / len(results)
    print(f"suite={ns.suite} engine={ns.engine} runs={ns.runs} best={best:.3f}s avg={avg:.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

