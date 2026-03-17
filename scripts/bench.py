from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class RunResult:
    suite: str
    engine: str
    worker: str
    numprocesses: str
    ipc_mode: str
    batch_size: int
    reuse_mode: str
    label: str
    returncode: int
    elapsed_s: float


def _run_pytest(
    *,
    suite: str,
    engine: str,
    worker: str,
    numprocesses: str,
    testpath: Path,
    runs: int,
    ipc_mode: str,
    batch_size: int,
    reuse_mode: str,
    profile_cmd: str | None = None,
) -> list[RunResult]:
    """
    Run pytest for a given configuration, optionally under an external profiler.

    If profile_cmd is provided, it should be a format string where `{cmd}` will be
    replaced with the underlying pytest invocation (joined as a shell string),
    for example:

        profile_cmd=\"perf record -- {cmd}\"
    """
    base_args = [
        "-p",
        "pytest_rxdist",
        "-q",
        "--numprocesses",
        str(numprocesses),
        "--rxdist-engine",
        engine,
        "--rxdist-worker",
        worker,
        str(testpath),
    ]

    results: list[RunResult] = []
    for _ in range(runs):
        merged_env = dict(os.environ)
        merged_env.setdefault("PYTEST_RXDIST_IPC", ipc_mode)
        merged_env.setdefault("PYTEST_RXDIST_IPC_BATCH_SIZE", str(batch_size))
        merged_env.setdefault("PYTEST_RXDIST_REUSE", reuse_mode)

        cmd = [sys.executable, "-m", "pytest", *base_args]
        if profile_cmd:
            shell_cmd = " ".join(subprocess.list2cmdline([c]) for c in cmd)
            full_cmd = profile_cmd.format(cmd=shell_cmd)
            run_cmd: Iterable[str] | str = full_cmd
            shell = True
        else:
            run_cmd = cmd
            shell = False

        start = time.perf_counter()
        p = subprocess.run(
            run_cmd,
            env=merged_env,
            capture_output=True,
            text=True,
            shell=shell,  # type: ignore[arg-type]
        )
        elapsed = time.perf_counter() - start
        label = " ".join(base_args)
        if p.returncode != 0:
            sys.stderr.write(p.stdout + "\n" + p.stderr + "\n")
        results.append(
            RunResult(
                suite=suite,
                engine=engine,
                worker=worker,
                numprocesses=str(numprocesses),
                ipc_mode=ipc_mode,
                batch_size=batch_size,
                reuse_mode=reuse_mode,
                label=label,
                returncode=p.returncode,
                elapsed_s=elapsed,
            )
        )
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", choices=["tiny", "fixture", "mixed"], default="tiny")
    ap.add_argument("--numprocesses", default="auto")
    ap.add_argument("--engine", choices=["python", "rust"], default="python")
    ap.add_argument("--worker", choices=["python", "rust"], default="python")
    ap.add_argument(
        "--matrix",
        action="store_true",
        help="Run the full engine/worker matrix (python/python, rust/python, rust/rust) for the suite.",
    )
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument(
        "--ipc-mode",
        default=os.environ.get("PYTEST_RXDIST_IPC", "baseline"),
        help="IPC mode to use (default: %(default)s).",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("PYTEST_RXDIST_IPC_BATCH_SIZE", "1")),
        help="IPC batch size (default: %(default)s).",
    )
    ap.add_argument(
        "--reuse-mode",
        default=os.environ.get("PYTEST_RXDIST_REUSE", "safe"),
        help="Worker reuse mode (default: %(default)s).",
    )
    ap.add_argument(
        "--profile-cmd",
        default=None,
        help="Optional external profiler command; use `{cmd}` as placeholder for the pytest command.",
    )
    ns = ap.parse_args(argv)

    repo = Path(__file__).resolve().parents[1]
    if ns.suite == "tiny":
        testpath = repo / "tests" / "bench_many_tiny.py"
    elif ns.suite == "fixture":
        testpath = repo / "tests" / "bench_fixture_heavy.py"
    else:
        testpath = repo / "tests" / "bench_mixed_duration.py"

    configs: list[tuple[str, str]] = []
    if ns.matrix:
        configs = [
            ("python", "python"),
            ("rust", "python"),
            ("rust", "rust"),
        ]
    else:
        configs = [(ns.engine, ns.worker)]

    all_results: list[RunResult] = []
    for engine, worker in configs:
        results = _run_pytest(
            suite=ns.suite,
            engine=engine,
            worker=worker,
            numprocesses=str(ns.numprocesses),
            testpath=testpath,
            runs=ns.runs,
            ipc_mode=ns.ipc_mode,
            batch_size=ns.batch_size,
            reuse_mode=ns.reuse_mode,
            profile_cmd=ns.profile_cmd,
        )
        for r in results:
            if r.returncode != 0:
                return r.returncode
        all_results.extend(results)

    # Summarize per (engine, worker) configuration.
    by_cfg: dict[tuple[str, str], list[RunResult]] = {}
    for r in all_results:
        by_cfg.setdefault((r.engine, r.worker), []).append(r)

    for (engine, worker), rs in by_cfg.items():
        best = min(x.elapsed_s for x in rs)
        avg = sum(x.elapsed_s for x in rs) / len(rs)
        print(
            "suite={suite} engine={engine} worker={worker} runs={runs} "
            "numprocesses={numprocesses} ipc={ipc_mode} batch_size={batch_size} reuse={reuse} "
            "best={best:.3f}s avg={avg:.3f}s".format(
                suite=ns.suite,
                engine=engine,
                worker=worker,
                runs=len(rs),
                numprocesses=ns.numprocesses,
                ipc_mode=ns.ipc_mode,
                batch_size=ns.batch_size,
                reuse=ns.reuse_mode,
                best=best,
                avg=avg,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

