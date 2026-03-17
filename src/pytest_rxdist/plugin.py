from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path


def pytest_addoption(parser):
    group = parser.getgroup("pytest-rxdist")
    # Pytest reserves lowercase short options like -n for its own ecosystem.
    # For now, keep xdist-like semantics on a long option. We can revisit once we
    # decide whether to depend on pytest-xdist's option registration or mirror it
    # via a compat shim.
    group.addoption(
        "--numprocesses",
        action="store",
        default=None,
        metavar="NUM",
        help="Number of worker processes to spawn (or 'auto'). (Milestone 1 MVP)",
    )
    group.addoption(
        "--rxdist-scheduler",
        action="store",
        default="baseline",
        help="Scheduler strategy (baseline|smart).",
    )
    group.addoption(
        "--rxdist-profile",
        action="store_true",
        default=False,
        help="Enable timing persistence + summary output (Milestone 2).",
    )
    group.addoption(
        "--rxdist-reuse",
        action="store",
        default="safe",
        choices=["off", "safe", "aggressive"],
        help="Worker reuse mode (off|safe|aggressive). Default: safe. (Milestone 4)",
    )
    group.addoption(
        "--rxdist-ipc",
        action="store",
        default="baseline",
        choices=["baseline", "shm"],
        help="IPC transport mode (baseline|shm). Default: baseline. (Milestone 5)",
    )
    group.addoption(
        "--rxdist-ipc-batch-size",
        action="store",
        default="1",
        metavar="N",
        help="IPC batch size (number of nodeids per message). Default: 1. (Milestone 5)",
    )
    group.addoption(
        "--rxdist-fixture-grouping",
        action="store",
        default="off",
        choices=["off", "session"],
        help="Fixture-aware grouping strategy (off|session). Default: off. (Milestone 6)",
    )
    group.addoption(
        "--rxdist-fixture-grouping-max-cohort-size",
        action="store",
        default="50",
        metavar="N",
        help="Max tests per fixture cohort chunk. Default: 50. (Milestone 6)",
    )
    group.addoption(
        "--rxdist-debug",
        action="store_true",
        default=False,
        help="Enable minimal debug output from pytest-rxdist (Milestone 0).",
    )


def pytest_sessionstart(session):
    config = session.config
    if _is_worker_process():
        return

    if config.getoption("--rxdist-profile"):
        _maybe_print_timing_summary(config)

    if not config.getoption("--rxdist-debug"):
        return

    try:
        from .core import CORE_AVAILABLE, engine_version  # local import to stay lightweight

        if CORE_AVAILABLE:
            config.pluginmanager.get_plugin("terminalreporter").write_line(
                f"pytest-rxdist: rust_core=available version={engine_version()}"
            )
        else:
            config.pluginmanager.get_plugin("terminalreporter").write_line(
                "pytest-rxdist: rust_core=unavailable"
            )
    except Exception as e:  # pragma: no cover
        reporter = config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_line(f"pytest-rxdist: debug hook error: {e!r}")


def _is_worker_process() -> bool:
    return os.environ.get("PYTEST_RXDIST_WORKER") == "1"


def _parse_numprocesses(value: str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, str) and value.lower() == "auto":
        try:
            return max(1, (os.cpu_count() or 1))
        except Exception:
            return 1
    try:
        n = int(value)
    except Exception as e:  # pragma: no cover
        raise ValueError(f"invalid -n/--numprocesses value: {value!r}") from e
    if n < 0:
        raise ValueError("numprocesses must be >= 0")
    return n


def pytest_configure(config):
    # Workers must not recursively start more workers.
    if _is_worker_process():
        return

    raw = config.getoption("numprocesses")
    try:
        n = _parse_numprocesses(raw)
    except ValueError as e:
        raise config.UsageError(str(e))

    # Store parsed value for later hooks.
    config._rxdist_numprocesses = n  # type: ignore[attr-defined]

    # IPC flags are read by controller/worker via env vars (controller uses Path.cwd()).
    ipc = config.getoption("rxdist_ipc")
    batch_raw = config.getoption("rxdist_ipc_batch_size")
    os.environ["PYTEST_RXDIST_IPC"] = str(ipc)
    try:
        batch_size = max(1, int(batch_raw))
    except Exception:
        batch_size = 1
    os.environ["PYTEST_RXDIST_IPC_BATCH_SIZE"] = str(batch_size)

    # Fixture grouping (Milestone 6).
    grouping = (config.getoption("rxdist_fixture_grouping") or "off").strip().lower()
    if grouping not in ("off", "session"):
        grouping = "off"
    try:
        max_cohort_size = max(1, int(config.getoption("rxdist_fixture_grouping_max_cohort_size") or 50))
    except Exception:
        max_cohort_size = 50
    config._rxdist_fixture_grouping = grouping  # type: ignore[attr-defined]
    config._rxdist_fixture_grouping_max_cohort_size = max_cohort_size  # type: ignore[attr-defined]

    if config.getoption("--rxdist-profile"):
        config._rxdist_serial_recorder = _SerialTimingRecorder(config)  # type: ignore[attr-defined]
        config.pluginmanager.register(config._rxdist_serial_recorder, "rxdist_serial_timing")  # type: ignore[attr-defined]


def pytest_runtestloop(session):
    config = session.config
    if _is_worker_process():
        return None

    n = getattr(config, "_rxdist_numprocesses", None)
    if not n:
        return None

    from .controller import RXDistController

    items_by_nodeid = {item.nodeid: item for item in session.items}
    nodeids = list(items_by_nodeid.keys())

    grouping = getattr(config, "_rxdist_fixture_grouping", "off")
    max_cohort_size = int(getattr(config, "_rxdist_fixture_grouping_max_cohort_size", 50))
    units: list[list[str]] | None = None
    grouping_stats = None
    if grouping == "session":
        from .fixture_grouping import build_session_fixture_units, stats_for_units

        units = build_session_fixture_units(session.items, max_cohort_size=max_cohort_size)
        grouping_stats = stats_for_units(units, max_cohort_size=max_cohort_size)
    controller = RXDistController(
        num_workers=n,
        scheduler=config.getoption("rxdist_scheduler"),
        reuse_mode=config.getoption("rxdist_reuse"),
        debug=bool(config.getoption("--rxdist-debug")),
    )
    results = controller.run(nodeids, units=units)

    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(f"pytest-rxdist: ran {len(results)} tests on {n} workers")

    if config.getoption("--rxdist-debug") and reporter is not None and grouping_stats is not None:
        reporter.write_line(
            "pytest-rxdist: fixture_grouping=session "
            f"cohorts={grouping_stats.cohorts} grouped={grouping_stats.grouped_tests} "
            f"ungrouped={grouping_stats.ungrouped_tests} max_cohort_size={grouping_stats.max_cohort_size}"
        )

    if config.getoption("--rxdist-debug") and reporter is not None:
        ipc = config.getoption("rxdist_ipc")
        batch_raw = config.getoption("rxdist_ipc_batch_size")
        try:
            batch_size = int(batch_raw)
        except Exception:
            batch_size = 1
        if ipc == "shm":
            shm_used = sum(int(r.get("_ipc_shm_used") or 0) for r in results)
            inline_used = sum(int(r.get("_ipc_inline_used") or 0) for r in results)
            reporter.write_line(
                f"pytest-rxdist: ipc mode=shm batch_size={batch_size} shm_used={shm_used} inline_used={inline_used}"
            )

    if config.getoption("--rxdist-debug") and reporter is not None:
        sched = config.getoption("rxdist_scheduler")
        if sched == "smart" and getattr(controller, "last_schedule", None) is not None:
            s = controller.last_schedule
            reporter.write_line(
                "pytest-rxdist: smart_schedule "
                f"known={s.known_count} unknown={s.unknown_count} "
                f"est_makespan={s.estimated_makespan_s:.3f}s"
            )

    if config.getoption("--rxdist-profile"):
        _write_timing_run(config, results)

    # Reconstruct pytest reporting so terminal summary is correct.
    from _pytest.reports import TestReport

    def _make_report(nodeid: str, outcome: str, duration_s: float, longrepr: str | None) -> TestReport:
        item = items_by_nodeid.get(nodeid)
        if item is None:
            location = ("<unknown>", 0, nodeid)
            keywords = {}
        else:
            location = item.location
            keywords = dict(item.keywords)
        return TestReport(
            nodeid=nodeid,
            location=location,
            keywords=keywords,
            outcome=outcome,
            longrepr=longrepr,
            when="call",
            sections=[],
            duration=duration_s,
            user_properties=[],
        )

    # Best-effort: attach worker stdout/stderr to failures for attribution.
    for r in results:
        nodeid = str(r.get("nodeid"))
        outcome = str(r.get("outcome") or "failed")
        duration_s = float(r.get("duration_s") or 0.0)
        if outcome == "failed":
            out = r.get("stdout") or ""
            err = r.get("stderr") or ""
            longrepr = (out + ("\n" if out and err else "") + err).strip() or "worker reported failure"
        else:
            longrepr = None

        report = _make_report(nodeid, outcome if outcome != "xfailed" else "skipped", duration_s, longrepr)
        config.hook.pytest_runtest_logreport(report=report)

    failed = sum(1 for r in results if r.get("outcome") == "failed")
    session.testsfailed = failed
    # Returning True tells pytest we ran the loop ourselves.
    return True


def pytest_sessionfinish(session, exitstatus):
    config = session.config
    if _is_worker_process():
        return
    if not config.getoption("--rxdist-profile"):
        return
    # Serial mode: recorder accumulates results and writes at session end.
    recorder = getattr(config, "_rxdist_serial_recorder", None)
    if recorder is not None:
        recorder.write()


@dataclass
class _TestTiming:
    nodeid: str
    duration_s: float
    outcome: str


class _SerialTimingRecorder:
    def __init__(self, config):
        self._config = config
        self._started_at = time.time()
        self._results: list[_TestTiming] = []

    def pytest_runtest_logreport(self, report):
        if report.when != "call":
            return
        self._results.append(
            _TestTiming(
                nodeid=report.nodeid,
                duration_s=float(getattr(report, "duration", 0.0) or 0.0),
                outcome=str(getattr(report, "outcome", "unknown") or "unknown"),
            )
        )

    def write(self) -> None:
        if not self._results:
            return
        results = [{"nodeid": r.nodeid, "duration_s": r.duration_s, "outcome": r.outcome} for r in self._results]
        _write_timing_run(self._config, results, started_at=self._started_at)


def _project_root(config) -> Path:
    try:
        return Path(str(config.rootpath))
    except Exception:
        return Path.cwd()


def _maybe_print_timing_summary(config) -> None:
    from .timing_store import TimingStore, default_timings_path

    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if reporter is None:
        return

    path = default_timings_path(_project_root(config))
    if not path.exists():
        return

    store = TimingStore.open(path)
    try:
        n = store.count_tests()
        rows = store.summary(limit=5)
    finally:
        store.close()

    reporter.write_line(f"pytest-rxdist: timings loaded ({n} tests) from {path}")
    for r in rows:
        reporter.write_line(f"  slow: {r.avg_duration_s:.3f}s avg ({r.count} runs) {r.nodeid}")


def _write_timing_run(config, results: list[dict], started_at: float | None = None) -> None:
    from . import __version__
    from .timing_store import TimingStore, default_timings_path, env_fingerprint

    path = default_timings_path(_project_root(config))
    store = TimingStore.open(path)
    try:
        store.write_run(
            started_at=float(started_at if started_at is not None else time.time()),
            env_fp=env_fingerprint(),
            rxdist_version=__version__,
            results=results,
        )
    finally:
        store.close()

