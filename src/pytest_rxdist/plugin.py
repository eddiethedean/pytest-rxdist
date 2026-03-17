from __future__ import annotations

import os


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
        help="Enable basic runtime profiling output (placeholder).",
    )
    group.addoption(
        "--rxdist-debug",
        action="store_true",
        default=False,
        help="Enable minimal debug output from pytest-rxdist (Milestone 0).",
    )


def pytest_sessionstart(session):
    config = session.config
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
    controller = RXDistController(
        num_workers=n,
        scheduler=config.getoption("rxdist_scheduler"),
        debug=bool(config.getoption("--rxdist-debug")),
    )
    results = controller.run(nodeids)

    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(f"pytest-rxdist: ran {len(results)} tests on {n} workers")

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

