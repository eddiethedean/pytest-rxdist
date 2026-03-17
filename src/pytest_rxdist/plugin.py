from __future__ import annotations


def pytest_addoption(parser):
    group = parser.getgroup("pytest-rxdist")
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

