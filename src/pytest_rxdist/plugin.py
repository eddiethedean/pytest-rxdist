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

