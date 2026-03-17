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


def test_reuse_off_isolates_module_globals_better_than_safe():
    # In safe mode, tests run in-process in a warm worker and can leak module globals.
    # In off mode, each nodeid runs via a separate python -m pytest invocation (more isolated).
    mod = Path("tests/_rxdist_tmp_globals.py")
    mod.write_text(
        "STATE = 0\n\n"
        "def test_set_state():\n"
        "    global STATE\n"
        "    STATE = 1\n"
        "    assert STATE == 1\n\n"
        "def test_state_is_zero_initially():\n"
        "    # If the module is reused in-process, STATE may be 1.\n"
        "    assert STATE == 0\n",
        encoding="utf-8",
    )

    try:
        # Safe mode may fail due to leakage (acceptable/expected limitation to document).
        safe = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "1", "--rxdist-reuse", "safe", str(mod)]
        )

        off = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "1", "--rxdist-reuse", "off", str(mod)]
        )
        assert off.returncode == 0, off.stdout + "\n" + off.stderr

        # Ensure the test file executed (avoid false positives).
        assert "2 passed" in (off.stdout + "\n" + off.stderr)
        assert safe.returncode in (0, 1)
    finally:
        try:
            mod.unlink()
        except OSError:
            pass


def test_fixture_heavy_suite_runs_under_reuse_safe(tmp_path: Path):
    # Exercise session/module/function fixtures, autouse fixtures, and parametrization.
    suite = Path("tests/_rxdist_tmp_fixtures_suite.py")
    conftest = Path("tests/conftest_rxdist_tmp.py")
    try:
        conftest.write_text(
            "import pytest\n\n"
            "@pytest.fixture(scope='session')\n"
            "def sess(tmp_path_factory):\n"
            "    return tmp_path_factory.mktemp('sess')\n\n"
            "@pytest.fixture(scope='module')\n"
            "def mod():\n"
            "    return {'x': 1}\n\n"
            "@pytest.fixture(autouse=True)\n"
            "def auto_env(monkeypatch):\n"
            "    monkeypatch.setenv('RXDIST_TMP', '1')\n"
            "    yield\n",
            encoding="utf-8",
        )
        suite.write_text(
            "import os\n"
            "import pytest\n\n"
            "def test_autouse_env():\n"
            "    assert os.environ.get('RXDIST_TMP') == '1'\n\n"
            "@pytest.mark.parametrize('v', [0,1,2,3])\n"
            "def test_param(v, mod, sess):\n"
            "    assert mod['x'] == 1\n"
            "    assert sess.exists()\n"
            "    assert v in [0,1,2,3]\n",
            encoding="utf-8",
        )

        # Note: we explicitly include conftest via PYTEST_ADDOPTS so it is loaded.
        env = {"PYTEST_ADDOPTS": "-p tests.conftest_rxdist_tmp"}
        p = _run_pytest(
            [
                "-p",
                "pytest_rxdist",
                "-q",
                "--numprocesses",
                "2",
                "--rxdist-reuse",
                "safe",
                str(suite),
            ],
            env=env,
        )
        assert p.returncode == 0, p.stdout + "\n" + p.stderr
    finally:
        for f in (suite, conftest):
            try:
                f.unlink()
            except OSError:
                pass


def test_stress_many_tests_under_smart_scheduler_and_reuse_safe():
    # Generate a moderate number of tests to exercise scheduling, IPC, and warm reuse.
    mod = Path("tests/_rxdist_tmp_stress.py")
    parts = ["import time\n\n"]
    for i in range(60):
        # Mix of short sleeps to create timing variability without making the suite too slow.
        sleep = 0.001 if i % 3 else 0.003
        parts.append(f"def test_{i}():\n    time.sleep({sleep})\n    assert True\n\n")
    mod.write_text("".join(parts), encoding="utf-8")

    try:
        p = _run_pytest(
            [
                "-p",
                "pytest_rxdist",
                "-q",
                "--numprocesses",
                "3",
                "--rxdist-reuse",
                "safe",
                "--rxdist-scheduler",
                "smart",
                str(mod),
            ]
        )
        assert p.returncode == 0, p.stdout + "\n" + p.stderr
    finally:
        try:
            mod.unlink()
        except OSError:
            pass


def test_failure_mode_worker_hard_exit_is_reported():
    # Simulate a worker hard-exiting after first test is run (existing crash hook).
    mod = Path("tests/_rxdist_tmp_exit.py")
    mod.write_text(
        "def test_ok1():\n    assert True\n\n"
        "def test_ok2():\n    assert True\n\n"
        "def test_ok3():\n    assert True\n",
        encoding="utf-8",
    )
    try:
        p = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "2", "--rxdist-reuse", "off", str(mod)],
            env={"PYTEST_RXDIST_WORKER_CRASH_AFTER": "1"},
        )
        assert p.returncode != 0
        assert "worker died" in (p.stdout + "\n" + p.stderr).lower()
    finally:
        try:
            mod.unlink()
        except OSError:
            pass


def test_timeout_like_behavior_does_not_hang_controller():
    # We don't have a real timeout feature yet, but ensure a long test doesn't hang the controller.
    # Keep it short enough for CI.
    mod = Path("tests/_rxdist_tmp_long.py")
    mod.write_text(
        "import time\n\n"
        "def test_long():\n"
        "    time.sleep(0.2)\n"
        "    assert True\n",
        encoding="utf-8",
    )
    try:
        start = time.perf_counter()
        p = _run_pytest(
            ["-p", "pytest_rxdist", "-q", "--numprocesses", "1", "--rxdist-reuse", "safe", str(mod)]
        )
        elapsed = time.perf_counter() - start
        assert p.returncode == 0, p.stdout + "\n" + p.stderr
        assert elapsed < 5.0
    finally:
        try:
            mod.unlink()
        except OSError:
            pass

