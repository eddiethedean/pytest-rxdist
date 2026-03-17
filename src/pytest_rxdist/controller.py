from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .ipc import iter_messages, send_message
from .scheduler import SmartSchedule, smart_schedule
from .timing_store import TimingStore, default_timings_path


@dataclass(frozen=True)
class WorkerProcess:
    proc: subprocess.Popen[str]
    idx: int


class RXDistController:
    def __init__(self, *, num_workers: int, scheduler: str = "baseline", debug: bool = False):
        self.num_workers = max(1, int(num_workers))
        self.scheduler = scheduler
        self.debug = debug

        self.last_schedule: SmartSchedule | None = None

    def _spawn_worker(self, idx: int) -> WorkerProcess:
        env = dict(os.environ)
        env["PYTEST_RXDIST_WORKER"] = "1"
        cmd = [sys.executable, "-m", "pytest_rxdist._worker_main"]
        proc = subprocess.Popen(
            cmd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
        )
        assert proc.stdin is not None
        assert proc.stdout is not None
        return WorkerProcess(proc=proc, idx=idx)

    def run(self, nodeids: list[str]) -> list[dict[str, Any]]:
        if not nodeids:
            return []

        results: list[dict[str, Any]] = []
        results_lock = threading.Lock()

        workers = [self._spawn_worker(i) for i in range(self.num_workers)]

        def wait_hello(w: WorkerProcess) -> None:
            assert w.proc.stdin is not None
            assert w.proc.stdout is not None

            # Wait for hello.
            for msg in iter_messages(w.proc.stdout):
                if msg.type == "hello":
                    break

        for w in workers:
            wait_hello(w)

        if self.scheduler == "smart":
            # Build a predictive schedule using historical timings.
            timings_path = default_timings_path(Path.cwd())
            avg: dict[str, float] = {}
            if timings_path.exists():
                store = TimingStore.open(timings_path)
                try:
                    avg = store.avg_durations(nodeids)
                finally:
                    store.close()

            schedule = smart_schedule(nodeids, num_workers=self.num_workers, avg_durations_s=avg)
            self.last_schedule = schedule

            def run_worker_queue(w: WorkerProcess, queue_nodeids: list[str]) -> None:
                assert w.proc.stdin is not None
                assert w.proc.stdout is not None
                for nodeid in queue_nodeids:
                    send_message(w.proc.stdin, "run", {"nodeid": nodeid})
                    for msg in iter_messages(w.proc.stdout):
                        if msg.type == "result" and msg.payload.get("nodeid") == nodeid:
                            with results_lock:
                                results.append(msg.payload)
                            break
                send_message(w.proc.stdin, "shutdown", {})

            threads = [
                threading.Thread(
                    target=run_worker_queue,
                    args=(w, schedule.per_worker[w.idx]),
                    daemon=True,
                )
                for w in workers
            ]
        else:
            # Baseline load-based via a shared work queue.
            work_q: queue.Queue[str] = queue.Queue()
            for nid in nodeids:
                work_q.put(nid)

            def worker_thread(w: WorkerProcess) -> None:
                assert w.proc.stdin is not None
                assert w.proc.stdout is not None
                while True:
                    try:
                        nodeid = work_q.get_nowait()
                    except queue.Empty:
                        send_message(w.proc.stdin, "shutdown", {})
                        return

                    send_message(w.proc.stdin, "run", {"nodeid": nodeid})

                    for msg in iter_messages(w.proc.stdout):
                        if msg.type == "result" and msg.payload.get("nodeid") == nodeid:
                            with results_lock:
                                results.append(msg.payload)
                            break

            threads = [threading.Thread(target=worker_thread, args=(w,), daemon=True) for w in workers]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for w in workers:
            if w.proc.poll() is None:
                w.proc.terminate()
            try:
                w.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                w.proc.kill()
                w.proc.wait(timeout=5)

        return results

