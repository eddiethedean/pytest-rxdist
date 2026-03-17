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
from .scheduler import SmartSchedule, smart_schedule, smart_schedule_units
from .timing_store import TimingStore, default_timings_path
from .shm import ShmTextRef, cleanup_shm, read_text_from_shm


@dataclass
class WorkerProcess:
    proc: subprocess.Popen[str]
    idx: int


class RXDistController:
    def __init__(
        self,
        *,
        num_workers: int,
        scheduler: str = "baseline",
        reuse_mode: str = "safe",
        debug: bool = False,
    ):
        self.num_workers = max(1, int(num_workers))
        self.scheduler = scheduler
        self.reuse_mode = reuse_mode
        self.debug = debug

        self.last_schedule: SmartSchedule | None = None

        self.ipc_mode = (os.environ.get("PYTEST_RXDIST_IPC") or "baseline").strip().lower()
        try:
            self.ipc_batch_size = int(os.environ.get("PYTEST_RXDIST_IPC_BATCH_SIZE") or 1)
        except Exception:
            self.ipc_batch_size = 1

    def _spawn_worker(self, idx: int) -> WorkerProcess:
        env = dict(os.environ)
        env["PYTEST_RXDIST_WORKER"] = "1"
        env["PYTEST_RXDIST_REUSE"] = self.reuse_mode
        env["PYTEST_RXDIST_IPC"] = self.ipc_mode
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

    def run(self, nodeids: list[str], *, units: list[list[str]] | None = None) -> list[dict[str, Any]]:
        if not nodeids:
            return []

        results: list[dict[str, Any]] = []
        results_lock = threading.Lock()

        if units is not None:
            # Use units as the source-of-truth ordering, but keep nodeids for reporting.
            flat: list[str] = []
            for u in units:
                for nid in u:
                    flat.append(str(nid))
            nodeids = flat

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

        def record_worker_failure(nodeid: str, why: str) -> None:
            with results_lock:
                results.append(
                    {
                        "nodeid": nodeid,
                        "outcome": "failed",
                        "duration_s": 0.0,
                        "returncode": 1,
                        "stdout": "",
                        "stderr": why,
                    }
                )

        def wait_result_for_nodeid(w: WorkerProcess, nodeid: str) -> bool:
            """Return True if result received, False if worker died/EOF."""
            assert w.proc.stdout is not None
            try:
                for msg in iter_messages(w.proc.stdout):
                    if msg.type == "result" and msg.payload.get("nodeid") == nodeid:
                        with results_lock:
                            results.append(decode_result_payload(msg.payload))
                        return True
            except Exception:
                return False
            return False

        def decode_result_payload(payload: dict[str, Any]) -> dict[str, Any]:
            if self.ipc_mode != "shm":
                return payload
            out = dict(payload)
            shm_used = 0
            inline_used = 0
            for key in ("stdout_blob", "stderr_blob"):
                blob = out.get(key)
                if not isinstance(blob, dict):
                    continue
                kind = blob.get("kind")
                if kind == "inline":
                    inline_used += 1
                    text = blob.get("text") or ""
                elif kind == "shm":
                    shm_used += 1
                    ref = ShmTextRef(
                        kind="shm",
                        name=str(blob.get("name")),
                        size=int(blob.get("size") or 0),
                        encoding=str(blob.get("encoding") or "utf-8"),
                    )
                    try:
                        text = read_text_from_shm(ref)
                    finally:
                        cleanup_shm(ref)
                else:
                    text = ""

                if key == "stdout_blob":
                    out["stdout"] = text
                else:
                    out["stderr"] = text

            out["_ipc_shm_used"] = shm_used
            out["_ipc_inline_used"] = inline_used
            return out

        def wait_one_or_batch_results(w: WorkerProcess, expected_nodeids: list[str]) -> bool:
            """Wait for result(s) for nodeids. Returns False on EOF/death."""
            assert w.proc.stdout is not None
            want = set(expected_nodeids)
            try:
                for msg in iter_messages(w.proc.stdout):
                    if msg.type == "result":
                        nid = msg.payload.get("nodeid")
                        if nid in want:
                            payload = decode_result_payload(msg.payload)
                            with results_lock:
                                results.append(payload)
                            want.remove(nid)
                            if not want:
                                return True
                    if msg.type == "results_batch":
                        rs = msg.payload.get("results") or []
                        if isinstance(rs, list):
                            for r in rs:
                                if not isinstance(r, dict):
                                    continue
                                nid = r.get("nodeid")
                                if nid in want:
                                    payload = decode_result_payload(r)
                                    with results_lock:
                                        results.append(payload)
                                    want.remove(nid)
                            if not want:
                                return True
            except Exception:
                return False
            return False

        def respawn_worker(w: WorkerProcess) -> WorkerProcess:
            # Replace a dead/bad worker with a fresh one (safe mode best-effort).
            try:
                if w.proc.poll() is None:
                    w.proc.terminate()
            except Exception:
                pass
            new_w = self._spawn_worker(w.idx)
            wait_hello(new_w)
            return new_w

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

            if units is not None:
                schedule = smart_schedule_units(units, num_workers=self.num_workers, avg_durations_s=avg)
            else:
                schedule = smart_schedule(nodeids, num_workers=self.num_workers, avg_durations_s=avg)
            self.last_schedule = schedule

            def run_worker_queue(w: WorkerProcess, queue_nodeids: list[str]) -> None:
                w_local = w
                assert w_local.proc.stdin is not None
                assert w_local.proc.stdout is not None
                respawned = False
                i = 0
                while i < len(queue_nodeids):
                    batch = queue_nodeids[i : i + max(1, self.ipc_batch_size)]
                    try:
                        if len(batch) == 1:
                            send_message(w_local.proc.stdin, "run", {"nodeid": batch[0]})
                        else:
                            send_message(w_local.proc.stdin, "run_batch", {"nodeids": batch})
                    except Exception:
                        record_worker_failure(batch[0], "worker died before receiving work")
                        for remaining in queue_nodeids[i + 1 :]:
                            record_worker_failure(remaining, "worker died before running test")
                        break

                    ok = wait_one_or_batch_results(w_local, batch)
                    if not ok:
                        if self.reuse_mode == "safe" and not respawned:
                            respawned = True
                            w_local = respawn_worker(w_local)
                            assert w_local.proc.stdin is not None
                            assert w_local.proc.stdout is not None
                            # Retry current batch as single nodeid (simpler).
                            try:
                                send_message(w_local.proc.stdin, "run", {"nodeid": batch[0]})
                            except Exception:
                                ok2 = False
                            else:
                                ok2 = wait_one_or_batch_results(w_local, [batch[0]])
                            if ok2:
                                i += 1
                                continue
                        record_worker_failure(batch[0], "worker died before reporting result")
                        for remaining in queue_nodeids[i + 1 :]:
                            record_worker_failure(remaining, "worker died before running test")
                        break
                    i += len(batch)
                if w_local.proc.stdin is not None:
                    try:
                        send_message(w_local.proc.stdin, "shutdown", {})
                    except Exception:
                        pass

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
            work_q: queue.Queue[list[str]] = queue.Queue()
            if units is not None:
                for u in units:
                    work_q.put([str(x) for x in u])
            else:
                for nid in nodeids:
                    work_q.put([str(nid)])

            def worker_thread(w: WorkerProcess) -> None:
                w_local = w
                assert w_local.proc.stdin is not None
                assert w_local.proc.stdout is not None
                respawned = False
                while True:
                    try:
                        unit = work_q.get_nowait()
                    except queue.Empty:
                        send_message(w_local.proc.stdin, "shutdown", {})
                        return

                    i = 0
                    while i < len(unit):
                        batch = unit[i : i + max(1, self.ipc_batch_size)]
                        if len(batch) == 1:
                            try:
                                send_message(w_local.proc.stdin, "run", {"nodeid": batch[0]})
                            except Exception:
                                record_worker_failure(batch[0], "worker died before receiving work")
                                while True:
                                    try:
                                        remaining_unit = work_q.get_nowait()
                                    except queue.Empty:
                                        break
                                    for remaining in remaining_unit:
                                        record_worker_failure(remaining, "worker died before running test")
                                for remaining in unit[i + 1 :]:
                                    record_worker_failure(remaining, "worker died before running test")
                                return
                        else:
                            try:
                                send_message(w_local.proc.stdin, "run_batch", {"nodeids": batch})
                            except Exception:
                                record_worker_failure(batch[0], "worker died before receiving work")
                                while True:
                                    try:
                                        remaining_unit = work_q.get_nowait()
                                    except queue.Empty:
                                        break
                                    for remaining in remaining_unit:
                                        record_worker_failure(remaining, "worker died before running test")
                                for remaining in unit[i + 1 :]:
                                    record_worker_failure(remaining, "worker died before running test")
                                return

                        ok = wait_one_or_batch_results(w_local, batch)
                        if not ok:
                            if self.reuse_mode == "safe" and not respawned:
                                respawned = True
                                w_local = respawn_worker(w_local)
                                assert w_local.proc.stdin is not None
                                assert w_local.proc.stdout is not None
                                try:
                                    send_message(w_local.proc.stdin, "run", {"nodeid": batch[0]})
                                except Exception:
                                    ok2 = False
                                else:
                                    ok2 = wait_one_or_batch_results(w_local, [batch[0]])
                                if ok2:
                                    i += 1
                                    continue
                            record_worker_failure(batch[0], "worker died before reporting result")
                            while True:
                                try:
                                    remaining_unit = work_q.get_nowait()
                                except queue.Empty:
                                    break
                                for remaining in remaining_unit:
                                    record_worker_failure(remaining, "worker died before running test")
                            for remaining in unit[i + 1 :]:
                                record_worker_failure(remaining, "worker died before running test")
                            return
                        i += len(batch)

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

