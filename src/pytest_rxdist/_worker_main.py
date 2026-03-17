from __future__ import annotations

import os
import sys
import time

from .ipc import iter_messages, send_message
from .worker import run_one
from .shm import DEFAULT_THRESHOLD_BYTES, write_text_to_shm


def main() -> int:
    inp = sys.stdin.buffer
    out = sys.stdout.buffer

    send_message(out, "hello", {"pid": os.getpid(), "ts": time.time()})

    ipc_mode = (os.environ.get("PYTEST_RXDIST_IPC") or "baseline").strip().lower()
    try:
        shm_threshold = int(os.environ.get("PYTEST_RXDIST_SHM_THRESHOLD_BYTES") or DEFAULT_THRESHOLD_BYTES)
    except Exception:
        shm_threshold = DEFAULT_THRESHOLD_BYTES

    crash_after = os.environ.get("PYTEST_RXDIST_WORKER_CRASH_AFTER")
    crash_after_n: int | None
    if crash_after is None:
        crash_after_n = None
    else:
        try:
            crash_after_n = int(crash_after)
        except Exception:
            crash_after_n = None

    for msg in iter_messages(inp):
        if msg.type == "shutdown":
            return 0
        if msg.type == "run":
            nodeid = str(msg.payload.get("nodeid"))
            r = run_one(nodeid)
            payload = {
                "nodeid": r.nodeid,
                "outcome": r.outcome,
                "duration_s": r.duration_s,
                "returncode": r.returncode,
            }
            if ipc_mode == "shm":
                payload["stdout_blob"] = _blobify_text(r.stdout, shm_threshold)
                payload["stderr_blob"] = _blobify_text(r.stderr, shm_threshold)
            else:
                payload["stdout"] = r.stdout
                payload["stderr"] = r.stderr
            send_message(out, "result", payload)
            if crash_after_n is not None:
                crash_after_n -= 1
                if crash_after_n <= 0:
                    os._exit(137)
        if msg.type == "run_batch":
            nodeids = list(msg.payload.get("nodeids") or [])
            results: list[dict] = []
            for nodeid in nodeids:
                r = run_one(str(nodeid))
                payload = {
                    "nodeid": r.nodeid,
                    "outcome": r.outcome,
                    "duration_s": r.duration_s,
                    "returncode": r.returncode,
                }
                if ipc_mode == "shm":
                    payload["stdout_blob"] = _blobify_text(r.stdout, shm_threshold)
                    payload["stderr_blob"] = _blobify_text(r.stderr, shm_threshold)
                else:
                    payload["stdout"] = r.stdout
                    payload["stderr"] = r.stderr
                results.append(payload)
            send_message(out, "results_batch", {"results": results})

    return 0


def _blobify_text(text: str, threshold: int) -> dict:
    if not text:
        return {"kind": "inline", "text": ""}
    encoded_len = len(text.encode("utf-8", errors="replace"))
    if encoded_len < threshold:
        return {"kind": "inline", "text": text}
    ref = write_text_to_shm(text)
    return {"kind": "shm", "name": ref.name, "size": ref.size, "encoding": ref.encoding}


if __name__ == "__main__":
    raise SystemExit(main())

