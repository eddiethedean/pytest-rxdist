from __future__ import annotations

import os
import sys
import time

from .ipc import iter_messages, send_message
from .worker import run_one


def main() -> int:
    inp = sys.stdin.buffer
    out = sys.stdout.buffer

    send_message(out, "hello", {"pid": os.getpid(), "ts": time.time()})

    for msg in iter_messages(inp):
        if msg.type == "shutdown":
            return 0
        if msg.type == "run":
            nodeid = str(msg.payload.get("nodeid"))
            r = run_one(nodeid)
            send_message(
                out,
                "result",
                {
                    "nodeid": r.nodeid,
                    "outcome": r.outcome,
                    "duration_s": r.duration_s,
                    "returncode": r.returncode,
                    "stdout": r.stdout,
                    "stderr": r.stderr,
                },
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

