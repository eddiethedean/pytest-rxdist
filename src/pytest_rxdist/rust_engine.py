from __future__ import annotations

from typing import Any

from .core import _core


def run_session_rust(
    *,
    nodeids: list[str],
    units: list[list[str]] | None,
    num_workers: int,
    scheduler: str,
    reuse_mode: str,
    debug: bool,
) -> list[dict[str, Any]]:
    """
    Invoke the Rust engine if present.

    For now this is a thin shim; the Rust side can gradually take ownership of
    controller/scheduler/IPC while keeping the Python surface stable.
    """
    if _core is None:
        raise RuntimeError("Rust extension not available")

    # Units are optional; pass through for atomic cohort scheduling.
    return _core.run_session(
        nodeids,
        units,
        int(num_workers),
        str(scheduler),
        str(reuse_mode),
        bool(debug),
    )

