from __future__ import annotations

from typing import Final


try:
    from . import _core  # built by maturin / PyO3
except Exception:  # pragma: no cover
    _core = None


CORE_AVAILABLE: Final[bool] = _core is not None


def engine_version() -> str:
    if _core is None:
        raise RuntimeError(
            "pytest_rxdist_core is not available. "
            "Build/install the package so the Rust extension can be imported."
        )
    return _core.engine_version()


def hello(name: str) -> str:
    if _core is None:
        raise RuntimeError(
            "pytest_rxdist_core is not available. "
            "Build/install the package so the Rust extension can be imported."
        )
    return _core.hello(name)
