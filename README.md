# pytest-rxdist

**rxdist** is a planned drop-in replacement for `pytest-xdist`, aiming to make large pytest suites run significantly faster via a Rust-powered execution core, smarter scheduling, and faster inter-process communication (IPC).

The Python package name is **`pytest-rxdist`** (import as `pytest_rxdist`).

## Status

Milestone 0–5 are implemented: the repo ships a minimal pytest plugin plus a tiny Rust core wired in via **PyO3 + maturin**, an MVP parallel runner, a local timing store foundation, a timing-informed smart scheduler, warm worker reuse, and optional shared-memory IPC for large payloads.

Published on PyPI as `pytest-rxdist` (currently an early, experimental build).

## Requirements

- **Python**: >= 3.10
- **Rust (for source builds)**: >= 1.85 (edition 2024 / MSRV 1.85)

## Install

From PyPI:

```bash
python -m pip install pytest-rxdist
```

From source (editable):

```bash
python -m pip install -e .
```

## Usage (Milestone 1)

Run normally (plugin is available via `pytest11` entrypoint), or force-load it:

```bash
pytest -p pytest_rxdist
```

Run in parallel (MVP):

```bash
pytest -p pytest_rxdist --numprocesses 4
pytest -p pytest_rxdist --numprocesses auto
```

Enable warm worker reuse (Milestone 4):

```bash
pytest -p pytest_rxdist --numprocesses auto --rxdist-reuse safe
pytest -p pytest_rxdist --numprocesses auto --rxdist-reuse off
```

Enable smart scheduling (Milestone 3):

```bash
pytest -p pytest_rxdist --numprocesses auto --rxdist-scheduler smart
```

Enable timing persistence + summary output (Milestone 2):

```bash
pytest -p pytest_rxdist --rxdist-profile
```

Enable minimal debug output (reports whether the Rust extension loaded):

```bash
pytest -p pytest_rxdist --rxdist-debug
```

## IPC options (Milestone 5)

- `--rxdist-ipc baseline` (default): inline stdout/stderr in IPC messages.
- `--rxdist-ipc shm`: store large stdout/stderr payloads in shared memory and pass references over IPC.
- `--rxdist-ipc-batch-size N`: send/receive batches of nodeids/results to reduce IPC overhead (default 1).

## What it aims to improve (vs `pytest-xdist`)

- **Scheduling efficiency**: predictive scheduling using historical runtimes to reduce idle workers
- **Lower overhead**: a Rust core to orchestrate workers efficiently
- **Faster IPC**: move beyond Python pickle toward binary / zero-copy options
- **Worker reuse**: “warm” workers with cached imports (and carefully controlled caching)

## Current limitations

- CLI compatibility is not complete yet: this MVP uses `--numprocesses` (not `-n`).
- Smart scheduling uses historical avg durations when available, and falls back gracefully when timings are missing.
- In-process reuse means Python process state can persist across tests within a worker (similar to normal pytest behavior in a single process). Use `--rxdist-reuse off` if you need stricter isolation.

## Worker reuse (Milestone 4)

- `--rxdist-reuse safe` (default): reuse long-lived workers and run nodeids in-process to reduce import/startup overhead.
- `--rxdist-reuse off`: fallback mode that runs each nodeid in a separate `python -m pytest <nodeid>` invocation (slower, more isolated).

## Timing store (Milestone 2)

- **Default location**: `.pytest_rxdist/timings.sqlite3` under the pytest root directory.
- **Reset**: delete `.pytest_rxdist/timings.sqlite3`.
- **Corruption recovery**: if the DB is invalid, it is rotated to `timings.sqlite3.corrupt.<timestamp>` and rebuilt.

## Smart scheduler (Milestone 3)

- Uses historical timings to predict test duration (average duration per `nodeid`).
- Unknown-duration tests are interleaved and the scheduler falls back safely when timings are absent/partial.

## Planned CLI (future)

```bash
pytest -p pytest_rxdist -n auto
pytest --rxdist-scheduler=smart
pytest --rxdist-profile=on
```

## Docs

- **Roadmap**: `ROADMAP.md`

## Repository notes

- `planning/` is intentionally ignored (see `.gitignore`) so you can iterate on internal notes without committing them.

