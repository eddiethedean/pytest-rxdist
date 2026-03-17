# pytest-rxdist

**rxdist** is a planned drop-in replacement for `pytest-xdist`, aiming to make large pytest suites run significantly faster via a Rust-powered execution core, smarter scheduling, and faster inter-process communication (IPC).

The Python package name is **`pytest-rxdist`** (import as `pytest_rxdist`).

## Status

Milestone 0–1 are implemented: the repo ships a minimal pytest plugin plus a tiny Rust core wired in via **PyO3 + maturin**, and an MVP parallel runner.

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

Enable minimal debug output (reports whether the Rust extension loaded):

```bash
pytest -p pytest_rxdist --rxdist-debug
```

## What it aims to improve (vs `pytest-xdist`)

- **Scheduling efficiency**: predictive scheduling using historical runtimes to reduce idle workers
- **Lower overhead**: a Rust core to orchestrate workers efficiently
- **Faster IPC**: move beyond Python pickle toward binary / zero-copy options
- **Worker reuse**: “warm” workers with cached imports (and carefully controlled caching)
- **Scales up**: from laptop parallelism to multi-machine distributed execution

## Current limitations

- Worker execution is conservative: each test is executed via a subprocess `python -m pytest <nodeid>`.
- CLI compatibility is not complete yet: this MVP uses `--numprocesses` (not `-n`).
- Scheduling is baseline (shared work queue); smarter strategies come later.

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

