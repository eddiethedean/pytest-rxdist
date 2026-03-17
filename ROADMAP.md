# rxdist Roadmap (Build Plan)

## Goals and constraints

- **Goal**: a drop-in replacement for `pytest-xdist` with a Rust-powered execution core, smarter scheduling, faster IPC, and optional distributed execution.
- **Constraints**: preserve pytest plugin compatibility, keep configuration minimal, and make performance wins measurable.

## High-level architecture (target)

- **Python layer (plugin/package)**
  - Registers pytest hooks, CLI options, and integrates with `pytest -n ...` workflows.
  - Translates pytest “collection + execution events” into messages for the core.
- **Rust core (engine)**
  - Worker pool manager, scheduler(s), IPC transport, profiling/timing store.
- **Bridge**
  - PyO3 bindings (initially), with an eye toward minimizing cross-language call frequency.

## Milestones

### Milestone 0 — Project skeleton + compatibility baseline

**Outcome**: a repo that builds a Python package with a minimal pytest plugin, and a Rust library wired via PyO3/maturin.

- **Progress**
  - [x] Python package `pytest-rxdist` (import `pytest_rxdist`) skeleton exists
  - [x] `pytest11` entrypoint configured (`pytest_rxdist = "pytest_rxdist.plugin"`)
  - [x] Switch build backend to `maturin` and configure `module-name = "pytest_rxdist_core"`
  - [x] Add Rust crate under `rust/` with `edition = "2024"` and `rust-version = "1.85"`
  - [x] Add a thin PyO3 surface (`engine_version`, `hello`)
  - [x] Add Python bridge module `pytest_rxdist.core` for importing the extension
  - [x] Add minimal in-repo `tests/` smoke suite (includes a Rust binding call)
  - [x] Plugin remains serial (no parallelism yet); optional debug output via `--rxdist-debug`

- **Deliverables**
  - Python package `pytest-rxdist` (import `pytest_rxdist`) with `pytest11` entrypoint (plugin auto-loadable).
  - Rust crate for the core engine with a thin PyO3 surface area.
  - A tiny example test suite in-repo to validate local runs.
- **Definition of done**
  - `pip install -e .` (or equivalent) works locally.
  - `pytest -p pytest_rxdist` loads the plugin without errors and runs tests serially (no parallelism yet).

---

### Milestone 1 — MVP: basic parallel runner (xdist replacement shape)

**Outcome**: parallel execution with a worker pool, producing correct pytest results with minimal features.

- **Progress**
  - [x] Implement `--numprocesses` with `auto` worker count selection
  - [x] Spawn-per-run Python worker pool (subprocess workers)
  - [x] Baseline scheduler: shared work queue (load-based)
  - [x] MessagePack IPC (framed messages)
  - [x] Best-effort pytest reporting (correct exit code + nodeid attribution)
  - [x] Integration tests covering `--numprocesses 2`, `auto`, and failure attribution

- **Scope**
  - Worker processes (Python) executing tests.
  - A simple scheduler strategy: **round-robin** or **load-based** baseline.
  - Basic result reporting back to pytest (pass/fail/skip/xfail, captured output where feasible).
- **Key decisions**
  - Start with a conservative IPC format (e.g., MessagePack) before attempting zero-copy.
  - Keep worker lifecycle simple (spawn per run) in MVP; reuse comes later.
- **Definition of done**
  - `pytest -p rxdist -n auto` runs tests in parallel and exits with correct status code.
  - Failures and tracebacks are attributable to the right test IDs.
  - No significant plugin breakage for common hooks (best-effort parity with xdist basics).

---

### Milestone 2 — Profiling + timing data foundation

**Outcome**: collect per-test runtime data and persist it for future scheduling.

- **Progress**
  - [x] SQLite timing store + schema (`.pytest_rxdist/timings.sqlite3` by default)
  - [x] Record outcomes + durations in serial mode (via `pytest_runtest_logreport`)
  - [x] Record outcomes + durations in parallel mode (from controller results)
  - [x] Print a short summary on subsequent runs (`--rxdist-profile`)
  - [x] Corruption recovery (rotate `.corrupt.<timestamp>` and rebuild)
  - [x] Tests for write/read summary + corruption recovery

- **Scope**
  - Record: test nodeid, duration, outcome, environment fingerprint (python version, platform, git sha optional).
  - Store: local file database (JSON/SQLite) as an implementation detail.
  - CLI: `--rxdist-profile=on` (or `--rxdist-profile`) to enable/force writing.
- **Definition of done**
  - After one run, a second run can read timing data and print a short summary.
  - Data corruption is handled safely (ignore + rebuild).

---

### Milestone 3 — Smart scheduler (predictive / historical)

**Outcome**: reduce idle time and improve makespan using timing history.

- **Progress**
  - [x] Avg-duration query from SQLite timing store
  - [x] Smart scheduler (LPT bin packing) with unknown-duration interleave
  - [x] Controller integration + debug stats (`--rxdist-debug`)
  - [x] Tests (unit + integration) for smart scheduling + fallback

- **Scope**
  - Scheduling strategies:
    - **Predictive**: greedy bin-packing by historical duration (Longest Processing Time first).
    - Fallback to load-based when timings missing.
  - CLI: `--rxdist-scheduler=smart` (or `--rxdist-mode=smart`) to select strategy.
- **Definition of done**
  - On an imbalanced suite, smart scheduling shows reduced idle worker time vs baseline.
  - Scheduler remains correct when timings are absent/partial.

---

### Milestone 4 — Worker reuse (warm pool)

**Outcome**: reduce overhead from process startup, imports, and repeated initialization.

- **Progress**
  - [x] In-process worker runner (warm workers run multiple nodeids without spawning Python per test)
  - [x] Safe-by-default reuse mode (`--rxdist-reuse=safe`) with `off` escape hatch
  - [x] Conservative per-test cleanup in worker
  - [x] Safe-mode respawn on worker failure (best-effort)
  - [x] Tests + basic benchmark proof (heavy-import suite faster with reuse enabled)

- **Scope**
  - Persistent workers within a single pytest invocation (and optionally across runs later).
  - Cache imports; optionally cache selected fixture setup (only if it is safe/opt-in).
  - Robust cleanup between tests to avoid cross-test contamination.
- **Definition of done**
  - Measurable speedup on suites with heavy imports/initialization.
  - A “safe mode” exists that disables risky caching features.

---

### Milestone 5 — Faster IPC (binary / optional shared memory)

**Outcome**: reduce serialization overhead and cross-process data movement.

- **Scope**
  - Replace/augment initial IPC with one of:
    - MessagePack (baseline) → Cap’n Proto (schema’d) (example path), or
    - Arrow for structured data, optionally shared memory for payloads.
  - Minimize Python↔Rust roundtrips (batch messages).
- **Definition of done**
  - Benchmarks show reduced overhead on chatty workloads (many small tests, lots of reporting).
  - IPC layer is pluggable/feature-flagged.

---

### Milestone 6 — Fixture-aware grouping (optional, high leverage)

**Outcome**: reduce redundant setup by colocating tests that share expensive fixtures.

- **Scope**
  - Derive fixture usage graph from collection phase (best-effort).
  - Group tests into “fixture cohorts” and schedule cohorts as units.
  - Provide escape hatches (disable grouping; cap cohort sizes).
- **Definition of done**
  - On fixture-heavy suites, reduces total setup time without breaking isolation assumptions.

---

### Milestone 7 — UX, analytics, and hardening

**Outcome**: make it production-friendly and easy to adopt.

- **Scope**
  - Better CLI help and docs.
  - Reporting: per-worker utilization, critical path, slowest tests, scheduler stats.
  - Compatibility testing against popular pytest plugins (best-effort matrix).
- **Definition of done**
  - A “getting started” path works for a new project in minutes.
  - Clear success metrics: 2–5× over `pytest-xdist` on at least one real-ish benchmark suite.

## Suggested repo layout (when you start implementing)

- `python/rxdist/` — pytest plugin + Python-side worker code
- `rust/` — Rust core engine crate(s)
- `tests/` — integration tests / example suites
- `docs/` — design notes and benchmarks

## Risk register (to manage early)

- **Pytest internals**: rely on stable hooks; avoid depending on private APIs where possible.
- **Plugin compatibility**: start conservative; document known incompatibilities; add a compatibility test suite.
- **Debuggability**: invest early in logs (coordinator + worker) and stable IDs for messages/tests.
- **Cross-language complexity**: keep the PyO3 surface minimal and message-driven.

