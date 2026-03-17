from __future__ import annotations

import os
import platform
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


def env_fingerprint() -> str:
    parts = {
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "platform_release": platform.release(),
        "machine": platform.machine(),
    }
    git_sha = _git_sha()
    if git_sha:
        parts["git_sha"] = git_sha
    return ";".join(f"{k}={v}" for k, v in sorted(parts.items()))


def _git_sha() -> str | None:
    try:
        p = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        sha = p.stdout.strip()
        return sha if sha else None
    except Exception:
        return None


def default_timings_path(project_root: Path) -> Path:
    override = os.environ.get("PYTEST_RXDIST_TIMINGS_PATH")
    if override:
        return Path(override).expanduser()
    return project_root / ".pytest_rxdist" / "timings.sqlite3"


@dataclass(frozen=True)
class TimingSummaryRow:
    nodeid: str
    last_duration_s: float
    avg_duration_s: float
    count: int


class TimingStore:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    @classmethod
    def open(cls, path: Path) -> "TimingStore":
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            conn = sqlite3.connect(str(path))
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            store = cls(conn)
            store._init_schema()
            return store
        except sqlite3.DatabaseError:
            # Corrupt/invalid DB: rotate it and rebuild.
            cls._rotate_corrupt_db(path)
            conn = sqlite3.connect(str(path))
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            store = cls(conn)
            store._init_schema()
            return store

    @staticmethod
    def _rotate_corrupt_db(path: Path) -> None:
        if not path.exists():
            return
        ts = time.strftime("%Y%m%d-%H%M%S")
        rotated = path.with_name(path.name + f".corrupt.{ts}")
        try:
            shutil.move(str(path), str(rotated))
        except Exception:
            # Best-effort: if rename/move fails, just remove it.
            try:
                path.unlink()
            except Exception:
                pass

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              started_at REAL NOT NULL,
              env_fingerprint TEXT NOT NULL,
              rxdist_version TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS test_results (
              run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
              nodeid TEXT NOT NULL,
              duration_s REAL NOT NULL,
              outcome TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_test_results_nodeid ON test_results(nodeid);
            """
        )
        self._conn.commit()

    def write_run(self, *, started_at: float, env_fp: str, rxdist_version: str, results: Iterable[dict[str, Any]]) -> int:
        cur = self._conn.cursor()
        cur.execute(
            "INSERT INTO runs (started_at, env_fingerprint, rxdist_version) VALUES (?, ?, ?)",
            (started_at, env_fp, rxdist_version),
        )
        run_id = int(cur.lastrowid)

        rows: list[tuple[int, str, float, str]] = []
        for r in results:
            nodeid = str(r.get("nodeid"))
            duration_s = float(r.get("duration_s") or 0.0)
            outcome = str(r.get("outcome") or "unknown")
            rows.append((run_id, nodeid, duration_s, outcome))

        cur.executemany(
            "INSERT INTO test_results (run_id, nodeid, duration_s, outcome) VALUES (?, ?, ?, ?)",
            rows,
        )
        self._conn.commit()
        return run_id

    def summary(self, *, limit: int = 10) -> list[TimingSummaryRow]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT
              tr.nodeid,
              (SELECT tr2.duration_s
               FROM test_results tr2
               JOIN runs r2 ON r2.id = tr2.run_id
               WHERE tr2.nodeid = tr.nodeid
               ORDER BY r2.started_at DESC
               LIMIT 1) AS last_duration_s,
              AVG(tr.duration_s) AS avg_duration_s,
              COUNT(*) AS count
            FROM test_results tr
            GROUP BY tr.nodeid
            ORDER BY avg_duration_s DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        out: list[TimingSummaryRow] = []
        for nodeid, last_d, avg_d, cnt in cur.fetchall():
            out.append(
                TimingSummaryRow(
                    nodeid=str(nodeid),
                    last_duration_s=float(last_d or 0.0),
                    avg_duration_s=float(avg_d or 0.0),
                    count=int(cnt or 0),
                )
            )
        return out

    def count_tests(self) -> int:
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT nodeid) FROM test_results")
        (n,) = cur.fetchone()
        return int(n or 0)

    def avg_durations(self, nodeids: Sequence[str]) -> dict[str, float]:
        if not nodeids:
            return {}

        # Chunk to avoid SQLite parameter limits in large suites.
        out: dict[str, float] = {}
        cur = self._conn.cursor()
        chunk_size = 500
        for i in range(0, len(nodeids), chunk_size):
            chunk = list(nodeids[i : i + chunk_size])
            placeholders = ",".join("?" for _ in chunk)
            cur.execute(
                f"""
                SELECT nodeid, AVG(duration_s) AS avg_duration_s
                FROM test_results
                WHERE nodeid IN ({placeholders})
                GROUP BY nodeid
                """,
                tuple(chunk),
            )
            for nodeid, avg_d in cur.fetchall():
                if nodeid is None:
                    continue
                out[str(nodeid)] = float(avg_d or 0.0)
        return out

