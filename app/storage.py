"""SQLite-backed durable storage.

All state that must survive a process restart lives here:

* ``probe_state`` - per-probe progress: largest observed seq / timestamp
* ``events``      - event-id dedup index: content hash + opaque replay token
* ``readings``    - every accepted reading, UNIQUE(probe, seq)
* ``windows``     - sealed windows; PRIMARY KEY(window_start) guarantees that
                    concurrent transactions and process restarts can never
                    create a second seal for the same window
* ``violations``  - first sequence violation per probe (gap / regression),
                    frozen together with the other probe's progress

Sealing reads its inputs and writes the window row inside one ``IMMEDIATE``
transaction, so totals/peak/alarm level are sealed atomically and can never be
rewritten by a late packet.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from app import ALARM_WINDOW_SECONDS as W

_SCHEMA = """
CREATE TABLE IF NOT EXISTS probe_state (
    probe            TEXT PRIMARY KEY,
    last_seq         INTEGER NOT NULL,
    last_observed_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    token       TEXT NOT NULL,
    probe       TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    observed_at REAL NOT NULL,
    dose        REAL NOT NULL,
    content_sha TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS readings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    window_start INTEGER NOT NULL,
    probe        TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    event_id     TEXT NOT NULL,
    observed_at  REAL NOT NULL,
    dose         REAL NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS readings_probe_seq
    ON readings(probe, seq);

CREATE TABLE IF NOT EXISTS windows (
    window_start      INTEGER PRIMARY KEY,
    window_end        INTEGER NOT NULL,
    total_dose        REAL NOT NULL,
    peak_dose         REAL NOT NULL,
    alarm_level       TEXT NOT NULL,
    sealed_at         REAL NOT NULL,
    readings_count    INTEGER NOT NULL,
    watermark_at_seal REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS violations (
    probe              TEXT PRIMARY KEY,
    first_seq          INTEGER NOT NULL,
    kind               TEXT NOT NULL,
    detail             TEXT NOT NULL,
    detected_at        REAL NOT NULL,
    observed_at        REAL NOT NULL,
    other_probe        TEXT,
    other_last_seq     INTEGER,
    other_observed_at  REAL
);
"""


class IntegrityConflict(Exception):
    """A retransmitted event id arrived with different content (HTTP 409)."""


class SequenceError(Exception):
    """Sequence regression: (probe, seq) already used (HTTP 422)."""

    def __init__(self, message: str, kind: str = "regression") -> None:
        super().__init__(message)
        self.kind = kind


class WindowSealedError(Exception):
    """A genuinely new reading landed in a window that is already sealed."""

    def __init__(self, window_start: int) -> None:
        super().__init__(f"window starting at {window_start} is already sealed")
        self.window_start = window_start


class Storage:
    """Thread-safe wrapper over one SQLite database (WAL, fsync on commit)."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        parent = Path(db_path).parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ----------------------------------------------------------- submission

    def submit_reading(
        self,
        *,
        probe: str,
        other_probe: str,
        seq: int,
        event_id: str,
        observed_at: float,
        dose: float,
        content_sha: str,
        token: str,
        window_start: int,
        now: float,
    ) -> tuple[str, str, float]:
        """Persist one reading in its own durable transaction.

        Returns ``(status, token, stored_observed_at)`` where status is
        ``"new"`` or ``"replay"``. All checks (event-id dedup, sealed-window
        rejection, sequence tracking) run inside one IMMEDIATE transaction,
        so concurrent submissions of the same event cannot both insert.
        Raises :class:`IntegrityConflict`, :class:`SequenceError` or
        :class:`WindowSealedError`.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            tx_open = True
            try:
                # 1) Event-id dedup: identical content replays, different
                #    content is an explicit conflict. This check comes first,
                #    so a retransmission of an already-sealed reading still
                #    replays its original result.
                row = self._conn.execute(
                    "SELECT * FROM events WHERE event_id = ?", (event_id,)
                ).fetchone()
                if row is not None:
                    if row["content_sha"] != content_sha:
                        raise IntegrityConflict(
                            f"event {event_id!r} was already submitted with "
                            "different content"
                        )
                    self._conn.execute("COMMIT")
                    tx_open = False
                    return "replay", row["token"], row["observed_at"]

                # 2) A never-seen event landing in a sealed window is a late
                #    packet: reject it and leave the sealed aggregate intact.
                sealed = self._conn.execute(
                    "SELECT 1 FROM windows WHERE window_start = ?",
                    (window_start,),
                ).fetchone()
                if sealed is not None:
                    raise WindowSealedError(window_start)

                # 3) Sequence accounting. A seq already consumed by this
                #    probe under a DIFFERENT event id is a regression (422).
                #    An unseen lower seq (e.g. seq 2 arriving after seq 3) is
                #    ordinary out-of-order delivery: it is accepted and must
                #    be merged before sealing. Only a discontinuity (gap) is
                #    recorded for audit, and neither case ever moves the
                #    watermark - progress tracks observed timestamps only.
                occupied = self._conn.execute(
                    "SELECT event_id FROM readings WHERE probe = ? AND seq = ?",
                    (probe, seq),
                ).fetchone()
                if occupied is not None:
                    other = self._conn.execute(
                        "SELECT * FROM probe_state WHERE probe = ?",
                        (other_probe,),
                    ).fetchone()
                    detail = (
                        f"seq {seq} already consumed by event "
                        f"{occupied['event_id']!r}"
                    )
                    # No writes have happened yet: commit the read lock and
                    # record the FIRST violation in its own transaction so
                    # the rejection itself can roll back without losing it.
                    self._conn.execute("COMMIT")
                    tx_open = False
                    self._persist_violation(
                        probe, seq, "regression", detail, observed_at,
                        other, now,
                    )
                    raise SequenceError(detail, kind="regression")

                max_row = self._conn.execute(
                    "SELECT MAX(seq) AS m FROM readings WHERE probe = ?",
                    (probe,),
                ).fetchone()
                high_water = max_row["m"]
                if high_water is None:
                    kind = "gap" if seq > 1 else None
                    detail = (
                        f"first seq {seq} jumps ahead of 1 "
                        f"(missing 1..{seq - 1})"
                        if kind else ""
                    )
                elif seq > high_water + 1:
                    kind = "gap"
                    detail = (
                        f"seq {seq} jumps from {high_water} "
                        f"(missing {high_water + 1}..{seq - 1})"
                    )
                else:
                    kind = None
                    detail = ""

                self._conn.execute(
                    """
                    INSERT INTO events(event_id, token, probe, seq, observed_at,
                                       dose, content_sha)
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (event_id, token, probe, seq, observed_at, dose,
                     content_sha),
                )
                self._conn.execute(
                    """
                    INSERT INTO readings(window_start, probe, seq, event_id,
                                         observed_at, dose)
                    VALUES (?,?,?,?,?,?)
                    """,
                    (window_start, probe, seq, event_id, observed_at, dose),
                )
                # Progress uses observed timestamps only - sequence gaps and
                # regressions can never fabricate or move the watermark.
                self._conn.execute(
                    """
                    INSERT INTO probe_state(probe, last_seq, last_observed_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(probe) DO UPDATE SET
                        last_seq = MAX(probe_state.last_seq, excluded.last_seq),
                        last_observed_at = MAX(probe_state.last_observed_at,
                                               excluded.last_observed_at)
                    """,
                    (probe, seq, observed_at),
                )

                if kind is not None:
                    other = self._conn.execute(
                        "SELECT * FROM probe_state WHERE probe = ?",
                        (other_probe,),
                    ).fetchone()
                    self._insert_violation(
                        probe, seq, kind, detail, observed_at, other, now,
                    )
                self._conn.execute("COMMIT")
                tx_open = False
                return "new", token, observed_at
            except BaseException:
                if tx_open:
                    self._conn.execute("ROLLBACK")
                raise

    def _insert_violation(
        self, probe, seq, kind, detail, observed_at, other, now
    ) -> None:
        """Insert the first violation; caller owns the open transaction."""
        self._conn.execute(
            """
            INSERT INTO violations(probe, first_seq, kind, detail, detected_at,
                                   observed_at, other_probe, other_last_seq,
                                   other_observed_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(probe) DO NOTHING
            """,
            (
                probe, seq, kind, detail, now, observed_at,
                other["probe"] if other else None,
                other["last_seq"] if other else None,
                other["last_observed_at"] if other else None,
            ),
        )

    def _persist_violation(
        self, probe, seq, kind, detail, observed_at, other, now
    ) -> None:
        """Durably record the first violation in its own transaction."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._insert_violation(
                probe, seq, kind, detail, observed_at, other, now
            )
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    # -------------------------------------------------------------- sealing

    def seal_pending(
        self,
        *,
        probes: tuple[str, ...],
        allowed_lateness: int,
        now: float,
        classify: Callable[[float, float], str],
    ) -> list[dict[str, Any]]:
        """Seal every window with ``window_end <= watermark``.

        Watermark = min(last_observed_at over all probes) - allowed_lateness.
        If any configured probe has never appeared, no watermark exists and
        nothing is sealed. Reads, aggregation and the insert of each window
        row happen in one IMMEDIATE transaction; window_start's PRIMARY KEY
        makes concurrent sealing idempotent.
        """
        sealed: list[dict[str, Any]] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                states = {
                    r["probe"]: r
                    for r in self._conn.execute(
                        "SELECT * FROM probe_state"
                    ).fetchall()
                }
                if not all(p in states for p in probes):
                    self._conn.execute("COMMIT")
                    return []

                watermark = (
                    min(states[p]["last_observed_at"] for p in probes)
                    - allowed_lateness
                )
                # Largest window END not later than the watermark.
                last_due_end = (int(watermark) // W) * W
                if last_due_end < W:
                    self._conn.execute("COMMIT")
                    return []

                already = {
                    r["window_start"]
                    for r in self._conn.execute(
                        "SELECT window_start FROM windows"
                    ).fetchall()
                }
                rows = self._conn.execute(
                    "SELECT window_start, dose FROM readings"
                ).fetchall()
                if not rows:
                    self._conn.execute("COMMIT")
                    return []
                min_start = min(r["window_start"] for r in rows)
                grouped: dict[int, list[float]] = {}
                for r in rows:
                    grouped.setdefault(r["window_start"], []).append(r["dose"])

                last_start = last_due_end - W
                # Only windows that actually contain readings are sealed; an
                # empty stretch between observed windows is not fabricated.
                for start in range(min_start, last_start + 1, W):
                    if start in already:
                        continue
                    doses = grouped.get(start)
                    if not doses:
                        continue
                    total = round(sum(doses), 9)
                    peak = max(doses, default=0.0)
                    seal = {
                        "window_start": start,
                        "window_end": start + W,
                        "total_dose": total,
                        "peak_dose": peak,
                        "alarm_level": classify(total, peak),
                        "sealed_at": now,
                        "readings_count": len(doses),
                        "watermark_at_seal": watermark,
                    }
                    self._conn.execute(
                        """
                        INSERT INTO windows(window_start, window_end,
                                            total_dose, peak_dose, alarm_level,
                                            sealed_at, readings_count,
                                            watermark_at_seal)
                        VALUES (?,?,?,?,?,?,?,?)
                        """,
                        (
                            seal["window_start"], seal["window_end"],
                            seal["total_dose"], seal["peak_dose"],
                            seal["alarm_level"], seal["sealed_at"],
                            seal["readings_count"], seal["watermark_at_seal"],
                        ),
                    )
                    sealed.append(seal)
                self._conn.execute("COMMIT")
                return sealed
            except sqlite3.IntegrityError:
                # Another process won the race for this window_start; its
                # record is the unique seal - report nothing as new.
                self._conn.execute("ROLLBACK")
                return []
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # ---------------------------------------------------------------- reads

    def get_window(self, window_start: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM windows WHERE window_start = ?",
                (window_start,),
            ).fetchone()

    def list_windows(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM windows ORDER BY window_start"
                ).fetchall()
            )

    def window_readings(self, window_start: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT probe, seq, event_id, observed_at, dose "
                    "FROM readings WHERE window_start = ? "
                    "ORDER BY probe, seq",
                    (window_start,),
                ).fetchall()
            )

    def list_violations(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM violations ORDER BY detected_at, probe"
                ).fetchall()
            )

    def progress(self, probes: tuple[str, ...]) -> dict[str, Any]:
        with self._lock:
            rows = {
                r["probe"]: r
                for r in self._conn.execute("SELECT * FROM probe_state")
            }
            if all(p in rows for p in probes):
                watermark: float | None = (
                    min(rows[p]["last_observed_at"] for p in probes)
                )
            else:
                watermark = None
        per_probe = []
        for p in probes:
            r = rows.get(p)
            per_probe.append(
                {
                    "probe": p,
                    "observed": r is not None,
                    "last_seq": r["last_seq"] if r else None,
                    "last_observed_at": (
                        r["last_observed_at"] if r else None
                    ),
                }
            )
        return {"probes": per_probe, "watermark_raw": watermark}
