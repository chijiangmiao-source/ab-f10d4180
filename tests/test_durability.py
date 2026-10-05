"""Durability across restart and uniqueness under concurrency."""

from __future__ import annotations

import threading

import pytest

from app.config import Settings
from app.engine import Engine
from app.storage import Storage, WindowSealedError


def _submit(engine, probe, seq, event_id, ts, dose):
    return engine.submit(
        probe=probe, seq=seq, event_id=event_id,
        observed_at=ts, dose=dose,
    )


def test_state_survives_restart(make_engine):
    eng = make_engine()
    _submit(eng, "probe-a", 1, "a1", 0.0, 10.0)
    _submit(eng, "probe-b", 1, "b1", 60.0, 20.0)
    _submit(eng, "probe-a", 3, "a3", 350.0, 99.0)   # gap
    _submit(eng, "probe-a", 2, "a2", 120.0, 30.0)  # late, pre-seal
    _submit(eng, "probe-b", 2, "b2", 360.0, 7.0)
    eng.storage.close()

    # Simulate process restart: a brand-new Engine over the same database.
    eng2 = make_engine()
    w = eng2.get_window(0)
    assert w is not None
    assert w["total_dose"] == 60.0
    assert w["peak_dose"] == 30.0
    assert w["readings_count"] == 3

    p = eng2.progress()
    assert {x["probe"]: x["last_seq"] for x in p["probes"]} == {
        "probe-a": 3,
        "probe-b": 2,
    }
    v = eng2.violations()
    assert len(v) == 1 and v[0]["kind"] == "gap"

    # Sealed window stays immutable after restart too.
    with pytest.raises(WindowSealedError):
        _submit(eng2, "probe-a", 4, "late-after-restart", 10.0, 500.0)
    assert eng2.get_window(0)["total_dose"] == 60.0


def test_replay_after_restart_returns_same_token(make_engine):
    eng = make_engine()
    r1 = _submit(eng, "probe-a", 1, "evt-x", 0.0, 10.0)
    eng.storage.close()

    eng2 = make_engine()
    _submit(eng2, "probe-b", 1, "b1", 5.0, 1.0)
    r2 = _submit(eng2, "probe-a", 1, "evt-x", 0.0, 10.0)
    assert r2["status"] == "replay"
    assert r2["token"] == r1["token"]


def test_concurrent_sealing_leaves_single_record(settings):
    """Many processes/threads sealing the same due window: exactly one row."""
    # Seed durable state with one shared Storage, then close it.
    seed = Storage(settings.db_path)
    seed.submit_reading(
        probe="probe-a", other_probe="probe-b", seq=1, event_id="a1",
        observed_at=0.0, dose=10.0,
        content_sha="x", token="t", window_start=0, now=0.0,
    )
    seed.submit_reading(
        probe="probe-b", other_probe="probe-a", seq=1, event_id="b1",
        observed_at=10.0, dose=20.0,
        content_sha="y", token="u", window_start=0, now=0.0,
    )
    seed.submit_reading(
        probe="probe-a", other_probe="probe-b", seq=2, event_id="a2",
        observed_at=330.0, dose=0.0,
        content_sha="x2", token="t2", window_start=300, now=0.0,
    )
    seed.submit_reading(
        probe="probe-b", other_probe="probe-a", seq=2, event_id="b2",
        observed_at=330.0, dose=0.0,
        content_sha="y2", token="u2", window_start=300, now=0.0,
    )
    seed.close()

    results: list = []
    errors: list = []

    def worker():
        st = Storage(settings.db_path)
        try:
            eng = Engine(st, settings)
            sealed = st.seal_pending(
                probes=settings.probes,
                allowed_lateness=30,
                now=999.0,
                classify=eng.classify,
            )
            results.append(sealed)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            st.close()

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    # Exactly one worker observes the new seal; the rest see it as existing.
    assert sum(len(r) for r in results) == 1

    check = Storage(settings.db_path)
    rows = check.list_windows()
    assert len(rows) == 1
    row = rows[0]
    assert row["window_start"] == 0
    assert row["total_dose"] == 30.0
    assert row["peak_dose"] == 20.0
    check.close()


def test_concurrent_duplicate_event_single_insertion(settings):
    """The same event submitted concurrently is inserted once."""
    outcomes: list = []

    def worker():
        st = Storage(settings.db_path)
        try:
            out = st.submit_reading(
                probe="probe-a", other_probe="probe-b", seq=1, event_id="dup",
                observed_at=0.0, dose=10.0,
                content_sha="same", token="tok", window_start=0, now=1.0,
            )
            outcomes.append(out[0])
        except Exception as exc:  # noqa: BLE001
            outcomes.append(("error", str(exc)))
        finally:
            st.close()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count("new") == 1
    assert all(o == "replay" for o in outcomes if o != "new")
