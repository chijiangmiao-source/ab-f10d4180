"""Core engine behaviour: watermark, out-of-order merge, sealing, replay."""

from __future__ import annotations

import pytest

from app.storage import IntegrityConflict, SequenceError, WindowSealedError


def _submit(engine, probe, seq, event_id, ts, dose):
    return engine.submit(
        probe=probe, seq=seq, event_id=event_id,
        observed_at=ts, dose=dose,
    )


def test_no_watermark_until_both_probes_seen(engine):
    _submit(engine, "probe-a", 1, "a1", 100.0, 10.0)
    p = engine.progress()
    assert p["watermark_raw"] is None
    assert p["watermark_effective"] is None
    assert [x["observed"] for x in p["probes"]] == [True, False]

    # Even far beyond a window boundary, one probe alone seals nothing.
    _submit(engine, "probe-a", 2, "a2", 10_000.0, 10.0)
    assert engine.list_windows() == []


def test_window_not_sealed_before_end_plus_lateness(engine):
    _submit(engine, "probe-a", 1, "a1", 0.0, 10.0)
    _submit(engine, "probe-b", 1, "b1", 60.0, 20.0)
    # min progress = 0 -> effective watermark -30: nothing due.
    assert engine.list_windows() == []

    _submit(engine, "probe-a", 2, "a2", 329.0, 5.0)
    _submit(engine, "probe-b", 2, "b2", 329.0, 7.0)
    # watermark = 329 - 30 = 299 < window end 300 -> still sealed.
    assert engine.list_windows() == []


def test_window_seals_with_total_peak_and_level(engine):
    _submit(engine, "probe-a", 1, "a1", 0.0, 10.0)
    _submit(engine, "probe-b", 1, "b1", 60.0, 20.0)
    _submit(engine, "probe-a", 2, "a2", 330.0, 5.0)
    res = _submit(engine, "probe-b", 2, "b2", 330.0, 7.0)

    assert [w["window_start"] for w in res["sealed_windows"]] == [0]
    w = engine.get_window(0)
    assert w["window_end"] == 300
    assert w["total_dose"] == 30.0
    assert w["peak_dose"] == 20.0
    assert w["alarm_level"] == "ok"
    assert w["readings_count"] == 2
    assert {r["event_id"] for r in w["readings"]} == {"a1", "b1"}


def test_out_of_order_reading_merged_before_sealing(engine):
    _submit(engine, "probe-a", 1, "a1", 0.0, 10.0)
    _submit(engine, "probe-b", 1, "b1", 60.0, 20.0)
    # seq 3 overtakes seq 2
    _submit(engine, "probe-a", 3, "a3", 350.0, 999.0)
    # seq 2 arrives late but BEFORE window 0 is sealed -> must be included
    _submit(engine, "probe-a", 2, "a2", 120.0, 30.0)
    _submit(engine, "probe-b", 2, "b2", 360.0, 7.0)  # wm=320 -> seal w0

    w = engine.get_window(0)
    assert w["total_dose"] == 60.0       # 10 + 20 + 30
    assert w["peak_dose"] == 30.0
    assert w["readings_count"] == 3
    # The 999 burst lives in window 1, which is not sealed yet.
    assert engine.get_window(300) is None


def test_late_packet_after_seal_is_rejected_and_aggregate_unchanged(engine):
    _submit(engine, "probe-a", 1, "a1", 0.0, 10.0)
    _submit(engine, "probe-b", 1, "b1", 60.0, 20.0)
    _submit(engine, "probe-a", 2, "a2", 330.0, 5.0)
    _submit(engine, "probe-b", 2, "b2", 330.0, 7.0)
    sealed_total = engine.get_window(0)["total_dose"]

    # New event, in sequence order, but its observation time is in a sealed
    # window: it must not rewrite the published result.
    with pytest.raises(WindowSealedError):
        _submit(engine, "probe-a", 3, "late1", 150.0, 500.0)
    w = engine.get_window(0)
    assert w["total_dose"] == sealed_total == 30.0
    assert w["peak_dose"] == 20.0
    assert w["readings_count"] == 2

    # Re-attempting the same rejected event keeps being rejected, idempotently.
    with pytest.raises(WindowSealedError):
        _submit(engine, "probe-a", 3, "late1", 150.0, 500.0)
    assert engine.get_window(0)["total_dose"] == 30.0


def test_retransmission_same_content_replays_original_result(engine):
    r1 = _submit(engine, "probe-a", 1, "evt-1", 10.0, 10.0)
    _submit(engine, "probe-b", 1, "b1", 20.0, 5.0)
    _submit(engine, "probe-a", 2, "a2", 330.0, 1.0)
    _submit(engine, "probe-b", 2, "b2", 330.0, 1.0)

    # Same event id, byte-identical content, even "resent" after sealing:
    # original result is played back, no duplicate reading.
    r2 = _submit(engine, "probe-a", 1, "evt-1", 10.0, 10.0)
    assert r2["status"] == "replay"
    assert r2["token"] == r1["token"]
    w = engine.get_window(0)
    assert w["readings_count"] == 2


def test_retransmission_different_content_is_conflict(engine):
    _submit(engine, "probe-a", 1, "evt-9", 10.0, 10.0)
    _submit(engine, "probe-b", 1, "b1", 20.0, 5.0)
    with pytest.raises(IntegrityConflict):
        _submit(engine, "probe-a", 1, "evt-9", 10.0, 11.0)  # dose changed
    with pytest.raises(IntegrityConflict):
        _submit(engine, "probe-a", 1, "evt-9", 11.0, 10.0)  # time changed
    # Original content still replays normally after rejected conflicts.
    r = _submit(engine, "probe-a", 1, "evt-9", 10.0, 10.0)
    assert r["status"] == "replay"


def test_duplicate_seq_with_new_event_id_is_regression(engine):
    _submit(engine, "probe-a", 1, "evt-a", 10.0, 10.0)
    with pytest.raises(SequenceError) as ei:
        _submit(engine, "probe-a", 1, "evt-b", 10.0, 10.0)
    assert ei.value.kind == "regression"

    # The rejected duplicate must not exist, and the violation is auditable.
    v = engine.violations()
    assert len(v) == 1
    assert v[0]["kind"] == "regression"
    assert v[0]["first_seq"] == 1
    # First violation is frozen even if a later gap occurs.
    _submit(engine, "probe-b", 1, "b1", 20.0, 1.0)
    _submit(engine, "probe-a", 9, "evt-c", 30.0, 1.0)  # gap
    v = engine.violations()
    assert len(v) == 1 and v[0]["kind"] == "regression"


def test_first_violation_records_other_probe_progress(engine):
    _submit(engine, "probe-a", 1, "a1", 0.0, 10.0)
    _submit(engine, "probe-b", 1, "b1", 60.0, 20.0)
    # Gap on probe-a while probe-b is still at seq 1 / t=60.
    _submit(engine, "probe-a", 4, "a4", 200.0, 3.0)

    v = engine.violations()
    assert len(v) == 1
    first = v[0]
    assert first["probe"] == "probe-a"
    assert first["first_seq"] == 4
    assert first["kind"] == "gap"
    assert first["other_probe"] == "probe-b"
    assert first["other_last_seq"] == 1
    assert first["other_observed_at"] == 60.0

    # A later regression must not overwrite the FIRST violation record.
    _submit(engine, "probe-a", 2, "a2", 90.0, 1.0)
    again = engine.violations()
    assert len(again) == 1
    assert again[0]["first_seq"] == 4


def test_alarm_levels(engine):
    seq = {"probe-a": 0, "probe-b": 0}

    def seal_window(start, readings):
        # readings: (probe, dose); observation times inside `start`.
        # Sequence numbers are strictly increasing across all windows.
        for probe, dose in readings:
            seq[probe] += 1
            _submit(
                engine, probe, seq[probe],
                f"e-{start}-{probe}", float(start + 10), dose,
            )
        # Advance BOTH probes past end+30 to trigger sealing of this window.
        for probe in ("probe-a", "probe-b"):
            seq[probe] += 1
            _submit(
                engine, probe, seq[probe],
                f"adv-{start}-{probe}", float(start + 330), 0.0,
            )
        return engine.get_window(start)

    # Window 0: total 150 >= threshold, peak below -> warning
    w0 = seal_window(0, [("probe-a", 80.0), ("probe-b", 70.0)])
    assert w0["alarm_level"] == "warning"

    # Window 300: peak 250 >= 2*threshold -> critical
    w1 = seal_window(300, [("probe-a", 250.0), ("probe-b", 1.0)])
    assert w1["alarm_level"] == "critical"

    # Window 600: benign -> ok
    w2 = seal_window(600, [("probe-a", 40.0), ("probe-b", 50.0)])
    assert w2["alarm_level"] == "ok"


def test_unknown_probe_rejected(engine):
    with pytest.raises(ValueError):
        _submit(engine, "probe-c", 1, "x", 0.0, 1.0)
