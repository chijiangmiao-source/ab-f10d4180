"""End-to-end HTTP behaviour via FastAPI's TestClient."""

from __future__ import annotations

import pytest


def _post(client, **kw):
    body = {"probe": "probe-a", "seq": 1, "event_id": "e",
            "observed_at": 0.0, "dose": 1.0}
    body.update(kw)
    return client.post("/readings", json=body)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    assert r.json()["window_seconds"] == 300


def test_submit_and_window_flow(client):
    assert _post(client, probe="probe-a", seq=1, event_id="a1",
                 observed_at=0.0, dose=10.0).status_code == 201
    assert _post(client, probe="probe-b", seq=1, event_id="b1",
                 observed_at=30.0, dose=20.0).status_code == 201

    # Not due yet.
    assert client.get("/windows/0").status_code == 404

    r = _post(client, probe="probe-a", seq=2, event_id="a2",
              observed_at=330.0, dose=1.0)
    assert r.status_code == 201
    r = _post(client, probe="probe-b", seq=2, event_id="b2",
              observed_at=330.0, dose=1.0)
    sealed = r.json()["sealed_windows"]
    assert [w["window_start"] for w in sealed] == [0]

    r = client.get("/windows/0")
    assert r.status_code == 200
    body = r.json()
    assert body["total_dose"] == 30.0
    assert body["peak_dose"] == 20.0
    assert body["alarm_level"] == "ok"

    r = client.get("/windows/by-time", params={"at": 123.4})
    assert r.status_code == 200 and r.json()["window_start"] == 0

    listed = client.get("/windows").json()
    assert len(listed) == 1


def test_replay_and_conflict_http(client):
    assert _post(client, seq=1, event_id="evt", observed_at=10.0,
                 dose=10.0).status_code == 201
    r = _post(client, seq=1, event_id="evt", observed_at=10.0, dose=10.0)
    assert r.status_code == 201
    assert r.json()["status"] == "replay"

    r = _post(client, seq=1, event_id="evt", observed_at=10.0, dose=11.0)
    assert r.status_code == 409
    assert r.json()["error"] == "integrity_conflict"


def test_late_packet_rejected_http(client):
    _post(client, probe="probe-a", seq=1, event_id="a1",
          observed_at=0.0, dose=10.0)
    _post(client, probe="probe-b", seq=1, event_id="b1",
          observed_at=30.0, dose=20.0)
    _post(client, probe="probe-a", seq=2, event_id="a2",
          observed_at=330.0, dose=1.0)
    _post(client, probe="probe-b", seq=2, event_id="b2",
          observed_at=330.0, dose=1.0)

    r = _post(client, seq=3, event_id="late", observed_at=100.0, dose=999.0)
    assert r.status_code == 409
    assert r.json()["error"] == "window_sealed"

    win = client.get("/windows/0").json()
    assert win["total_dose"] == 30.0
    assert win["readings_count"] == 2


def test_progress_and_violations_http(client):
    _post(client, probe="probe-b", seq=1, event_id="b1",
          observed_at=0.0, dose=1.0)
    r = client.get("/progress").json()
    assert r["watermark_effective"] is None
    probe_a = next(p for p in r["probes"] if p["probe"] == "probe-a")
    assert probe_a["observed"] is False

    _post(client, probe="probe-a", seq=5, event_id="a5",
          observed_at=10.0, dose=1.0)  # gap
    r = client.get("/violations").json()
    assert len(r) == 1
    assert r[0]["kind"] == "gap"
    assert r[0]["first_seq"] == 5
    assert r[0]["other_probe"] == "probe-b"


def test_unknown_probe_and_validation(client):
    r = _post(client, probe="probe-z", seq=1, event_id="x",
              observed_at=0.0, dose=1.0)
    assert r.status_code == 400

    r = client.post("/readings", json={
        "probe": "probe-a", "seq": -1, "event_id": "x",
        "observed_at": 0.0, "dose": 1.0,
    })
    assert r.status_code == 422
