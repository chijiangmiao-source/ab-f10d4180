#!/usr/bin/env python3
"""HTTP smoke test for the running radiation-monitoring service.

Covers, over real HTTP:
  * health
  * out-of-order arrival merged BEFORE sealing (seq overtaking)
  * retransmission with identical content -> replay, same token, no duplicate
  * same event id with different content -> explicit 409 conflict
  * a genuinely new packet for a sealed window -> 409, aggregate unchanged
  * progress / violations audit endpoints

Re-runnable: sequence numbers continue from each probe's published progress
and a fresh future window is chosen on every invocation.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
import uuid

WINDOW = 300
LATENESS = 30


class Client:
    def __init__(self, base: str) -> None:
        self.base = base.rstrip("/")

    def request(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode() or "{}")
        except urllib.error.HTTPError as exc:
            payload = exc.read().decode()
            return exc.code, json.loads(payload) if payload else {}


def check(name: str, ok: bool, detail: str = "") -> None:
    if not ok:
        print(f"FAIL  {name} {detail}")
        sys.exit(1)
    print(f"PASS  {name}")


def main(base: str) -> None:
    c = Client(base)

    status, body = c.request("GET", "/health")
    check("health 200", status == 200, str(body))
    check("health window=300", body.get("window_seconds") == 300, str(body))

    # Continue sequences from persisted progress so reruns never regress.
    _, progress = c.request("GET", "/progress")
    seqs = {p["probe"]: (p["last_seq"] or 0) for p in progress["probes"]}
    check("two probes tracked", set(seqs) == {"probe-a", "probe-b"}, str(seqs))
    sa, sb = seqs["probe-a"], seqs["probe-b"]

    run = uuid.uuid4().hex[:8]

    # Choose a target window strictly beyond BOTH the clock and every reading
    # already observed by either probe, with a two-window margin. This keeps
    # the target window and the follower window (where the watermark-advancing
    # packets land) free of readings left by previous smoke runs.
    lead = max([time.time()] + [
        p["last_observed_at"] or 0.0 for p in progress["probes"]
    ])
    base_t = (int(lead) // WINDOW) * WINDOW + 2 * WINDOW
    t_a1 = float(base_t + 10)
    t_b1 = float(base_t + 20)
    t_next = float(base_t + WINDOW + 50)  # inside following window
    t_a2 = float(base_t + 120)            # late inside target window

    s1, _ = c.request("POST", "/readings", {
        "probe": "probe-a", "seq": sa + 1, "event_id": f"{run}-a1",
        "observed_at": t_a1, "dose": 10.0,
    })
    check("first reading accepted", s1 == 201, f"status={s1}")

    c.request("POST", "/readings", {
        "probe": "probe-b", "seq": sb + 1, "event_id": f"{run}-b1",
        "observed_at": t_b1, "dose": 20.0,
    })
    # probe-a seq sa+3 overtakes seq sa+2 (out of order).
    c.request("POST", "/readings", {
        "probe": "probe-a", "seq": sa + 3, "event_id": f"{run}-a3",
        "observed_at": t_next, "dose": 999.0,
    })
    # The missing seq arrives late, still BEFORE the window seals.
    s4, r4 = c.request("POST", "/readings", {
        "probe": "probe-a", "seq": sa + 2, "event_id": f"{run}-a2",
        "observed_at": t_a2, "dose": 30.0,
    })
    check("out-of-order reading accepted pre-seal", s4 == 201, f"status={s4}")

    # probe-b advances to the following window -> watermark reaches target end.
    _, r5 = c.request("POST", "/readings", {
        "probe": "probe-b", "seq": sb + 2, "event_id": f"{run}-b2",
        "observed_at": t_next, "dose": 7.0,
    })
    sealed_ids = [w["window_start"] for w in r5.get("sealed_windows", [])]
    check("target window sealed in submit response", base_t in sealed_ids,
          f"sealed={sealed_ids} want={base_t}")

    status, win = c.request("GET", f"/windows/{base_t}")
    check("sealed window readable", status == 200, str(win))
    check("total merges out-of-order packet (10+20+30=60)",
          win["total_dose"] == 60.0, str(win["total_dose"]))
    check("peak is 30 (999 burst is in the next window)",
          win["peak_dose"] == 30.0, str(win["peak_dose"]))
    check("readings_count == 3", win["readings_count"] == 3,
          str(win["readings_count"]))
    check("alarm_level ok", win["alarm_level"] == "ok", str(win["alarm_level"]))
    wm = win["watermark_at_seal"]
    check("sealed only when end <= watermark",
          win["window_end"] <= wm + 1e-9, f"end={win['window_end']} wm={wm}")

    # Identical retransmission replays the original result.
    s, rep = c.request("POST", "/readings", {
        "probe": "probe-a", "seq": sa + 1, "event_id": f"{run}-a1",
        "observed_at": t_a1, "dose": 10.0,
    })
    check("retransmission -> 201 replay", s == 201 and rep["status"] == "replay",
          f"status={s} body={rep}")
    _, win_after_replay = c.request("GET", f"/windows/{base_t}")
    check("replay does not duplicate readings",
          win_after_replay["readings_count"] == 3
          and win_after_replay["total_dose"] == 60.0,
          str(win_after_replay))

    # Same event id, different content -> explicit conflict.
    s, conflict = c.request("POST", "/readings", {
        "probe": "probe-a", "seq": sa + 1, "event_id": f"{run}-a1",
        "observed_at": t_a1, "dose": 11.0,
    })
    check("content conflict -> 409 integrity_conflict",
          s == 409 and conflict.get("error") == "integrity_conflict",
          f"status={s} body={conflict}")

    # New event with an observation time inside the sealed window is late.
    s, late = c.request("POST", "/readings", {
        "probe": "probe-a", "seq": sa + 4, "event_id": f"{run}-late",
        "observed_at": t_a2, "dose": 500.0,
    })
    check("late packet -> 409 window_sealed",
          s == 409 and late.get("error") == "window_sealed",
          f"status={s} body={late}")
    _, win_after_late = c.request("GET", f"/windows/{base_t}")
    check("sealed aggregate unchanged after late packet",
          win_after_late["total_dose"] == 60.0
          and win_after_late["peak_dose"] == 30.0
          and win_after_late["readings_count"] == 3,
          str(win_after_late))

    # Audit endpoints.
    s, violations = c.request("GET", "/violations")
    check("violations listable", s == 200 and isinstance(violations, list),
          str(violations))
    gap = next((v for v in violations
                if v["probe"] == "probe-a" and v["kind"] == "gap"), None)
    check("first gap violation recorded with other-probe progress",
          gap is not None and gap.get("other_probe") == "probe-b"
          and gap.get("other_last_seq") is not None,
          str(gap))

    print("ALL HTTP SMOKE CHECKS PASSED")


if __name__ == "__main__":
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://app:8080"
    main(base_url)
