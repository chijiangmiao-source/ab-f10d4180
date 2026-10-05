"""Domain engine: windows, watermarks, replay identity and alarm levels."""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from typing import Any

from app import ALARM_WINDOW_SECONDS as W
from app.config import Settings
from app.storage import Storage


def window_start_of(t: float) -> int:
    """Return the epoch-aligned start of the five-minute window containing t."""
    return (int(t) // W) * W


def content_hash(probe: str, seq: int, observed_at: float, dose: float) -> str:
    """Stable hash of a reading's content (the event id is the key, excluded)."""
    canonical = json.dumps(
        [probe, seq, observed_at, dose], separators=(",", ":"), sort_keys=True
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Engine:
    def __init__(self, storage: Storage, settings: Settings) -> None:
        if len(settings.probes) != 2:
            raise SystemExit(
                f"exactly two probes must be configured, got {settings.probes!r}"
            )
        self.storage = storage
        self.settings = settings
        self.probe_a, self.probe_b = settings.probes

    # ------------------------------------------------------------- alarm

    def classify(self, total_dose: float, peak_dose: float) -> str:
        """Alarm level from a sealed window's total and peak dose.

        published risk levels are never recomputed after sealing, so this
        function only runs inside the sealing transaction.
        """
        t = self.settings.alarm_threshold
        if peak_dose >= 2 * t or total_dose >= 3 * t:
            return "critical"
        if peak_dose >= t or total_dose >= t:
            return "warning"
        return "ok"

    def _other_probe(self, probe: str) -> str:
        return self.probe_b if probe == self.probe_a else self.probe_a

    # ------------------------------------------------------------ submit

    def submit(
        self,
        *,
        probe: str,
        seq: int,
        event_id: str,
        observed_at: float,
        dose: float,
    ) -> dict[str, Any]:
        if probe not in self.settings.probes:
            raise ValueError(f"unknown probe {probe!r}")

        sha = content_hash(probe, seq, observed_at, dose)
        now = time.time()
        token = secrets.token_hex(16)
        start = window_start_of(observed_at)

        # All identity / sealing checks happen in storage's one IMMEDIATE
        # transaction. An identical re-send replays the original result even
        # when the window it belongs to has since been sealed; a never-seen
        # event for a sealed window is rejected as a late packet.
        status, stored_token, stored_observed_at = self.storage.submit_reading(
            probe=probe,
            other_probe=self._other_probe(probe),
            seq=seq,
            event_id=event_id,
            observed_at=observed_at,
            dose=dose,
            content_sha=sha,
            token=token,
            window_start=start,
            now=now,
        )
        if status == "replay":
            # Echo the originally recorded window, not the retransmitted copy.
            token = stored_token
            start = window_start_of(stored_observed_at)

        sealed = self.storage.seal_pending(
            probes=self.settings.probes,
            allowed_lateness=self.settings.allowed_lateness_seconds,
            now=time.time(),
            classify=self.classify,
        )
        return {
            "status": status,
            "event_id": event_id,
            "token": token,
            "window_start": start,
            "window_end": start + W,
            "sealed_windows": [self._window_payload(s) for s in sealed],
        }

    # ------------------------------------------------------------- reads

    def _window_payload(self, seal_or_row: Any, *, include_readings: bool = True) -> dict[str, Any]:
        g = seal_or_row.__getitem__
        start = g("window_start")
        payload = {
            "window_start": start,
            "window_end": g("window_end"),
            "total_dose": g("total_dose"),
            "peak_dose": g("peak_dose"),
            "alarm_level": g("alarm_level"),
            "readings_count": g("readings_count"),
            "watermark_at_seal": g("watermark_at_seal"),
            "sealed_at": g("sealed_at"),
            "readings": [],
        }
        if include_readings:
            payload["readings"] = [dict(r) for r in self.storage.window_readings(start)]
        return payload

    def get_window(self, window_start: int) -> dict[str, Any] | None:
        row = self.storage.get_window(window_start)
        if row is None:
            return None
        return self._window_payload(row)

    def list_windows(self) -> list[dict[str, Any]]:
        return [
            self._window_payload(r, include_readings=False)
            for r in self.storage.list_windows()
        ]

    def progress(self) -> dict[str, Any]:
        snap = self.storage.progress(self.settings.probes)
        raw = snap["watermark_raw"]
        return {
            "watermark_raw": raw,
            "watermark_effective": (
                raw - self.settings.allowed_lateness_seconds
                if raw is not None
                else None
            ),
            "allowed_lateness_seconds": self.settings.allowed_lateness_seconds,
            "probes": snap["probes"],
        }

    def violations(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.storage.list_violations()]
