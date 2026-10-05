"""Runtime configuration, sourced from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma no cover - defensive, exercised via bad env
        raise SystemExit(f"invalid integer for {name}: {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    db_path: str
    #: Extra time subtracted from the min-progress watermark (watermark slack).
    allowed_lateness_seconds: int = 30
    #: Known probe identifiers; readings from other probes are rejected.
    probes: tuple[str, ...] = ("probe-a", "probe-b")
    #: Dose (in the same unit as readings) at or above which a window alarms.
    alarm_threshold: float = 100.0

    @staticmethod
    def from_env() -> "Settings":
        lateness = _env_int("ALLOWED_LATENESS_SECONDS", 30)
        if lateness < 0:
            raise SystemExit("ALLOWED_LATENESS_SECONDS must be >= 0")
        port = _env_int("APP_PORT", 8080)
        if not 1 <= port <= 65535:
            raise SystemExit("APP_PORT must be between 1 and 65535")
        return Settings(
            host=os.environ.get("APP_HOST", "0.0.0.0"),
            port=port,
            db_path=os.environ.get("DB_PATH", "/data/radiation.db"),
            allowed_lateness_seconds=lateness,
            probes=tuple(
                p.strip()
                for p in os.environ.get("PROBES", "probe-a,probe-b").split(",")
                if p.strip()
            ),
            alarm_threshold=float(os.environ.get("ALARM_THRESHOLD", "100.0")),
        )
