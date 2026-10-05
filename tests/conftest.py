"""Pytest fixtures."""

from __future__ import annotations

import os

import pytest

from app.config import Settings
from app.engine import Engine
from app.storage import Storage


@pytest.fixture
def settings(tmp_path):
    return Settings(
        host="127.0.0.1",
        port=8080,
        db_path=str(tmp_path / "test.db"),
        allowed_lateness_seconds=30,
        probes=("probe-a", "probe-b"),
        alarm_threshold=100.0,
    )


@pytest.fixture
def storage(settings):
    s = Storage(settings.db_path)
    yield s
    s.close()


@pytest.fixture
def engine(storage, settings):
    return Engine(storage, settings)


@pytest.fixture
def make_engine(settings):
    """Factory: a fresh Engine on the SAME database file (restart sim)."""
    engines: list[Engine] = []

    def _make():
        st = Storage(settings.db_path)
        eng = Engine(st, settings)
        engines.append(eng)
        return eng

    yield _make

    for eng in engines:
        eng.storage.close()


@pytest.fixture
def client(settings, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("DB_PATH", settings.db_path)
    monkeypatch.setenv("ALLOWED_LATENESS_SECONDS", "30")
    monkeypatch.setenv("ALARM_THRESHOLD", "100")
    # Import lazily so module-level env reading picks up the monkeypatched env.
    import importlib

    import app.main as main_mod

    importlib.reload(main_mod)
    with TestClient(main_mod.app) as c:
        c.main_mod = main_mod  # type: ignore[attr-defined]
        yield c
    main_mod.storage.close()
