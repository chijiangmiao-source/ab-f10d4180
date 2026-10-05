"""FastAPI application: submit readings and read sealed window results."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from app import ALARM_WINDOW_SECONDS
from app.config import Settings
from app.engine import Engine, window_start_of
from app.models import (
    ProgressResponse,
    ReadingIn,
    SubmitResponse,
    ViolationOut,
    WindowResult,
)
from app.storage import (
    IntegrityConflict,
    SequenceError,
    Storage,
    WindowSealedError,
)

settings = Settings.from_env()
storage = Storage(settings.db_path)
engine = Engine(storage, settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.settings = settings
    app.state.storage = storage
    app.state.engine = engine
    try:
        yield
    finally:
        storage.close()


app = FastAPI(
    title="LEO Radiation Monitor",
    version="1.0.0",
    description=(
        "Two-probe dose ingestion with out-of-order/retransmit handling, a "
        "min-progress watermark and immutable sealed five-minute windows."
    ),
    lifespan=lifespan,
)


@app.exception_handler(IntegrityConflict)
async def integrity_conflict_handler(request, exc: IntegrityConflict):
    return JSONResponse(
        status_code=409,
        content={"error": "integrity_conflict", "detail": str(exc)},
    )


@app.exception_handler(SequenceError)
async def sequence_error_handler(request, exc: SequenceError):
    return JSONResponse(
        status_code=422,
        content={
            "error": "sequence_violation",
            "kind": exc.kind,
            "detail": str(exc),
        },
    )


@app.exception_handler(WindowSealedError)
async def window_sealed_handler(request, exc: WindowSealedError):
    return JSONResponse(
        status_code=409,
        content={
            "error": "window_sealed",
            "detail": (
                f"window starting at {exc.window_start} is already sealed; "
                "late reading rejected, aggregate unchanged"
            ),
            "window_start": exc.window_start,
        },
    )


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "leo-radiation-monitor",
        "window_seconds": ALARM_WINDOW_SECONDS,
        "time": time.time(),
    }


@app.post("/readings", response_model=SubmitResponse, status_code=201)
def submit_reading(reading: ReadingIn):
    try:
        return engine.submit(
            probe=reading.probe,
            seq=reading.seq,
            event_id=reading.event_id,
            observed_at=reading.observed_at,
            dose=reading.dose,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/windows", response_model=list[WindowResult])
def list_windows():
    """List all sealed windows (the published, immutable risk records)."""
    return engine.list_windows()


@app.get("/windows/by-time", response_model=WindowResult)
def window_by_time(at: float = Query(..., description="any instant inside the window")):
    start = window_start_of(at)
    result = engine.get_window(start)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "window_not_sealed",
                "window_start": start,
                "window_end": start + ALARM_WINDOW_SECONDS,
            },
        )
    return result


@app.get("/windows/{window_start}", response_model=WindowResult)
def get_window(window_start: int):
    """Read one sealed window by its epoch-aligned start instant."""
    result = engine.get_window(window_start)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "window_not_sealed",
                "window_start": window_start,
                "window_end": window_start + ALARM_WINDOW_SECONDS,
            },
        )
    return result


@app.get("/progress", response_model=ProgressResponse)
def get_progress():
    """Current per-probe progress and the min-progress watermark.

    ``watermark_effective`` is null until BOTH probes have appeared - a
    missing probe can never fabricate a watermark.
    """
    return engine.progress()


@app.get("/violations", response_model=list[ViolationOut])
def get_violations():
    """First sequence violation per probe, with the other probe's progress."""
    return engine.violations()


def main() -> None:  # pragma: no cover - process entry point
    import uvicorn

    uvicorn.run(app, host=settings.host, port=settings.port, log_level="info")


if __name__ == "__main__":  # pragma: no cover
    main()
