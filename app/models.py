"""HTTP request / response models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ReadingIn(BaseModel):
    probe: str = Field(..., description="Configured probe identifier")
    seq: int = Field(..., ge=0, description="Per-probe monotonically increasing sequence")
    event_id: str = Field(..., min_length=1, description="Stable event identifier")
    observed_at: float = Field(..., ge=0, description="Observation instant, seconds since epoch")
    dose: float = Field(..., ge=0, description="Measured dose")


class SubmitResponse(BaseModel):
    status: Literal["new", "replay"]
    event_id: str
    token: str
    window_start: int
    window_end: int
    sealed_windows: list["WindowResult"] = []


class WindowResult(BaseModel):
    window_start: int
    window_end: int
    total_dose: float
    peak_dose: float
    alarm_level: str
    readings_count: int
    watermark_at_seal: float
    sealed_at: float
    readings: list["ReadingOut"] = []


class ReadingOut(BaseModel):
    probe: str
    seq: int
    event_id: str
    observed_at: float
    dose: float


class ProbeProgress(BaseModel):
    probe: str
    observed: bool
    last_seq: int | None
    last_observed_at: float | None


class ProgressResponse(BaseModel):
    watermark_raw: float | None
    watermark_effective: float | None
    allowed_lateness_seconds: int
    probes: list[ProbeProgress]


class ViolationOut(BaseModel):
    probe: str
    first_seq: int
    kind: str
    detail: str
    detected_at: float
    observed_at: float
    other_probe: str | None
    other_last_seq: int | None
    other_observed_at: float | None


class ErrorBody(BaseModel):
    error: str
    detail: str


SubmitResponse.model_rebuild()
WindowResult.model_rebuild()
