"""Pydantic response models for the source-images API."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Possible states for a sourcing job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class JobProgress(BaseModel):
    """Progress information for a running job."""

    current_benchmark: str | None = Field(
        default=None, alias="current-benchmark"
    )
    completed_items: int = Field(default=0, alias="completed-items")
    total_items: int = Field(default=0, alias="total-items")

    model_config = {"populate_by_name": True}


class SubmitJobResponse(BaseModel):
    """Response from POST /api/v1/source-images."""

    job_id: str = Field(alias="job-id")
    status: JobStatus

    model_config = {"populate_by_name": True, "use_enum_values": True}


class JobStatusResponse(BaseModel):
    """Response from GET /api/v1/jobs/{job_id}."""

    job_id: str = Field(alias="job-id")
    status: JobStatus
    progress: JobProgress | None = None
    result: dict[str, Any] | None = None
    error: str | None = None

    model_config = {"populate_by_name": True, "use_enum_values": True}


class JobLogResponse(BaseModel):
    """Response from GET /api/v1/jobs/{job_id}/log."""

    job_id: str = Field(alias="job-id")
    log: str
    offset: int
    total_length: int = Field(alias="total-length")

    model_config = {"populate_by_name": True}


class HealthResponse(BaseModel):
    """Response from GET /api/v1/health."""

    status: str
    version: str
    active_jobs: int = Field(alias="active-jobs")
    pending_jobs: int = Field(alias="pending-jobs")

    model_config = {"populate_by_name": True}
