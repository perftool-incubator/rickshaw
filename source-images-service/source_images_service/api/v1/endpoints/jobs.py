"""Job status and log retrieval endpoints."""

from fastapi import APIRouter, HTTPException, Query, Request

from source_images_service.models.responses import (
    JobLogResponse,
    JobStatusResponse,
)

router = APIRouter()


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(request: Request, job_id: str) -> dict:
    """Return the current status, progress, and result of a job."""
    job_manager = request.app.state.job_manager
    job = job_manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    response: dict = {
        "job-id": job.id,
        "status": job.status.value,
        "progress": None,
        "result": None,
        "error": None,
    }
    if job.progress:
        response["progress"] = {
            "current-benchmark": job.progress.current_benchmark,
            "completed-items": job.progress.completed_items,
            "total-items": job.progress.total_items,
        }
    if job.result is not None:
        response["result"] = job.result
    if job.error is not None:
        response["error"] = job.error
    return response


@router.get("/jobs/{job_id}/log", response_model=JobLogResponse)
async def get_job_log(
    request: Request,
    job_id: str,
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Return accumulated build log output, optionally starting from an offset."""
    job_manager = request.app.state.job_manager
    job = job_manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    log_text = job.log_buffer
    total_length = len(log_text)
    sliced = log_text[offset:]
    return {
        "job-id": job.id,
        "log": sliced,
        "offset": offset,
        "total-length": total_length,
    }
