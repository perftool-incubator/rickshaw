"""Source images endpoint — submit image sourcing jobs."""

from fastapi import APIRouter, Request, status

from source_images_service.models.requests import SourceImagesRequest
from source_images_service.models.responses import SubmitJobResponse

router = APIRouter()


@router.post(
    "/source-images",
    response_model=SubmitJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_source_images(
    request: Request,
    body: SourceImagesRequest,
) -> dict:
    """Submit an image sourcing job. Returns immediately with a job ID."""
    job_manager = request.app.state.job_manager
    job = job_manager.submit_job(body)
    return {
        "job-id": job.id,
        "status": job.status.value,
    }
