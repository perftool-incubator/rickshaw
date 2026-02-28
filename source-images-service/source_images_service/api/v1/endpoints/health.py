"""Health check endpoint."""

from fastapi import APIRouter, Request

from source_images_service.models.responses import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> dict:
    """Return service health status and job counts."""
    job_manager = request.app.state.job_manager
    config = request.app.state.config
    active, pending = job_manager.get_job_counts()
    return {
        "status": "healthy",
        "version": config.version,
        "active-jobs": active,
        "pending-jobs": pending,
    }
