"""V1 API router — aggregates all endpoint routers."""

from fastapi import APIRouter

from source_images_service.api.v1.endpoints import health, jobs, source_images

router = APIRouter(prefix="/api/v1")

router.include_router(health.router, tags=["health"])
router.include_router(source_images.router, tags=["source-images"])
router.include_router(jobs.router, tags=["jobs"])
