"""FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

import uvicorn
from fastapi import FastAPI

from source_images_service.api.v1.router import router as v1_router
from source_images_service.config import ServiceConfig
from source_images_service.core.job_manager import JobManager

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "fmt": "%(asctime)s %(levelprefix)s %(message)s",
            "use_colors": None,
            "()": "uvicorn.logging.DefaultFormatter",
        },
        "access": {
            "fmt": '%(asctime)s %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            "()": "uvicorn.logging.AccessFormatter",
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "access": {
            "formatter": "access",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"level": "INFO"},
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
    },
}


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup: create JobManager. Shutdown: clean up."""
    config = app.state.config
    _configure_logging(config.log_level)
    logger = logging.getLogger(__name__)
    logger.info(
        "Starting source-images-service v%s on %s:%d",
        config.version,
        config.host,
        config.port,
    )
    app.state.job_manager = JobManager(config)
    yield
    logger.info("Shutting down source-images-service")
    app.state.job_manager.shutdown()


def create_app(config: ServiceConfig | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    if config is None:
        config = ServiceConfig()
    app = FastAPI(
        title="Source Images Service",
        description="Container image sourcing service for benchmarks and tools",
        version=config.version,
        lifespan=lifespan,
    )
    app.state.config = config
    app.include_router(v1_router)
    return app


def run() -> None:
    """Entry point for the uvicorn server."""
    config = ServiceConfig()
    app = create_app(config)
    uvicorn.run(app, host=config.host, port=config.port, log_config=LOGGING_CONFIG)


if __name__ == "__main__":
    run()
