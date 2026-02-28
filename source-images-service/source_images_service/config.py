"""Service configuration from environment variables."""

from pydantic_settings import BaseSettings


class ServiceConfig(BaseSettings):
    """Configuration loaded from environment variables with SIS_ prefix."""

    host: str = "0.0.0.0"
    port: int = 8080
    worker_pool_size: int = 4
    job_ttl_hours: int = 24
    temp_dir: str = "/tmp/source-images-service"
    log_level: str = "info"
    version: str = "0.1.0"

    model_config = {"env_prefix": "SIS_"}
