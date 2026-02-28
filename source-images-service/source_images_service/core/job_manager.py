"""Job lifecycle management with in-memory store and thread pool executor."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from source_images_service.config import ServiceConfig
from source_images_service.core.workspace import (
    WorkspacePaths,
    cleanup_workspace,
    materialize_workspace,
)
from source_images_service.models.requests import SourceImagesRequest
from source_images_service.models.responses import JobProgress, JobStatus

logger = logging.getLogger(__name__)


@dataclass
class Job:
    """In-memory representation of an image sourcing job."""

    id: str
    status: JobStatus = JobStatus.PENDING
    progress: JobProgress = field(default_factory=JobProgress)
    result: dict[str, Any] | None = None
    error: str | None = None
    log_buffer: str = ""
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    request: SourceImagesRequest | None = None
    workspace: WorkspacePaths | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def append_log(self, message: str) -> None:
        with self._lock:
            self.log_buffer += message + "\n"


class JobManager:
    """Manages job submission, execution, and retrieval."""

    def __init__(self, config: ServiceConfig) -> None:
        self._config = config
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=config.worker_pool_size,
            thread_name_prefix="source-images-worker",
        )
        logger.info(
            "JobManager initialized with %d workers", config.worker_pool_size
        )

    def submit_job(self, request: SourceImagesRequest) -> Job:
        """Create a new job and submit it for execution. Returns the Job."""
        job_id = str(uuid.uuid4())
        job = Job(id=job_id, request=request)

        # Count total items for progress tracking
        total = sum(
            len(userenvs)
            for userenvs in request.image_ids.values()
        )
        job.progress = JobProgress(total_items=total)

        with self._lock:
            self._jobs[job_id] = job

        self._executor.submit(self._run_job, job)
        logger.info("Submitted job %s (%d items)", job_id, total)
        return job

    def get_job(self, job_id: str) -> Job | None:
        """Retrieve a job by ID."""
        with self._lock:
            return self._jobs.get(job_id)

    def get_job_counts(self) -> tuple[int, int]:
        """Return (active_jobs, pending_jobs) counts."""
        with self._lock:
            active = sum(
                1 for j in self._jobs.values() if j.status == JobStatus.RUNNING
            )
            pending = sum(
                1 for j in self._jobs.values() if j.status == JobStatus.PENDING
            )
            return active, pending

    def shutdown(self) -> None:
        """Shut down the executor and clean up any remaining workspaces."""
        logger.info("Shutting down JobManager")
        self._executor.shutdown(wait=True)
        with self._lock:
            for job in self._jobs.values():
                if job.workspace:
                    try:
                        cleanup_workspace(job.workspace)
                    except Exception:
                        logger.exception(
                            "Error cleaning workspace for job %s", job.id
                        )

    def cleanup_expired_jobs(self) -> int:
        """Remove jobs older than job_ttl_hours. Returns count removed."""
        cutoff = time.time() - (self._config.job_ttl_hours * 3600)
        removed = 0
        with self._lock:
            expired_ids = [
                jid
                for jid, job in self._jobs.items()
                if job.created_at < cutoff
                and job.status in (JobStatus.COMPLETED, JobStatus.FAILED)
            ]
            for jid in expired_ids:
                job = self._jobs.pop(jid)
                if job.workspace:
                    try:
                        cleanup_workspace(job.workspace)
                    except Exception:
                        logger.exception(
                            "Error cleaning workspace for expired job %s", jid
                        )
                removed += 1
        if removed:
            logger.info("Cleaned up %d expired jobs", removed)
        return removed

    def _run_job(self, job: Job) -> None:
        """Execute the job worker. Called in a thread pool thread."""
        job.status = JobStatus.RUNNING
        job.started_at = time.time()
        job.append_log(f"Job {job.id} started")

        try:
            assert job.request is not None
            workspace = materialize_workspace(
                job.request, self._config.temp_dir
            )
            job.workspace = workspace
            job.append_log(f"Workspace materialized at {workspace.root}")

            # Phase 1 placeholder: echo back image_ids keys with placeholder values
            result = self._placeholder_worker(job)

            job.result = result
            job.status = JobStatus.COMPLETED
            job.append_log("Job completed successfully")
            logger.info("Job %s completed", job.id)

        except Exception as exc:
            job.status = JobStatus.FAILED
            job.error = str(exc)
            job.append_log(f"Job failed: {exc}")
            logger.exception("Job %s failed", job.id)

        finally:
            job.completed_at = time.time()
            if job.workspace:
                try:
                    cleanup_workspace(job.workspace)
                    job.workspace = None
                except Exception:
                    logger.exception(
                        "Error cleaning workspace for job %s", job.id
                    )
            # Release the request to free memory
            job.request = None

    def _placeholder_worker(self, job: Job) -> dict:
        """Phase 1 placeholder: return dummy results echoing image_ids structure."""
        assert job.request is not None
        image_ids: dict[str, dict[str, dict]] = {}

        completed = 0
        for benchmark, userenvs in sorted(job.request.image_ids.items()):
            image_ids[benchmark] = {}
            for userenv in sorted(userenvs.keys()):
                job.progress = JobProgress(
                    current_benchmark=benchmark,
                    completed_items=completed,
                    total_items=job.progress.total_items,
                )
                job.append_log(f"Processing {benchmark}/{userenv} (placeholder)")
                image_ids[benchmark][userenv] = {"image": "placeholder"}
                completed += 1

        job.progress = JobProgress(
            completed_items=completed,
            total_items=job.progress.total_items,
        )
        return {"image-ids": image_ids}
