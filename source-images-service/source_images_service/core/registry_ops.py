"""Container registry operations via skopeo/buildah — port of Perl L264-503."""

from __future__ import annotations

import logging
import os
import time
from typing import TYPE_CHECKING

from source_images_service.core.exceptions import RegistryError
from source_images_service.core.subprocess_runner import run_cmd

if TYPE_CHECKING:
    from source_images_service.core.job_manager import Job
    from source_images_service.core.workspace import WorkspacePaths
    from source_images_service.models.requests import RegistryConfig

logger = logging.getLogger(__name__)


def _resolve_full_url(
    image: str,
    registry_config: RegistryConfig,
    url_key: str = "dest",
) -> str:
    """Build a full image URL.

    If *image* already contains a ``:``, use it as-is.  Otherwise prepend
    the appropriate registry URL (``dest-image-url`` or ``source-image-url``).
    """
    if ":" in image:
        return image
    if url_key == "dest":
        return f"{registry_config.url_details.dest_image_url}:{image}"
    return f"{registry_config.url_details.source_image_url}:{image}"


def remote_image_found(
    image: str,
    registry_type: str,
    registries: dict[str, RegistryConfig],
    workspace_paths: WorkspacePaths,
    *,
    job: Job | None = None,
) -> bool:
    """Check whether *image* exists in the remote registry via ``skopeo inspect``.

    Returns True if the image is found, False otherwise.
    """
    reg = registries[registry_type]
    full_url = _resolve_full_url(image, reg, url_key="dest")

    logger.debug("Checking for remote image: %s", full_url)

    if full_url.startswith("dir:") or full_url.startswith("docker://"):
        skopeo_url = full_url
    else:
        skopeo_url = f"docker://{full_url}"

    auth_arg = ""
    if registry_type == "private":
        pull_token = workspace_paths.registries / f"pull-token-{registry_type}"
        auth_arg = f"--authfile={pull_token}"

    tls = str(reg.tls_verify).lower()
    cmd = (
        f"skopeo inspect --no-tags --raw {auth_arg} "
        f"--tls-verify={tls} {skopeo_url} 2>&1"
    )

    result = run_cmd(cmd, job=job)
    return result.return_code == 0


def local_image_found(
    image: str,
    registry_type: str,
    registries: dict[str, RegistryConfig],
    *,
    job: Job | None = None,
) -> bool:
    """Check whether *image* exists locally via ``buildah images``.

    Returns True if the image is found, False otherwise.
    """
    reg = registries[registry_type]
    full_url = _resolve_full_url(image, reg, url_key="source")

    logger.debug("Checking for local image: %s", full_url)

    result = run_cmd(f"buildah images {full_url}", job=job)
    if result.return_code == 0:
        return True

    # Not found — dump all local images for debug visibility
    logger.debug("Image not found locally; listing all buildah images")
    run_cmd("buildah images", job=job)
    return False


def push_local_image(
    image_tag: str,
    registry_type: str,
    registries: dict[str, RegistryConfig],
    workspace_paths: WorkspacePaths,
    *,
    job: Job | None = None,
) -> None:
    """Push a locally-built image to the remote registry.

    Retries up to 5 times on transient errors (502, rate-limiting).
    After a successful push, polls ``remote_image_found()`` up to 20 times
    (3 s apart) to verify the image is visible.

    Raises RegistryError on exhausted retries or failed verification.
    """
    reg = registries[registry_type]
    full_src = f"{reg.url_details.source_image_url}:{image_tag}"
    full_dest = f"{reg.url_details.dest_image_url}:{image_tag}"

    # For dir: scheme destinations, create the directory
    if full_dest.startswith("dir:"):
        image_dir = full_dest[4:]  # strip "dir:"
        if not os.path.isdir(image_dir):
            logger.info("Creating local registry directory: %s", image_dir)
            os.makedirs(image_dir, exist_ok=True)

    # Pre-check: image must exist locally
    if not local_image_found(image_tag, registry_type, registries, job=job):
        raise RegistryError(
            f"Could not find local image before push ({image_tag})"
        )

    # Build the push command
    auth_arg = ""
    push_token = workspace_paths.registries / f"push-token-{registry_type}"
    if push_token.exists():
        auth_arg = f"--authfile {push_token}"

    tls = str(reg.tls_verify).lower()
    cmd = (
        f"buildah {auth_arg} push "
        f"--tls-verify={tls} {full_src} {full_dest}"
    )

    # Retry loop
    max_attempts = 5
    last_result = None
    for attempt in range(1, max_attempts + 1):
        logger.debug("push attempt %d/%d: %s", attempt, max_attempts, cmd)
        last_result = run_cmd(cmd, job=job)

        if last_result.return_code == 0:
            if attempt > 1:
                msg = f"push succeeded after {attempt}/{max_attempts} attempts"
                logger.warning(msg)
                if job is not None:
                    job.append_log(f"WARNING: {msg}")
            break

        # Check if retryable
        retryable = (
            "502 Bad Gateway" in last_result.output
            or "502 (Bad Gateway)" in last_result.output
            or "too many requests" in last_result.output
        )

        if not retryable or attempt >= max_attempts:
            raise RegistryError(
                f"push failed (rc={last_result.return_code}) after "
                f"{attempt}/{max_attempts} attempts: {cmd}\n"
                f"Output:\n{last_result.output}"
            )

        logger.debug("Retryable push failure, sleeping %ds", attempt)
        time.sleep(attempt)

    # Post-push verification
    if not remote_image_found(
        image_tag, registry_type, registries, workspace_paths, job=job
    ):
        msg = "Failed to find remote image after push...retrying"
        logger.warning(msg)
        if job is not None:
            job.append_log(f"WARNING: {msg}")

        for i in range(1, 21):
            time.sleep(3)
            if remote_image_found(
                image_tag, registry_type, registries, workspace_paths, job=job
            ):
                logger.info("Found image on verification retry %d", i)
                return
            logger.info("Verification retry %d: image not yet visible", i)

        raise RegistryError(
            f"Could not find remote image after push ({image_tag})"
        )


def delete_local_image(
    image: str,
    registry_type: str,
    registries: dict[str, RegistryConfig],
    *,
    job: Job | None = None,
) -> None:
    """Delete a locally-built image via ``buildah rmi``.

    Raises RegistryError if the image is not found locally or if the
    delete command fails.
    """
    reg = registries[registry_type]
    full_url = _resolve_full_url(image, reg, url_key="source")

    if not local_image_found(image, registry_type, registries, job=job):
        raise RegistryError(
            f"Could not find local image before delete ({full_url})"
        )

    logger.debug("Deleting local image %s", full_url)
    result = run_cmd(f"buildah rmi {full_url}", job=job)

    if result.return_code != 0:
        raise RegistryError(
            f"rmi failed (rc={result.return_code}): buildah rmi {full_url}\n"
            f"Output:\n{result.output}"
        )
