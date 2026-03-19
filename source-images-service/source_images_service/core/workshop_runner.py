"""Image building via workshop script — port of Perl workshop_build_image() (L323-378)."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from source_images_service.core.exceptions import SubprocessError, WorkshopBuildError
from source_images_service.core.subprocess_runner import run_cmd

if TYPE_CHECKING:
    from source_images_service.core.job_manager import Job
    from source_images_service.core.workspace import WorkspacePaths
    from source_images_service.models.requests import RegistryConfig

logger = logging.getLogger(__name__)


def _parse_json_from_output(output_lines: list[str]) -> list[dict]:
    """Extract JSON array from the end of workshop script output.

    Workshop emits a JSON array at the very end of its stdout, possibly
    preceded by many lines of non-JSON debug/info text.  We scan backwards
    from the last line looking for a line that equals ``[`` (the start of
    the JSON array), then concatenate everything from that line forward.

    Raises WorkshopBuildError if no JSON can be extracted.
    """
    json_text = ""
    for i in range(len(output_lines) - 1, -1, -1):
        json_text = output_lines[i] + "\n" + json_text
        if output_lines[i].strip() == "[":
            break
    else:
        raise WorkshopBuildError(
            "Could not locate JSON array start '[' in workshop output"
        )

    try:
        return json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise WorkshopBuildError(
            f"Failed to parse workshop JSON output: {exc}\n"
            f"Extracted text:\n{json_text}"
        ) from exc


def workshop_build_image(
    userenv: str,
    registry_type: str,
    bench_or_tool: str,
    workshop_base_cmd: str,
    stage: int,
    userenv_arg: str,
    req_args: str | None,
    tag: str,
    skip_update: str | None,
    registries: dict[str, RegistryConfig],
    workspace_paths: WorkspacePaths,
    *,
    job: Job | None = None,
) -> str:
    """Run workshop script to build a container image and return the image ID.

    Raises WorkshopBuildError on build failure or if the image ID cannot
    be parsed from the output.
    """
    if skip_update is None:
        logger.info("skip_update not defined, defaulting to 'false'")
        skip_update = "false"

    reg = registries[registry_type]
    if reg.url_details.project:
        proj = f"{reg.url_details.host}/{reg.url_details.project}"
    else:
        proj = reg.url_details.host

    label = reg.url_details.label

    parts = [
        workshop_base_cmd,
        f"--skip-update {skip_update}",
        userenv_arg,
    ]
    if req_args:
        parts.append(req_args)
    parts.extend([
        f"--proj {proj}",
        f"--label {label}",
        f"--tag {tag}",
    ])
    build_cmd = " ".join(parts)

    logger.debug("Workshop build command: %s", build_cmd)
    result = run_cmd(build_cmd, job=job)

    # Save full output to a file for debugging
    output_lines = result.output.splitlines()
    output_file = (
        workspace_paths.build
        / f"{userenv}__{bench_or_tool}__stage-{stage}.{tag}.stdout.txt"
    )
    output_file.write_text(result.output)

    if result.return_code != 0:
        raise WorkshopBuildError(
            f"Workshop build failed (rc={result.return_code}).\n"
            f"Command: {build_cmd}\n"
            f"Output:\n{result.output}"
        )

    # Parse JSON from end of output
    parsed = _parse_json_from_output(output_lines)

    try:
        image_id: str = parsed[0]["id"]
    except (IndexError, KeyError, TypeError) as exc:
        raise WorkshopBuildError(
            f"Workshop JSON output missing 'id' field.\n"
            f"Parsed: {parsed}"
        ) from exc

    return image_id
