"""MD5 image tag calculation — port of Perl calc_image_md5() (L96-261)."""

from __future__ import annotations

import hashlib
import logging
import os
import re
from typing import TYPE_CHECKING

from source_images_service.core.exceptions import HashCalculationError, SubprocessError
from source_images_service.core.file_utils import find_files
from source_images_service.core.subprocess_runner import run_cmd

if TYPE_CHECKING:
    from source_images_service.core.job_manager import Job
    from source_images_service.core.workspace import WorkspacePaths

logger = logging.getLogger(__name__)

_READ_CHUNK = 8192


def _extract_after_marker(lines: list[str], marker: str) -> list[str]:
    """Return all lines after the **last** occurrence of *marker*.

    Scans the full list so that if the marker appears more than once the
    final occurrence wins — matching the Perl behaviour.
    """
    break_line = 0
    for i, line in enumerate(lines):
        if marker in line:
            break_line = i
    return lines[break_line + 1 :]


def _download_remote_file(url: str, *, job: Job | None = None) -> str:
    """Download *url* to a deterministic temp path and return that path.

    The temp filename is ``/tmp/rickshaw-run.<md5(url)>`` so that the same
    URL always maps to the same local file.

    Raises HashCalculationError on download failure.
    """
    url_hash = hashlib.md5(url.encode()).hexdigest()
    dest = f"/tmp/rickshaw-run.{url_hash}"

    curl_cmd = (
        f'curl --output {dest} --show-error --stderr - '
        f'--fail-with-body "{url}"'
    )
    logger.debug("Downloading %s -> %s", url, dest)

    result = run_cmd(curl_cmd, job=job)
    if result.return_code != 0:
        # Try to read whatever was written for diagnostics
        contents = ""
        if os.path.isfile(dest):
            try:
                contents = open(dest).read()
            except OSError:
                pass
        raise HashCalculationError(
            f"Failed to download '{url}' for hash calculation.\n"
            f"Command: {curl_cmd}\n"
            f"Output:\n{result.output}\n"
            f"Downloaded content:\n{contents}"
        )

    return dest


def calc_image_md5(
    workshop_base_cmd: str,
    userenv_arg: str,
    req_args: str | None,
    arch_suffix: str,
    userenv: str,
    benchmark_tool: str,
    stage: int,
    workspace_paths: WorkspacePaths,
    *,
    job: Job | None = None,
) -> str:
    """Calculate an MD5-based image tag for a workshop build.

    The tag encodes both the workshop config output **and** the binary
    content of every file that workshop identifies as part of the image.

    Returns ``<md5hex>_<arch_suffix>``.

    Raises HashCalculationError or SubprocessError on failure.
    """
    logger.debug(
        "calc_image_md5: userenv=%s benchmark_tool=%s stage=%d",
        userenv, benchmark_tool, stage,
    )

    # Build the sub-command prefix
    if req_args:
        sub_cmd = f"{workshop_base_cmd} {userenv_arg} {req_args}"
    else:
        sub_cmd = f"{workshop_base_cmd} {userenv_arg}"

    # --- Step 1: dump-config ---
    config_cmd = f"{sub_cmd} --label config-analysis --dump-config true"
    try:
        config_result = run_cmd(config_cmd, job=job, check=True)
    except SubprocessError as exc:
        raise HashCalculationError(
            f"Workshop dump-config failed:\n{exc.output}"
        ) from exc

    config_lines = config_result.output.splitlines()
    config_data = _extract_after_marker(config_lines, "Config dump:")

    # --- Step 2: dump-files ---
    files_cmd = f"{sub_cmd} --label files-listing --dump-files true"
    try:
        files_result = run_cmd(files_cmd, job=job, check=True)
    except SubprocessError as exc:
        raise HashCalculationError(
            f"Workshop dump-files failed:\n{exc.output}"
        ) from exc

    files_lines = files_result.output.splitlines()
    files_data = _extract_after_marker(files_lines, "Files dump:")

    # --- Step 3: build file list ---
    files: list[str] = [
        str(workspace_paths.workshop / "workshop.pl"),
        str(workspace_paths.workshop / "schema.json"),
    ]

    for dumped_file in files_data:
        # Skip verbose/replacement log lines
        if re.match(r"^\[VERBOSE\]|^replacing", dumped_file):
            continue

        logger.debug("Found file from workshop: %s", dumped_file)

        # Remote URL detection (http://, https://, etc.)
        if re.match(r"^[a-zA-Z]+://", dumped_file):
            logger.debug("File appears remote, downloading: %s", dumped_file)
            dumped_file = _download_remote_file(dumped_file, job=job)

        # Resolve symlinks
        real_path = os.path.realpath(dumped_file)
        if real_path != dumped_file:
            logger.debug("Resolved symlink %s -> %s", dumped_file, real_path)
            dumped_file = real_path

        if os.path.isfile(dumped_file):
            files.append(dumped_file)
        elif os.path.isdir(dumped_file):
            logger.debug("Entry is a directory, expanding: %s", dumped_file)
            files.extend(find_files(dumped_file))

    # --- Step 4: compute MD5 ---
    md5 = hashlib.md5()

    # The workspace root is a random temp directory (mkdtemp) that changes
    # every run.  Replace it with a fixed placeholder before feeding data
    # into the hash so that identical inputs always produce the same tag.
    workspace_root_bytes = str(workspace_paths.root).encode()
    canonical_root_bytes = b"/RICKSHAW_WORKSPACE"

    # Hash config output first
    config_text = "".join(config_data)
    config_bytes = config_text.encode()
    config_bytes = config_bytes.replace(workspace_root_bytes, canonical_root_bytes)
    md5.update(config_bytes)

    # Write audit file
    build_dir = workspace_paths.build
    audit_path = (
        build_dir
        / f"tag-calc-data__{userenv}__{benchmark_tool}__stage-{stage}.txt"
    )
    item_header = (
        "# Item "
        "#########################################################################\n"
    )

    with open(audit_path, "w") as audit:
        audit.write(f"{item_header}Workshop Config Output:\n{config_text}\n")

        # Hash file contents (sorted)
        for filepath in sorted(files):
            logger.debug("Adding to hash: %s", filepath)
            audit.write(f"{item_header}File: {filepath}\nFile Contents:\n")

            try:
                with open(filepath, "rb") as fh:
                    # Write text representation to audit
                    data = fh.read()
                    try:
                        audit.write(data.decode("utf-8", errors="replace"))
                    except Exception:
                        audit.write("<binary data>\n")
                    audit.write("\n")

                    # Hash binary content (with workspace path normalized)
                    md5.update(data.replace(workspace_root_bytes, canonical_root_bytes))
            except OSError as exc:
                raise HashCalculationError(
                    f"Cannot read file for hashing: {filepath}"
                ) from exc

        full_hash = f"{md5.hexdigest()}_{arch_suffix}"
        audit.write(f"{item_header}Hash: {full_hash}\n")

    logger.debug("calc_image_md5: returning %s", full_hash)
    return full_hash
