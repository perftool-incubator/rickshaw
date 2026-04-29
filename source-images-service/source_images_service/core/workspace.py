"""Workspace materialization — decode base64 request content to temp directories."""

from __future__ import annotations

import base64
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from source_images_service.models.requests import (
    Base64Directory,
    Base64File,
    SourceImagesRequest,
)

logger = logging.getLogger(__name__)


@dataclass
class WorkspacePaths:
    """Filesystem paths for a materialized job workspace."""

    root: Path
    workshop: Path = field(init=False)
    toolbox: Path = field(init=False)
    roadblock: Path = field(init=False)
    rickshaw: Path = field(init=False)
    bench_dirs: Path = field(init=False)
    config: Path = field(init=False)
    registries: Path = field(init=False)
    build: Path = field(init=False)
    workshop_script: str = "workshop.py"

    def __post_init__(self) -> None:
        self.workshop = self.root / "workshop"
        self.toolbox = self.root / "toolbox"
        self.roadblock = self.root / "roadblock"
        self.rickshaw = self.root / "rickshaw"
        self.bench_dirs = self.root / "bench-dirs"
        self.config = self.root / "config"
        self.registries = self.root / "registries"
        self.build = self.root / "build"


def _decode_and_write(path: Path, content_base64: str, executable: bool = False) -> None:
    """Decode base64 content and write to a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = base64.b64decode(content_base64)
    path.write_bytes(data)
    if executable:
        path.chmod(0o755)


def _write_file(base_dir: Path, b64file: Base64File) -> None:
    """Write a single Base64File into the base directory."""
    target = base_dir / b64file.filename
    _decode_and_write(target, b64file.content_base64)
    if b64file.mode is not None:
        target.chmod(b64file.mode)


def _write_directory(base_dir: Path, b64dir: Base64Directory) -> None:
    """Write all files from a Base64Directory into a subdirectory."""
    target_dir = base_dir / b64dir.name
    target_dir.mkdir(parents=True, exist_ok=True)
    for f in b64dir.files:
        _write_file(target_dir, f)


def materialize_workspace(
    request: SourceImagesRequest,
    base_temp_dir: str = "/tmp/source-images-service",
) -> WorkspacePaths:
    """Create a temporary directory tree with all decoded files from the request.

    Returns a WorkspacePaths pointing to the created directories.
    """
    os.makedirs(base_temp_dir, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="job-", dir=base_temp_dir))
    ws = WorkspacePaths(root=root)

    # Create all subdirectories
    for d in (ws.workshop, ws.toolbox, ws.roadblock, ws.rickshaw,
              ws.bench_dirs, ws.config, ws.registries, ws.build):
        d.mkdir(parents=True, exist_ok=True)

    file_count = 0

    # Workshop script + schema
    workshop_script = os.environ.get("WORKSHOP_SCRIPT", request.workshop.workshop_script)
    ws.workshop_script = workshop_script
    _decode_and_write(
        ws.workshop / workshop_script,
        request.workshop.workshop_script_content,
        executable=True,
    )
    file_count += 1
    _decode_and_write(ws.workshop / "schema.json", request.workshop.schema_content)
    file_count += 1
    _decode_and_write(ws.workshop / "registries-schema.json", request.workshop.registries_schema_content)
    file_count += 1

    # Toolbox directory
    for f in request.toolbox_dir.files:
        _write_file(ws.toolbox, f)
        file_count += 1

    # Toolbox workshop.json
    _decode_and_write(ws.config / "toolbox-workshop.json", request.toolbox_workshop_json)
    file_count += 1

    # Roadblock workshop.json
    _decode_and_write(ws.roadblock / "workshop.json", request.roadblock_workshop_json)
    file_count += 1

    # Rickshaw files (engine/, userenvs/, etc.)
    for f in request.rickshaw_files.files:
        _write_file(ws.rickshaw, f)
        file_count += 1

    # Benchmark directories
    for bench_name, b64dir in request.bench_dirs.items():
        _write_directory(ws.bench_dirs, b64dir)
        file_count += len(b64dir.files)

    # Utility directories
    if request.utility_dirs:
        for util_name, b64dir in request.utility_dirs.items():
            _write_directory(ws.bench_dirs, b64dir)
            file_count += len(b64dir.files)

    # Userenv files
    for userenv_name, content_b64 in request.userenv_files.items():
        _decode_and_write(ws.config / f"userenv-{userenv_name}.json", content_b64)
        file_count += 1

    # Registries
    _decode_and_write(ws.registries / "registries.json", request.registries_json_content)
    file_count += 1

    if request.push_token_content:
        for reg_type, token_b64 in request.push_token_content.items():
            _decode_and_write(ws.registries / f"push-token-{reg_type}", token_b64)
            file_count += 1

    if request.pull_token_content:
        for reg_type, token_b64 in request.pull_token_content.items():
            _decode_and_write(ws.registries / f"pull-token-{reg_type}", token_b64)
            file_count += 1

    logger.info("Materialized workspace at %s with %d files", root, file_count)
    return ws


def cleanup_workspace(workspace: WorkspacePaths) -> None:
    """Remove the temporary workspace directory tree."""
    if workspace.root.exists():
        shutil.rmtree(workspace.root)
        logger.info("Cleaned up workspace %s", workspace.root)
