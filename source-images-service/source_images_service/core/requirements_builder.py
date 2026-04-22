"""Ordered requirement list builder — port of Perl build_reqs() (L505-583)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from source_images_service.core.workspace import WorkspacePaths

logger = logging.getLogger(__name__)

# Hardcoded toolbox-req.json schema matching the Perl original
_TOOLBOX_REQ_SCHEMA = {
    "workshop": {
        "schema": {
            "version": "2020.03.02",
        },
    },
    "userenvs": [
        {
            "name": "default",
            "requirements": ["toolbox"],
        },
    ],
    "requirements": [
        {
            "name": "toolbox",
            "type": "files",
            "files_info": {
                "files": [
                    {
                        "src": None,  # filled in at runtime with toolbox_home
                        "dst": "/opt/toolbox",
                    },
                ],
            },
        },
    ],
}


def _write_toolbox_req_json(config_dir: Path, toolbox_home: Path) -> Path:
    """Write the toolbox-req.json file into *config_dir* and return its path."""
    req = json.loads(json.dumps(_TOOLBOX_REQ_SCHEMA))  # deep copy
    req["requirements"][0]["files_info"]["files"][0]["src"] = str(toolbox_home)

    req_path = config_dir / "toolbox-req.json"
    req_path.write_text(json.dumps(req, indent=2) + "\n")
    return req_path


@dataclass
class StageRequirement:
    """A workshop requirement argument paired with the repos it depends on."""
    req_arg: str
    repos: list[str]
    description: str = ""


def build_reqs(
    userenv: str,
    benchmark: str,
    workspace_paths: WorkspacePaths,
    utilities: list[str],
    use_workshop: bool,
) -> list[StageRequirement]:
    """Build an ordered list of stage requirements for workshop script.

    The ordering ensures that the most commonly shared, least-frequently-changed
    requirements come first so that incremental image builds can reuse earlier
    layers.

    Each entry includes the ``--requirement`` argument and a list of repo names
    whose provenance is relevant to that stage.

    Returns an empty list if *use_workshop* is False.
    """
    if not use_workshop:
        return []

    reqs: list[StageRequirement] = []

    # 1. Toolbox files requirement (write config JSON, then reference it)
    tb_req = _write_toolbox_req_json(workspace_paths.config, workspace_paths.toolbox)
    reqs.append(StageRequirement(f"--requirement {tb_req}", ["toolbox"], "toolbox files"))

    # 2. Toolbox workshop.json (dependency installation)
    reqs.append(StageRequirement(
        f"--requirement {workspace_paths.config / 'toolbox-workshop.json'}",
        ["toolbox"],
        "toolbox dependencies",
    ))

    # 3. Roadblock workshop.json (Python libraries)
    reqs.append(StageRequirement(
        f"--requirement {workspace_paths.roadblock / 'workshop.json'}",
        ["roadblock"],
        "roadblock dependencies",
    ))

    # 4. Rickshaw engine workshop.json
    reqs.append(StageRequirement(
        f"--requirement {workspace_paths.rickshaw / 'engine' / 'workshop.json'}",
        ["rickshaw"],
        "rickshaw engine",
    ))

    # 5. Utility workshop.json files (if they exist)
    for utility in utilities:
        utility_req = workspace_paths.bench_dirs / utility / "workshop.json"
        if utility_req.exists():
            reqs.append(StageRequirement(
                f"--requirement {utility_req}", [utility], f"{utility} requirements",
            ))

    # 6. Benchmark workshop.json (last — most likely to change)
    reqs.append(StageRequirement(
        f"--requirement {workspace_paths.bench_dirs / benchmark / 'workshop.json'}",
        [benchmark],
        f"{benchmark} requirements",
    ))

    return reqs
