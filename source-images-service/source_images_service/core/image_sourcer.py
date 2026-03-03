"""Main orchestration — port of Perl source_container_image() (L592-1009) and main loop (L1078-1089)."""

from __future__ import annotations

import base64
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from source_images_service.core.build_coordinator import BuildCoordinator
from source_images_service.core.exceptions import (
    RegistryError,
    SourceImagesError,
)
from source_images_service.core.hash_calculator import calc_image_hash
from source_images_service.core.registry_ops import (
    delete_local_image,
    local_image_found,
    push_local_image,
    remote_image_found,
)
from source_images_service.core.requirements_builder import build_reqs
from source_images_service.core.workshop_runner import workshop_build_image
from source_images_service.models.responses import JobProgress

if TYPE_CHECKING:
    from source_images_service.core.job_manager import Job
    from source_images_service.core.workspace import WorkspacePaths
    from source_images_service.models.requests import RegistryConfig, SourceImagesRequest

logger = logging.getLogger(__name__)


def _write_cs_conf(path: Path, quay_label: str | None = None) -> None:
    """Write the container-spec config JSON.

    Called twice per stage: once without quay label (for hash calculation),
    once with (for the actual build).
    """
    cs_conf: dict[str, Any] = {
        "workshop": {"schema": {"version": "2020.04.30"}},
        "config": {
            "entrypoint": ["/bin/sh", "-c", "/usr/local/bin/bootstrap"],
            "envs": ["TOOLBOX_HOME=/opt/toolbox"],
        },
    }
    if quay_label is not None:
        cs_conf["config"]["labels"] = [quay_label]

    path.write_text(json.dumps(cs_conf, indent=2) + "\n")


def _refresh_quay_expiration(
    tag: str,
    registry_type: str,
    registries: dict[str, RegistryConfig],
    quay_tokens: dict[str, str],
    job: Job,
) -> None:
    """Refresh Quay image tag expiration — port of Perl L866-965.

    Uses httpx instead of shelling out to curl.
    """
    reg = registries[registry_type]

    if reg.quay_refresh_expiration_api_url is None:
        return
    if registry_type not in quay_tokens:
        return

    # Calculate target expiration
    exp_length = reg.quay_expiration_length
    if exp_length is None:
        return

    match = re.match(r"(\d+)w", exp_length)
    if match:
        refresh_expiration = time.time() + int(match.group(1)) * 7 * 24 * 3600
    else:
        match = re.match(r"(\d+)d", exp_length)
        if match:
            refresh_expiration = time.time() + int(match.group(1)) * 24 * 3600
        else:
            raise SourceImagesError(
                f"Failed to determine quay image expiration from '{exp_length}'"
            )

    token = quay_tokens[registry_type]
    api_url = reg.quay_refresh_expiration_api_url
    headers = {"Authorization": f"Bearer {token}"}
    max_attempts = 3

    # Query current expiration
    query_url = f"{api_url}/tag/?onlyActiveTags=true&specificTag={tag}"
    current_expiration: int | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = httpx.get(query_url, headers=headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("tags") and len(data["tags"]) > 0:
                    current_expiration = data["tags"][0].get("end_ts")
                    break
                else:
                    job.append_log(
                        "\tTag information returned from query is incomplete"
                    )
            else:
                job.append_log(
                    f"\tQuay query failed (status={resp.status_code})"
                )
        except Exception as exc:
            job.append_log(f"\tQuay query exception: {exc}")

        if attempt >= max_attempts:
            msg = f"\tFailed to query for tag information on {max_attempts} attempts"
            job.append_log(msg)
            if reg.quay_refresh_expiration_require_success:
                raise SourceImagesError(msg)
            return
        time.sleep(1)

    if current_expiration is None:
        return

    # Check if refresh is needed
    time_buffer = 1 * 24 * 3600  # 1 day in seconds

    if current_expiration >= refresh_expiration:
        ts_str = datetime.fromtimestamp(
            current_expiration, tz=timezone.utc
        ).strftime("%c")
        job.append_log(
            f"\tThe current expiration ({current_expiration} / {ts_str}) "
            "is greater than or equal to the refresh expiration so not refreshing"
        )
        return

    if (refresh_expiration - current_expiration) <= time_buffer:
        ts_str = datetime.fromtimestamp(
            current_expiration, tz=timezone.utc
        ).strftime("%c")
        job.append_log(
            f"\tSkipping expiration refresh because the current expiration "
            f"({current_expiration} / {ts_str}) is less than 1 day(s) "
            f"({time_buffer} seconds) older than the new expiration"
        )
        return

    # Refresh expiration
    refresh_url = f"{api_url}/tag/{tag}"
    refresh_headers = {**headers, "Content-type": "application/json"}
    refresh_body = {"expiration": int(refresh_expiration)}

    for attempt in range(1, max_attempts + 1):
        try:
            resp = httpx.put(
                refresh_url,
                headers=refresh_headers,
                json=refresh_body,
                timeout=30,
            )
            if resp.status_code in (200, 201) and resp.text.strip().strip('"') == "Updated":
                ts_str = datetime.fromtimestamp(
                    current_expiration, tz=timezone.utc
                ).strftime("%c")
                job.append_log(
                    f"\tRefreshed expiration (was {current_expiration} / {ts_str})"
                )
                return
            elif resp.status_code not in (200, 201):
                job.append_log(
                    f"\tQuay refresh PUT failed (status={resp.status_code}): {resp.text}"
                )
            else:
                job.append_log(
                    f"\tQuay refresh PUT returned {resp.status_code} but unexpected body: {resp.text!r}"
                )
        except Exception as exc:
            job.append_log(f"\tQuay refresh exception: {exc}")

        if attempt >= max_attempts:
            ts_str = datetime.fromtimestamp(
                current_expiration, tz=timezone.utc
            ).strftime("%c")
            msg = (
                f"\tFailed to refresh expiration on {max_attempts} attempts "
                f"(currently {current_expiration} / {ts_str})"
            )
            job.append_log(msg)
            if reg.quay_refresh_expiration_require_success:
                raise SourceImagesError(
                    "Expiration refresh success is required"
                )
            else:
                job.append_log(
                    "\tExpiration refresh success is not required, continuing"
                )
            return
        time.sleep(1)


def _source_container_image(
    userenv: str,
    benchmark: str,
    arch: str,
    request: SourceImagesRequest,
    workspace: WorkspacePaths,
    job: Job,
    workshop_built_tags: dict[str, int],
    build_coordinator: BuildCoordinator,
) -> str:
    """Source a single container image — port of Perl source_container_image() (L592-1009).

    Returns the final image URL string.
    """
    local_images: list[str] = []

    # Load userenv JSON to determine registry type
    userenv_file_path = workspace.config / f"userenv-{userenv}.json"
    if not userenv_file_path.exists():
        raise SourceImagesError(
            f"Could not locate the requested userenv: {userenv}"
        )

    userenv_data = json.loads(userenv_file_path.read_text())

    registry_type = "public"
    origin = userenv_data.get("userenv", {}).get("origin", {})
    if origin.get("requires-pull-token") == "true":
        registry_type = "private"

    if registry_type == "public":
        job.append_log(
            f"Userenv {userenv} does not require a pull token "
            "so it comes from a public registry repo."
        )
    else:
        job.append_log(
            f"Userenv {userenv} does require a pull token "
            "so it comes from a private registry repo."
        )

    if registry_type not in request.registries:
        raise SourceImagesError(
            f"The requested userenv is a {registry_type} image but there is "
            f"not a defined {registry_type} engine registry!"
        )

    reg = request.registries[registry_type]
    cs_conf_file = workspace.config / "cs-conf.json"

    # Build workshop base command
    workshop_base_cmd = (
        f"{workspace.workshop}/workshop.pl"
        f" --log-level verbose"
        f" --config {cs_conf_file}"
        f" --param %bench-dir%={workspace.bench_dirs}/{benchmark}"
        f" --param %engine-dir%={workspace.rickshaw}/engine/"
        f" --param %rickshaw-dir%={workspace.rickshaw}"
        f" --reg-tls-verify={str(reg.tls_verify).lower()}"
        f" --registries-json={workspace.registries}/registries.json"
        f" 2>&1"
    )

    job.append_log(
        f"Sourcing container image for userenv '{userenv}' ({registry_type}) "
        f"and benchmark/tool '{benchmark}'; this may take a few minutes"
    )
    logger.info("[%s] Sourcing image: userenv=%s benchmark=%s (%s)",
                job.id[:8], userenv, benchmark, registry_type)

    # Build ordered requirements list
    requirements = build_reqs(
        userenv, benchmark, workspace, request.utilities, request.use_workshop
    )

    # Build staged workshop_args list
    workshop_args: list[dict[str, str]] = []
    userenv_arg = ""
    count = 0
    userenv_image = f"{origin.get('image', '')}:{origin.get('tag', '')}"

    while len(requirements) > 0:
        if count == 0:
            userenv_arg = f" --userenv {userenv_file_path}"
            req_arg = ""
            update_policy = origin.get("update-policy", "")
            if update_policy == "never":
                skip_update = "true"
            else:
                skip_update = "false"
        else:
            req_arg = requirements.pop(0)
            skip_update = "true"

        # Write cs-conf.json without quay label (for hash calculation)
        _write_cs_conf(cs_conf_file)

        tag = calc_image_hash(
            workshop_base_cmd,
            userenv_arg,
            req_arg if req_arg else None,
            arch,
            userenv,
            benchmark,
            len(workshop_args) + 1,
            workspace,
            job=job,
        )

        # Rewrite cs-conf.json with quay label
        quay_exp = request.registries.get("public", reg)
        exp_length = quay_exp.quay_expiration_length or ""
        _write_cs_conf(cs_conf_file, quay_label=f"quay.expires-after={exp_length}")

        workshop_args.append({
            "userenv": userenv_arg,
            "reqs": req_arg,
            "tag": tag,
            "skip-update": skip_update,
        })

        count += 1

        if len(requirements) > 0:
            # Create intermediate userenv JSON pointing to previous stage's image
            userenv_data.pop("requirements", None)
            userenv_data["requirements"] = []
            userenv_data["userenv"]["origin"]["image"] = (
                reg.url_details.dest_image_url
            )
            userenv_data["userenv"]["origin"]["tag"] = tag
            userenv_data["userenv"]["origin"].pop("build-policy", None)

            intermediate_file = workspace.config / f"userenv-{tag}.json"
            intermediate_file.write_text(json.dumps(userenv_data, indent=2) + "\n")
            userenv_arg = f" --userenv {intermediate_file}"

    logger.debug("workshop_args: %s", workshop_args)

    num_images = len(workshop_args)
    force_builds = request.workshop.force_builds

    if not force_builds:
        # Search for existing stages, most-complete first
        job.append_log(
            f"Searching for existing stages (1 to {num_images}, "
            f"{num_images} being most complete)"
        )
        i = num_images - 1
        while i >= 0:
            logger.debug(
                "Checking for stage number %d (of %d)", i + 1, num_images
            )
            if not remote_image_found(
                workshop_args[i]["tag"],
                registry_type,
                request.registries,
                workspace,
                job=job,
            ):
                if not local_image_found(
                    workshop_args[i]["tag"],
                    registry_type,
                    request.registries,
                    job=job,
                ):
                    i -= 1
                    continue
                else:
                    job.append_log(
                        "\tCould not find image remotely but found it "
                        "locally so pushing it..."
                    )
                    begin = time.time()
                    push_local_image(
                        workshop_args[i]["tag"],
                        registry_type,
                        request.registries,
                        workspace,
                        job=job,
                    )
                    end = time.time()
                    job.append_log(
                        f"\t\tPushing took {int(end - begin)} seconds"
                    )
                    break
            else:
                break

        if i == -1:
            job.append_log("Did not find any existing stages")
            logger.info("[%s] No existing stages found, building all %d", job.id[:8], num_images)
        elif i < num_images - 1:
            job.append_log(
                f"Found stage number {i + 1} (of {num_images}), "
                f"need to build {num_images - 1 - i} stage(s)"
            )
            logger.info("[%s] Found stage %d of %d, building %d remaining",
                        job.id[:8], i + 1, num_images, num_images - 1 - i)
        elif i == num_images - 1:
            job.append_log(
                f"Found most complete stage (number {i + 1})"
            )
            logger.info("[%s] All %d stages found, no build needed", job.id[:8], num_images)
        else:
            raise SourceImagesError(
                f"Something went wrong, stage number: {i + 1}, "
                f"num_images: {num_images}"
            )
    else:
        # Force-builds mode: check workshop_built_tags cache
        job.append_log(
            f"Image building is forced, checking if any of the needed stages "
            f"were already built during this run (1 to {num_images}, "
            f"{num_images} being most complete)"
        )
        i = num_images - 1
        while i >= 0:
            logger.debug(
                "Checking for stage number %d (of %d)", i + 1, num_images
            )
            if workshop_args[i]["tag"] in workshop_built_tags:
                if not remote_image_found(
                    workshop_args[i]["tag"],
                    registry_type,
                    request.registries,
                    workspace,
                    job=job,
                ):
                    if not local_image_found(
                        workshop_args[i]["tag"],
                        registry_type,
                        request.registries,
                        job=job,
                    ):
                        raise SourceImagesError(
                            "Cannot find the image I was previously forced "
                            f"to build ({workshop_args[i]['tag']})"
                        )
                    else:
                        push_local_image(
                            workshop_args[i]["tag"],
                            registry_type,
                            request.registries,
                            workspace,
                            job=job,
                        )
                        break
                else:
                    break
            else:
                i -= 1
                continue

        if i == -1:
            job.append_log(
                "Did not find any existing stages built during this run"
            )
        elif i < num_images - 1:
            job.append_log(
                f"Found stage number {i + 1} (of {num_images}) built "
                f"during this run, need to build {num_images - 1 - i} stage(s)"
            )
        elif i == num_images - 1:
            job.append_log(
                f"Found most complete stage built during this run "
                f"(number {i + 1})"
            )
        else:
            raise SourceImagesError(
                "Something went wrong searching for stages built during "
                f"this run, stage number: {i + 1}, num_images: {num_images}"
            )

    image = (
        f"{reg.url_details.dest_image_url}:{workshop_args[i]['tag']}"
    )

    # Build any remaining stages
    i += 1
    if i == 0:
        # First stage pulls a base userenv image — track for later deletion
        local_images.append(userenv_image)

    # Determine if we should refresh Quay expirations on existing stages
    quay_tokens = request.quay_refresh_expiration_tokens or {}
    has_quay_refresh = (
        registry_type in quay_tokens
        and reg.quay_refresh_expiration_api_url is not None
    )

    if has_quay_refresh:
        exp_length = reg.quay_expiration_length or ""
        match = re.match(r"(\d+)w", exp_length)
        if match:
            refresh_ts = time.time() + int(match.group(1)) * 7 * 24 * 3600
        else:
            match = re.match(r"(\d+)d", exp_length)
            if match:
                refresh_ts = time.time() + int(match.group(1)) * 24 * 3600
            else:
                raise SourceImagesError(
                    f"Failed to determine quay image expiration from '{exp_length}'"
                )
        ts_str = datetime.fromtimestamp(
            refresh_ts, tz=timezone.utc
        ).strftime("%c")
        job.append_log(
            f"Going to refresh image expiration with this value: "
            f"{int(refresh_ts)} ({ts_str})"
        )

    # Refresh expirations on already-existing stages
    x = 0
    while x < i:
        job.append_log(
            f"Processing stage {x + 1} ({workshop_args[x]['tag']})..."
        )
        if has_quay_refresh:
            if remote_image_found(
                workshop_args[x]["tag"],
                registry_type,
                request.registries,
                workspace,
                job=job,
            ):
                _refresh_quay_expiration(
                    workshop_args[x]["tag"],
                    registry_type,
                    request.registries,
                    quay_tokens,
                    job,
                )
            else:
                job.append_log(
                    "\tskipping expiration refresh because remote "
                    "image does not exist (!!)"
                )
        job.append_log("\tReady")
        x += 1

    # Build missing stages
    while i <= num_images - 1:
        tag = workshop_args[i]["tag"]
        job.append_log(
            f"Processing stage {i + 1} ({tag})..."
        )

        acquired = False

        # Non-force-builds: coordinate with other jobs building the same tag
        if not force_builds:
            wait_event = build_coordinator.try_acquire(tag)
            if wait_event is not None:
                job.append_log(
                    f"\tAnother job is already building tag {tag}, waiting..."
                )
                logger.info("[%s] Waiting for another builder of tag %s",
                            job.id[:8], tag)
                wait_start = time.time()
                wait_event.wait()
                wait_duration = int(time.time() - wait_start)
                job.append_log(
                    f"\tOther builder finished (waited {wait_duration}s)"
                )
                logger.info("[%s] Wait for tag %s finished (%ds)",
                            job.id[:8], tag, wait_duration)

                # Check registry — if image landed, skip build+push
                if remote_image_found(
                    tag,
                    registry_type,
                    request.registries,
                    workspace,
                    job=job,
                ):
                    job.append_log(
                        "\tImage found in registry after wait, skipping build"
                    )
                    logger.info("[%s] Skipping build of tag %s (found after wait)",
                                job.id[:8], tag)
                    workshop_built_tags[tag] = 1
                    image = f"{reg.url_details.dest_image_url}:{tag}"
                    i += 1
                    continue
                else:
                    job.append_log(
                        "\tImage NOT found after wait, building it ourselves"
                    )
                    logger.warning("[%s] Tag %s not found after wait, building",
                                   job.id[:8], tag)
                    # Fall through to build without re-acquiring
            else:
                acquired = True

        try:
            job.append_log(f"\tBuilding stage {i + 1} ({tag})")
            logger.info("[%s] Building stage %d (%s)", job.id[:8], i + 1, tag)
            begin = time.time()
            workshop_build_image(
                userenv,
                registry_type,
                benchmark,
                workshop_base_cmd,
                i + 1,
                workshop_args[i]["userenv"],
                workshop_args[i]["reqs"] if workshop_args[i]["reqs"] else None,
                tag,
                workshop_args[i]["skip-update"],
                request.registries,
                workspace,
                job=job,
            )
            end = time.time()
            job.append_log(f"\tBuilding took {int(end - begin)} seconds")
            logger.info("[%s] Built stage %d in %ds", job.id[:8], i + 1, int(end - begin))

            # Check for race condition — skip push if image now exists remotely
            if remote_image_found(
                tag,
                registry_type,
                request.registries,
                workspace,
                job=job,
            ):
                job.append_log(
                    "\tSkipping push because image now exists remotely"
                )
            else:
                begin = time.time()
                push_local_image(
                    tag,
                    registry_type,
                    request.registries,
                    workspace,
                    job=job,
                )
                end = time.time()
                job.append_log(f"\tPushing took {int(end - begin)} seconds")
                logger.info("[%s] Pushed stage %d in %ds", job.id[:8], i + 1, int(end - begin))
        finally:
            if acquired:
                build_coordinator.release(tag)

        workshop_built_tags[tag] = 1
        local_images.append(tag)
        image = f"{reg.url_details.dest_image_url}:{tag}"
        i += 1

    # Delete all local images (including the initial userenv base image)
    if local_images:
        job.append_log(f"Deleting {len(local_images)} local images")
        while local_images:
            img = local_images.pop()
            try:
                delete_local_image(
                    img, registry_type, request.registries, job=job
                )
            except RegistryError as exc:
                # Log but don't fail on cleanup errors
                job.append_log(f"Warning: failed to delete local image: {exc}")
                logger.warning("Failed to delete local image %s: %s", img, exc)

    job.append_log(
        f"Finished sourcing container image for userenv '{userenv}' "
        f"({registry_type}) and benchmark/tool '{benchmark}'"
    )

    # Append pull-token path for private registries
    if registry_type == "private":
        image += f"::{workspace.registries}/pull-token-{registry_type}"

    return image


def _collect_result_files(
    workspace: WorkspacePaths,
    original_userenvs: set[str],
) -> dict[str, list[dict[str, str]]]:
    """Collect build artifact files from the workspace for return to the client.

    Gathers:
    - All files from workspace.build/ (tag-calc-data and build stdout files)
    - Intermediate userenv-*.json files from workspace.config/ that were
      generated during multi-stage builds (excludes original userenvs)

    Each file's content is base64-encoded for transport over JSON.
    """
    result: dict[str, list[dict[str, str]]] = {"config": [], "workshop": []}

    # Collect build artifacts (tag-calc-data, stdout files)
    if workspace.build.is_dir():
        for fpath in sorted(workspace.build.iterdir()):
            if fpath.is_file():
                result["workshop"].append({
                    "filename": fpath.name,
                    "content_base64": base64.b64encode(
                        fpath.read_bytes()
                    ).decode("ascii"),
                })

    # Collect intermediate userenv JSON files
    if workspace.config.is_dir():
        for fpath in sorted(workspace.config.glob("userenv-*.json")):
            # Extract the tag from "userenv-<tag>.json"
            stem = fpath.stem  # "userenv-<tag>"
            tag = stem[len("userenv-"):]
            if tag not in original_userenvs:
                result["config"].append({
                    "filename": fpath.name,
                    "content_base64": base64.b64encode(
                        fpath.read_bytes()
                    ).decode("ascii"),
                })

    config_count = len(result["config"])
    workshop_count = len(result["workshop"])
    logger.debug(
        "Collected %d config and %d workshop result files",
        config_count, workshop_count,
    )

    return result


def source_all_images(job: Job, build_coordinator: BuildCoordinator) -> dict[str, Any]:
    """Top-level entry point — port of Perl main loop (L1078-1089).

    Iterates sorted benchmarks x sorted userenvs, calls
    _source_container_image() for each pair, updates job.progress,
    and returns ``{"image-ids": {...}}``.
    """
    assert job.request is not None
    assert job.workspace is not None

    request = job.request
    workspace = job.workspace

    image_ids: dict[str, dict[str, dict[str, Any]]] = {}
    workshop_built_tags: dict[str, int] = {}
    completed = 0

    job.append_log("rickshaw-source-images: starting image sourcing")
    logger.info("[%s] Starting image sourcing", job.id[:8])

    for benchmark in sorted(request.image_ids.keys()):
        job.append_log(f"Working on {benchmark} benchmark or tool")
        logger.info("[%s] Working on %s", job.id[:8], benchmark)
        image_ids[benchmark] = {}

        for userenv in sorted(request.image_ids[benchmark].keys()):
            job.append_log(f"Working on {userenv} userenv")

            job.progress = JobProgress(
                current_benchmark=benchmark,
                completed_items=completed,
                total_items=job.progress.total_items,
            )

            image = _source_container_image(
                userenv,
                benchmark,
                request.arch,
                request,
                workspace,
                job,
                workshop_built_tags,
                build_coordinator,
            )

            job.append_log(f"Image is: {image}")
            image_ids[benchmark][userenv] = {"image": image}
            completed += 1

    job.progress = JobProgress(
        completed_items=completed,
        total_items=job.progress.total_items,
    )

    job.append_log("rickshaw-source-images: image sourcing complete")
    logger.info("[%s] Image sourcing complete", job.id[:8])

    original_userenvs = set(request.userenv_files.keys())
    result_files = _collect_result_files(workspace, original_userenvs)

    return {"image-ids": image_ids, "result-files": result_files}
