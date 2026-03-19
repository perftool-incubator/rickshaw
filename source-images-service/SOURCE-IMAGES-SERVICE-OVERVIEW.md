# Source-Images-Service: Program Flow Overview

## High-Level Architecture

There are three components that work together:

1. **`rickshaw-run`** (Perl) — the orchestrator that decides to source images
2. **`rickshaw-source-images-client`** (Python) — a CLI bridge that translates local file paths into an HTTP API call
3. **`source-images-service`** (Python/FastAPI) — the web service that does the actual work

```
rickshaw-run (Perl)
    │  writes input JSON (validated against schema)
    ▼
rickshaw-source-images-client (Python)
    │  reads files, base64-encodes everything, POSTs to service
    ▼
source-images-service (FastAPI)
    │  decodes files to temp workspace, builds/finds images
    │  client polls for status + streams logs
    ▼
rickshaw-source-images-client
    │  writes output JSON + result files back to disk
    ▼
rickshaw-run (Perl)
    reads output JSON (validated against schema), continues with image IDs
```

---

## 1. Service Startup

**`main.py`** — Entry point registered as `source-images-service` console script.

- `run()` (line 89): Loads `ServiceConfig` from `SIS_`-prefixed env vars, calls `create_app()`, starts uvicorn.
- `create_app()` (line 74): Creates FastAPI app with a lifespan context manager.
- `lifespan()` (line 56): On startup, configures logging and creates a **`JobManager`**. On shutdown, calls `job_manager.shutdown()`.

**`config.py`** — `ServiceConfig` (pydantic-settings):

| Env Var | Default | Purpose |
|---------|---------|---------|
| `SIS_HOST` | `0.0.0.0` | Bind address |
| `SIS_PORT` | `8080` | HTTP port |
| `SIS_WORKER_POOL_SIZE` | `4` | Thread pool size |
| `SIS_JOB_TTL_HOURS` | `24` | Job retention |
| `SIS_TEMP_DIR` | `/tmp/source-images-service` | Workspace root |
| `SIS_LOG_LEVEL` | `info` | Log level |

---

## 2. API Endpoints

**`api/v1/router.py`** aggregates three routers under `/api/v1/`:

| Endpoint | Method | File | Purpose |
|----------|--------|------|---------|
| `/health` | GET | `health.py` | Returns service status, version, active/pending job counts |
| `/source-images` | POST | `source_images.py` | Accepts a job, returns 202 with job-id |
| `/jobs/{job_id}` | GET | `jobs.py` | Returns job status, progress, result, or error |
| `/jobs/{job_id}/log?offset=N` | GET | `jobs.py` | Returns log text from byte offset N |

The POST endpoint calls `job_manager.submit_job(request)` and returns immediately — all real work happens asynchronously in a worker thread.

---

## 3. Request & Response Models

**`models/requests.py`** — `SourceImagesRequest` contains everything the service needs, fully self-contained:
- `arch` — CPU architecture (x86_64, aarch64)
- `image-ids` — benchmark → userenv → {} mapping (what to source)
- `registries` — registry configs with URLs, TLS settings, quay expiration settings
- `bench-dirs`, `utility-dirs`, `toolbox-dir`, `rickshaw-files` — all as `Base64Directory` objects (lists of `Base64File` with filename, content, permission mode)
- `workshop` — `WorkshopConfig` with workshop script content, script filename, schema files, force-builds flag
- `userenv-files` — userenv name → base64 JSON content
- Various tokens (push, pull, quay refresh) as base64 strings

**`models/responses.py`**:
- `JobStatus` enum: `pending → running → completed / failed`
- `JobProgress`: current-benchmark, completed-items, total-items
- `SubmitJobResponse`: job-id + status
- `JobStatusResponse`: job-id + status + progress + result + error
- `JobLogResponse`: job-id + log text + offset + total-length

---

## 4. Job Management

**`core/job_manager.py`**:

**`Job`** dataclass (line 27): Holds id, status, progress, result, error, log_buffer, timestamps, request, workspace, and a threading lock for log safety.
- `append_log(msg)` — thread-safe append to log_buffer (what the client streams)
- `append_debug_log(msg)` — only appends when log_level is "debug"

**`JobManager`** (line 55): Owns a `ThreadPoolExecutor`, a `BuildCoordinator`, and an in-memory `dict[str, Job]`.

- **`submit_job()`** (line 71): Creates Job with UUID, calculates total_items from the image-ids structure, stores in dict, submits `_run_job` to thread pool, returns immediately.

- **`_run_job()`** (line 145): The worker thread entry point:
  1. Sets status = RUNNING
  2. Calls `materialize_workspace()` — decodes all base64 content to a temp directory
  3. Calls `source_all_images(job, build_coordinator)` — the core engine
  4. On success: sets result + status=COMPLETED
  5. On exception: sets error + status=FAILED
  6. Finally: cleans up workspace, releases request reference

---

## 5. Workspace Materialization

**`core/workspace.py`**:

`materialize_workspace()` (line 72) creates a temp directory tree and decodes all base64 content from the request into it:

```
/tmp/source-images-service/job-<random>/
  ├── workshop/       workshop script (executable), schema.json, registries-schema.json
  ├── toolbox/        toolbox files
  ├── roadblock/      roadblock workshop.json
  ├── rickshaw/       engine/, userenvs/
  ├── bench-dirs/     benchmark & utility directories
  ├── config/         userenv-*.json files
  ├── registries/     registries.json, push/pull tokens
  └── build/          (output: tag-calc-data, build logs)
```

Returns a `WorkspacePaths` dataclass with paths to each subdirectory.

---

## 6. Image Sourcing — The Core Engine

**`core/image_sourcer.py`** (~859 lines) — This is where the real work happens.

### `source_all_images()` (line 799)

Top-level orchestrator. Iterates sorted benchmarks × sorted userenvs, calling `_source_container_image()` for each pair and updating `job.progress` after each.

### `_source_container_image()` (line 210) — ~530 lines

This is the big function. For a single (benchmark, userenv) pair, it:

**Stage 1 — Determine registry type** (lines 226-249):
- Reads userenv JSON, checks `requires-pull-token`
- Sets registry_type = "public" or "private"

**Stage 2 — Build requirements list** (lines 280-283):
- Calls `build_reqs()` from `requirements_builder.py`
- Returns ordered `--requirement` args: toolbox → toolbox-workshop → roadblock → rickshaw-engine → utilities → benchmark
- Order goes from most stable (reusable base layers) to least stable (changes often)

**Stage 3 — Calculate multi-stage build plan** (lines 285-345):
- For each requirement level, computes a SHA256 tag via `calc_image_hash()` from `hash_calculator.py`
- Each stage builds on the previous stage's image
- Creates intermediate userenv JSON files for each stage

**Stage 4 — Search for existing stages** (lines 352-492):
- Searches backward from the final (most-complete) stage
- For each stage, checks: remote registry (`skopeo inspect`) → local store (`buildah images`)
- If found locally but not remotely, pushes it (and tracks the image for later cleanup)
- Determines the starting build index — only builds what's missing
- Tracks an `existing_stages` set recording which stages were confirmed present
- After finding the most-complete stage, verifies which earlier stages still exist in the registry (they may have expired in Quay), logging present/missing stages

**Stage 5 — Refresh Quay expirations** (lines 595-614):
- For stages in the `existing_stages` set, refreshes their tag expiration dates via Quay API
- Skips stages not confirmed to exist (avoids redundant remote checks since existence was already verified in Stage 4)

**Stage 6 — Build missing stages** (lines 615-721):
- For each missing stage:
  - **Coordination**: Calls `build_coordinator.try_acquire(tag)`. If another job is already building this tag, waits on a `threading.Event` and then checks if the image landed in the registry
  - **Build**: Calls `workshop_build_image()` which runs `workshop.pl` as a subprocess
  - **Push**: Calls `push_local_image()` (buildah push, with up to 5 retries and post-push verification polling)
  - **Release**: Calls `build_coordinator.release(tag)` to wake any waiting threads

**Stage 7 — Clean up local images** (lines 723-735):
- Deletes all locally-pulled/built images via `buildah rmi` (includes images found locally and pushed during the search phase)

**Stage 8 — Return result** (lines 737-746):
- Returns the final image URL as `{registry_url}:{final_tag}::{pull_token_path}`

---

## 7. Supporting Modules

| Module | Purpose |
|--------|---------|
| **`build_coordinator.py`** | Cross-job deduplication. `try_acquire(tag)` returns None if you're the builder, or an Event to wait on. `release(tag)` wakes waiters. |
| **`hash_calculator.py`** | Runs workshop script with `--dump-config` and `--dump-files` to discover inputs, then SHA256-hashes config text + all file contents. Normalizes workspace paths for determinism. |
| **`requirements_builder.py`** | Builds ordered requirement list for multi-stage builds. Writes a dynamic `toolbox-req.json`. |
| **`workshop_runner.py`** | Executes workshop script subprocess, captures output, parses JSON result from end of output, returns image ID. |
| **`registry_ops.py`** | `remote_image_found()` (skopeo), `local_image_found()` (buildah images), `push_local_image()` (buildah push with retries), `delete_local_image()` (buildah rmi). |
| **`subprocess_runner.py`** | `run_cmd()` wrapper around subprocess.run. Shell=True, merged stdout+stderr, optional timeout, optional debug logging to job. |
| **`exceptions.py`** | `SourceImagesError` base → `SubprocessError`, `RegistryError`, `HashCalculationError`, `WorkshopBuildError`. |

---

## 8. The Client Side

**`rickshaw-source-images-client`** (690 lines of Python, no pip dependencies):

1. Reads input JSON from rickshaw-run and validates it against `schema/source-images-input.json` (via `toolbox.json.validate_schema`)
2. `_build_api_request()` — reads all those files and base64-encodes them into a `SourceImagesRequest`-shaped dict
3. `_health_check()` — GET `/api/v1/health`
4. `_submit_job()` — POST `/api/v1/source-images`, gets job-id
5. `_poll_job()` — adaptive polling loop (2s → 15s intervals):
   - GET `/api/v1/jobs/{job-id}` for status/progress
   - GET `/api/v1/jobs/{job-id}/log?offset=N` for log streaming (prints to stdout)
   - Continues until completed or failed
6. `_write_result_files()` — decodes base64 result files back to disk
7. Validates output against `schema/source-images-output.json`, then writes output JSON with image-ids for rickshaw-run to consume

---

## 9. Call Chain Summary

```
rickshaw-run
  └─ rickshaw-source-images-client
       └─ POST /api/v1/source-images
            └─ JobManager.submit_job()
                 └─ ThreadPool → _run_job()
                      ├─ materialize_workspace()
                      └─ source_all_images()
                           └─ _source_container_image()  [per benchmark×userenv]
                                ├─ build_reqs()
                                ├─ calc_image_hash()      [per stage]
                                ├─ remote_image_found()   [search existing]
                                ├─ build_coordinator.try_acquire()
                                ├─ workshop_build_image()  [workshop script subprocess]
                                ├─ push_local_image()      [buildah push]
                                ├─ build_coordinator.release()
                                └─ delete_local_image()    [cleanup]
       └─ Poll GET /api/v1/jobs/{id} + /log until done
       └─ Write results back to disk
  └─ rickshaw-run reads output, continues
```
