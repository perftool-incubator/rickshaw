# Rickshaw

Rickshaw is a benchmark orchestration framework that manages container image builds, benchmark execution, and result collection across multiple endpoints (Kubernetes, remote hosts).

## Project Structure

- **`rickshaw-run.py`** — Main orchestrator script. Parses CLI args, validates environment, coordinates image sourcing, and manages benchmark execution across endpoints.
- **`rickshaw-source-images-client.py`** (Python, no pip deps) — CLI bridge that translates local files into HTTP API calls to the source-images-service.
- **`source-images-service/`** (Python/FastAPI) — Web service for container image building. See `SOURCE-IMAGES-SERVICE-OVERVIEW.md` for detailed architecture.
- **`endpoints/`** — Endpoint implementations (kube, remotehosts, etc.) in Python.
- **`engine/`** — Engine scripts for benchmark/tool execution inside containers.
- **`userenvs/`** — User environment definitions (JSON files describing container base images).
- **`schema/`** — JSON schemas for validation (`run.json`, `source-images-input.json`, `source-images-output.json`, etc.).
- **`util/`** — Utility scripts (CI job generation, etc.).

## Languages

- **Python 3.10+**: `rickshaw-run.py`, `rickshaw-post-process-bench.py`, `rickshaw-post-process-tools.py`, `rickshaw-gen-docs.py`, `rickshaw-source-images-client.py`, `source-images-service/`, `endpoints/`, `util/`
- **Bash**: `engine/` scripts
- **JSON**: Schema definitions, configuration files

## Key Conventions

- CLI arguments use `--kebab-case` (e.g., `--workshop-dir`, `--bench-params`)
- JSON keys use `kebab-case` (e.g., `"force-builds"`, `"workshop-script"`)
- Python Pydantic models use `snake_case` fields with `alias="kebab-case"` for JSON serialization
- Commit messages follow conventional commits: `feat:`, `fix:`, etc.
- Schema validation is enforced at boundaries between components

## source-images-service (Python)

Located in `source-images-service/`. Uses FastAPI + Pydantic.

```
pip install -e source-images-service/   # editable install
```

Key modules under `source_images_service/`:
- `models/requests.py` — Pydantic request models
- `models/responses.py` — Pydantic response models
- `core/workspace.py` — Temp directory materialization from base64 request content
- `core/image_sourcer.py` — Core image sourcing engine
- `core/hash_calculator.py` — Image tag hash computation
- `core/workshop_runner.py` — Workshop script subprocess execution
- `core/requirements_builder.py` — Multi-stage build requirement ordering
- `core/registry_ops.py` — Container registry operations (skopeo, buildah)
- `core/build_coordinator.py` — Cross-job build deduplication

## Workshop Integration

The workshop script (`workshop.py`) builds container images. The `--workshop-script` flag (or `WORKSHOP_SCRIPT` env var) can override the default. This flows through:
1. `rickshaw-run` → input JSON (`workshop.script`)
2. `rickshaw-source-images-client.py` → reads from JSON/env
3. `source-images-service` → `WorkshopConfig.workshop_script` field

## Multi-Architecture Image Sourcing

Endpoints detect and report their CPU architecture(s) during validation via the `arch` keyword (e.g., `arch x86_64 aarch64`). The kube endpoint reads `node.status.nodeInfo.architecture` from `kubectl get nodes --output json` and normalizes K8s names to Linux names (`amd64` → `x86_64`, `arm64` → `aarch64`). The remotehosts endpoint runs `uname -m` on each remote host.

`rickshaw-run.py` collects required architectures across all endpoints and routes image sourcing requests to per-arch service URLs read from `image-sourcing-urls.json` (written by crucible's `bin/_main`). When multiple architectures are needed, sourcing runs in parallel. Image specifications are passed to endpoints via a structured JSON file (`image-map.json`, schema in `schema/image-map.json`) using `--image-map=<filepath>`. The JSON maps `bench → role → userenv → arch → {image, auth-file}`. The `get_image()` and `get_engine_id_image()` functions in `endpoints/endpoints.py` accept an optional `arch` parameter and return a dict with `image` (URL) and optional `auth-file` keys, or None.

For the kube endpoint, the `arch` setting in `schema/kube.json` lets users target a specific architecture without writing a raw `kubernetes.io/arch` nodeSelector. On multi-arch clusters without explicit arch constraints, the endpoint defaults to the controller's native architecture and adds a `kubernetes.io/arch` nodeSelector automatically.

The source-images-service validates that `request.arch` matches `platform.machine()` at the start of each job, preventing architecture mismatches from producing incorrectly-tagged images. The `/api/v1/health` endpoint reports the service's native architecture.

## Log Level Propagation

All rickshaw scripts accept `--log-level` with a standard vocabulary: `normal`, `verbose`, `debug`, `verbose-debug`. When `crucible run --log-level <level>` is invoked with a non-default level, `rickshaw-run.py` overrides the `endpoints.log-level` and `roadblock.log-level` values from `rickshaw-settings.json` and updates the settings dict before saving it, so engine scripts also pick up the override via the saved settings file. The `verbose-debug` level enables roadblock's ultra-verbose mode end-to-end (controller, endpoints, and engine-side roadblock invocations).

## CI

GitHub Actions workflows in `.github/workflows/`:
- `crucible-ci.yaml` / `faux-crucible-ci.yaml` — Integration tests
- `unittest.yaml` / `faux-unittest.yaml` — Unit tests
- `run-crucible-tracking.yaml` — Tracking runs

CI jobs are generated by `util/generate-ci-jobs.py`.
