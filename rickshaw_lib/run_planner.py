# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Side-effect-free inspection of a Crucible run document.

This module deliberately stops at static run planning. It does not invoke an
endpoint, execute a benchmark helper, create a run directory, or reserve any
runtime resource. Multiplex is injected by the caller so this module remains
usable by Rickshaw without embedding a second parameter-expansion engine.
"""

import copy
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from jsonschema import ValidationError, validate

PLANNER_CONTRACT_VERSION = "1"
_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_ID_RANGE = re.compile(r"^(\d+)-(\d+)\Z")
_ID_VALUE = re.compile(r"^\d+\Z")


class PlanningError(ValueError):
    """A structured planner configuration or dependency failure."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class PlannerLimits:
    """Bounds applied before a plan is returned to a caller."""

    max_parameter_sets: int = 1000
    max_engine_ids: int = 1000
    max_tool_entries: int = 1000

    def validate(self):
        for name, value in (
            ("max_parameter_sets", self.max_parameter_sets),
            ("max_engine_ids", self.max_engine_ids),
            ("max_tool_entries", self.max_tool_entries),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise PlanningError("invalid_limit", f"{name} must be a positive integer")


class RunPlanner:
    """Build a deterministic, bounded plan from a validated run document."""

    def __init__(
        self,
        rickshaw_dir,
        multiplex_module,
        benchmarks_root=None,
        benchmark_resolver=None,
    ):
        self.rickshaw_dir = Path(rickshaw_dir).resolve()
        self.multiplex = multiplex_module
        self.benchmarks_root = (
            Path(benchmarks_root)
            if benchmarks_root is not None
            else None
        )
        self.benchmark_resolver = benchmark_resolver

    def plan(self, document, limits=None):
        limits = limits or PlannerLimits()
        limits.validate()
        digest = self._input_digest(document)
        base = {
            "contract_version": PLANNER_CONTRACT_VERSION,
            "input_digest": digest,
        }

        if not isinstance(document, dict):
            return self._invalid_plan(base, "invalid_input", "run document must be an object")

        schema_path = self.rickshaw_dir / "schema" / "run-file.json"
        try:
            with schema_path.open(encoding="utf-8") as stream:
                schema = json.load(stream)
            validate(instance=document, schema=schema)
        except (OSError, json.JSONDecodeError) as exc:
            return self._invalid_plan(
                base, "planner_unavailable", f"could not load run-file schema: {exc}"
            )
        except ValidationError as exc:
            return self._invalid_plan(base, "invalid_input", exc.message)

        try:
            benchmarks = self._plan_benchmarks(document, limits)
        except PlanningError as exc:
            return self._invalid_plan(base, exc.code, exc.message)

        sample_count = document.get("run-params", {}).get("num-samples", 1)
        global_iteration_count = max(
            (entry["iteration_count"] for entry in benchmarks),
            default=0,
        )
        truncated = any(
            entry["parameter_sets"]["truncated"] or entry["engine_ids"]["truncated"]
            for entry in benchmarks
        )
        endpoint_types = sorted({
            endpoint["type"]
            for endpoint in document["endpoints"]
            if isinstance(endpoint.get("type"), str)
        })
        topology_warnings = []
        if len(endpoint_types) != len(document["endpoints"]):
            topology_warnings.append("one or more endpoint types are unavailable")
        declared_engine_count = sum(entry["engine_ids"]["count"] for entry in benchmarks)

        tool_block = document.get("tool-params")
        if tool_block is None:
            default_tools, tool_warnings = self._load_default_tools()
            tools = {
                "mode": "default",
                "entries": default_tools[:limits.max_tool_entries],
                "truncated": len(default_tools) > limits.max_tool_entries,
                "warnings": tool_warnings,
            }
        else:
            tools = {
                "mode": "explicit",
                "entries": copy.deepcopy(tool_block[:limits.max_tool_entries]),
                "truncated": len(tool_block) > limits.max_tool_entries,
                "warnings": [],
            }
            truncated = truncated or tools["truncated"]
        truncated = truncated or tools["truncated"]

        return {
            **base,
            "validation": {"valid": True, "errors": [], "warnings": []},
            "benchmarks": benchmarks,
            "totals": {
                "benchmark_count": len(benchmarks),
                "global_iteration_count": global_iteration_count,
                "sample_count": sample_count,
                "sample_execution_count": global_iteration_count * sample_count,
            },
            "tools": tools,
            "topology": {
                "endpoint_types": endpoint_types,
                "engine_counts": {"declared_benchmark_ids": declared_engine_count},
                "confidence": "derived" if not topology_warnings else "unknown",
                "warnings": topology_warnings,
            },
            "runtime": {
                "serial_seconds": None,
                "wall_clock_seconds": None,
                "confidence": "unavailable",
                "unknown_reasons": [
                    "benchmark runtime and endpoint capacity require execution-time data"
                ],
            },
            "limits": {
                "max_parameter_sets": limits.max_parameter_sets,
                "max_engine_ids": limits.max_engine_ids,
                "max_tool_entries": limits.max_tool_entries,
                "truncated": truncated,
                "warnings": [],
            },
        }

    def _load_default_tools(self):
        path = self.rickshaw_dir / "config" / "tool-params.json"
        try:
            with path.open(encoding="utf-8") as stream:
                tools = json.load(stream)
        except (OSError, json.JSONDecodeError):
            return [], ["default tool configuration is unavailable"]
        if not isinstance(tools, list):
            return [], ["default tool configuration is invalid"]
        return copy.deepcopy(tools), []

    def _plan_benchmarks(self, document, limits):
        planned = []
        for occurrence, benchmark in enumerate(document["benchmarks"]):
            name = benchmark["name"]
            ids = self._bounded_ids(benchmark["ids"], limits.max_engine_ids)
            requirements = self._load_requirements(name)
            mv_params = benchmark["mv-params"]
            # blockbreaker currently selects the first object when the legacy
            # array form is used; preserve that execution contract here.
            if isinstance(mv_params, list):
                if not mv_params:
                    raise PlanningError(
                        "invalid_input", f"benchmark {name} has empty mv-params"
                    )
                mv_params = mv_params[0]

            try:
                expansion = self.multiplex.expand_parameters(
                    copy.deepcopy(mv_params),
                    requirements_json=copy.deepcopy(requirements),
                    max_results=limits.max_parameter_sets,
                )
            except Exception as exc:
                code = getattr(exc, "code", "expansion_failed")
                message = getattr(exc, "message", str(exc))
                raise PlanningError(
                    code,
                    f"benchmark {name} parameter expansion failed: {message}",
                ) from exc

            parameter_sets = {
                "count": expansion["count"],
                "returned": expansion.get("returned", len(expansion["sets"])),
                "items": copy.deepcopy(expansion["sets"]),
                "truncated": expansion["truncated"],
            }
            planned.append({
                "name": name,
                "occurrence": occurrence,
                "engine_ids": ids,
                "parameter_sets": parameter_sets,
                "iteration_count": expansion["count"],
                "sample_count": document.get("run-params", {}).get("num-samples", 1),
                "sample_execution_count": (
                    expansion["count"]
                    * document.get("run-params", {}).get("num-samples", 1)
                ),
            })
        return planned

    def _load_requirements(self, benchmark_name):
        if _SAFE_COMPONENT.fullmatch(benchmark_name) is None:
            raise PlanningError("invalid_input", "benchmark name is not a safe component")

        if self.benchmark_resolver is not None:
            resolved = self.benchmark_resolver(benchmark_name)
            if resolved is None:
                raise PlanningError(
                    "not_found", f"benchmark is not installed: {benchmark_name}"
                )
            benchmark_dir = Path(resolved)
        elif self.benchmarks_root is not None:
            candidate = self.benchmarks_root / benchmark_name
            try:
                benchmark_dir = candidate.resolve(strict=True)
            except FileNotFoundError:
                raise PlanningError(
                    "not_found", f"benchmark is not installed: {benchmark_name}"
                ) from None
        else:
            return None

        if not benchmark_dir.is_dir():
            raise PlanningError("not_found", f"benchmark is not installed: {benchmark_name}")
        requirements_path = benchmark_dir / "multiplex.json"
        if not requirements_path.is_file():
            return None
        try:
            with requirements_path.open(encoding="utf-8") as stream:
                return json.load(stream)
        except (OSError, json.JSONDecodeError) as exc:
            raise PlanningError(
                "invalid_requirements",
                f"could not load benchmark requirements: {exc}",
            ) from exc

    @staticmethod
    def _bounded_ids(raw_ids, limit):
        intervals = []
        singles = set()
        raw_values = raw_ids if isinstance(raw_ids, list) else [raw_ids]
        for item in raw_values:
            for segment in str(item).split(","):
                for token in segment.split("+"):
                    range_match = _ID_RANGE.fullmatch(token)
                    if range_match:
                        start, end = (int(value) for value in range_match.groups())
                        if start <= end:
                            intervals.append((start, end))
                        continue
                    if _ID_VALUE.fullmatch(token):
                        singles.add(int(token))

        intervals.extend((value, value) for value in singles)
        intervals.sort()
        merged = []
        for start, end in intervals:
            if merged and start <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))

        count = sum(end - start + 1 for start, end in merged)
        items = []
        for start, end in merged:
            for value in range(start, end + 1):
                if len(items) >= limit:
                    break
                items.append(str(value))
            if len(items) >= limit:
                break
        return {
            "count": count,
            "items": items,
            "truncated": count > limit,
        }

    @staticmethod
    def _input_digest(document):
        try:
            encoded = json.dumps(
                {
                    "contract_version": PLANNER_CONTRACT_VERSION,
                    "document": document,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError):
            return None
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _invalid_plan(base, code, message):
        return {
            **base,
            "validation": {
                "valid": False,
                "errors": [{"code": code, "message": message}],
                "warnings": [],
            },
            "benchmarks": [],
            "totals": {
                "benchmark_count": 0,
                "global_iteration_count": 0,
                "sample_count": None,
                "sample_execution_count": 0,
            },
            "tools": {
                "mode": "unavailable",
                "entries": [],
                "truncated": False,
                "warnings": [],
            },
            "topology": {
                "endpoint_types": [],
                "engine_counts": {},
                "confidence": "unknown",
                "warnings": [],
            },
            "runtime": {
                "serial_seconds": None,
                "wall_clock_seconds": None,
                "confidence": "unavailable",
                "unknown_reasons": ["input validation failed"],
            },
            "limits": {"truncated": False, "warnings": []},
        }
