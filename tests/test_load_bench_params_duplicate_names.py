#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-run.py's expand_id_ranges() and
RunState.load_bench_params()'s automatic per-instance param id-scoping.

Fixes a bug where two "benchmarks[]" array entries sharing the same
benchmark name (e.g. two concurrent "oslat" engine pairs with
different params, ids "1" and "2") had their params flattened into one
shared per-iteration params list with no way to tell which engine-id
group a given param belonged to -- so one instance's params silently
applied to (or overwrote, depending on getopt parsing order in the
benchmark script) every other instance sharing that name. Confirmed live
via a real crucible run before this fix existed.

load_bench_params() now auto-tags each instance's own params with an
"id" scoped to that instance's own declared "ids" (expanded via
expand_id_ranges(), "+"-joined for multi-id instances) -- but only when
the benchmark name is actually ambiguous (appears more than once), and
only for params that don't already carry an explicit "id" of their own,
so a single-instance benchmark (the overwhelmingly common case) and any
deliberate per-param "id" override are both unaffected.

toolbox is mocked out rather than required, since rickshaw-run.py imports
from it at module scope and CI does not check toolbox out for this job.
load_json_file/save_json_file are backed by real temp files (matching
test_apply_tool_multiplex.py's convention) so load_bench_params()'s own
file-existence/glob logic runs unmocked against real paths.
"""

import importlib.machinery
import importlib.util
import json
import logging
import os
import sys
import tempfile
import types
import unittest


def import_rickshaw_run():
    """Load rickshaw-run.py as a module with toolbox mocked out."""
    mock_fileio = types.ModuleType("toolbox.fileio")
    mock_fileio.open_write_text_file = lambda *a, **k: None

    mock_json = types.ModuleType("toolbox.json")

    def fake_load_json_file(json_file, uselzma=False):
        try:
            with open(json_file, "r") as f:
                return json.load(f), None
        except Exception as e:
            return None, str(e)

    mock_json.load_json_file = fake_load_json_file
    mock_json.save_json_file = lambda *a, **k: None
    mock_json.validate_schema = lambda *a, **k: (True, None)

    mock_jsonsettings = types.ModuleType("toolbox.jsonsettings")
    mock_jsonsettings.get_json_setting = lambda *a, **k: None

    mock_logging_mod = types.ModuleType("toolbox.logging")
    mock_logging_mod.setup_logging = lambda *a, **k: None

    mock_roadblock = types.ModuleType("toolbox.roadblock")
    mock_roadblock.do_roadblock = lambda *a, **k: (0, None)
    mock_roadblock.ROADBLOCK_EXITS = {
        "success": 0, "input": 2, "timeout": 3,
        "abort": 4, "heartbeat_timeout": 5, "abort_waiting": 6,
    }

    mock_run = types.ModuleType("toolbox.run")
    mock_run.run_cmd = lambda *a, **k: ("cmd", "", 0)

    mock_toolbox = types.ModuleType("toolbox")
    mock_toolbox.fileio = mock_fileio
    mock_toolbox.json = mock_json
    mock_toolbox.jsonsettings = mock_jsonsettings
    mock_toolbox.logging = mock_logging_mod
    mock_toolbox.roadblock = mock_roadblock
    mock_toolbox.run = mock_run

    mod_name = "rickshaw_run_under_test_dup_bench_names"
    sys.modules.pop(mod_name, None)

    mocks = {
        "toolbox": mock_toolbox,
        "toolbox.fileio": mock_fileio,
        "toolbox.json": mock_json,
        "toolbox.jsonsettings": mock_jsonsettings,
        "toolbox.logging": mock_logging_mod,
        "toolbox.roadblock": mock_roadblock,
        "toolbox.run": mock_run,
    }
    saved = {key: sys.modules.get(key) for key in mocks}
    sys.modules.update(mocks)

    try:
        script_path = os.path.join(os.path.dirname(__file__), "..", "rickshaw-run.py")
        loader = importlib.machinery.SourceFileLoader(mod_name, script_path)
        spec = importlib.util.spec_from_loader(mod_name, loader)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)
    finally:
        for key, val in saved.items():
            if val is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = val

    mod.logger = logging.getLogger("test_load_bench_params_duplicate_names")
    return mod


class TestExpandIdRanges(unittest.TestCase):
    def setUp(self):
        self.mod = import_rickshaw_run()

    def test_single_id(self):
        self.assertEqual(self.mod.expand_id_ranges("1"), ["1"])

    def test_dash_range(self):
        self.assertEqual(self.mod.expand_id_ranges("1-3"), ["1", "2", "3"])

    def test_plus_joined_list(self):
        self.assertEqual(self.mod.expand_id_ranges("1+3"), ["1", "3"])

    def test_mixed_ranges_and_singles(self):
        self.assertEqual(self.mod.expand_id_ranges("1-2+5-7+9"), ["1", "2", "5", "6", "7", "9"])

    def test_result_is_sorted_numerically_not_lexically(self):
        self.assertEqual(self.mod.expand_id_ranges("10+2"), ["2", "10"])

    def test_unrecognized_segment_is_skipped_with_a_warning(self):
        with self.assertLogs(self.mod.logger, level="WARNING"):
            result = self.mod.expand_id_ranges("1+not-an-id")
        self.assertEqual(result, ["1"])


class LoadBenchParamsTestBase(unittest.TestCase):
    """Shared fixture-writing helpers for load_bench_params() tests."""

    def setUp(self):
        self.mod = import_rickshaw_run()
        self.state = self.mod.RunState()
        self.bench_dir = tempfile.mkdtemp()
        with open(os.path.join(self.bench_dir, "rickshaw.json"), "w") as f:
            json.dump({"benchmark": "oslat"}, f)

    def _write_params_file(self, iterations):
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(iterations, f)
        return path


class TestLoadBenchParamsSingleInstance(LoadBenchParamsTestBase):
    """The common case -- one benchmark name, one instance -- must be
    completely unaffected by the auto-scoping added for the ambiguous
    (duplicate-name) case."""

    def test_no_id_is_auto_injected_for_a_single_instance(self):
        params_file = self._write_params_file(
            [[{"arg": "duration", "val": "10", "role": "all"}]]
        )
        self.state.run["bench-dir"] = self.bench_dir
        self.state.run["bench-params"] = params_file
        self.state.run["bench-ids"] = "oslat:1"

        self.state.load_bench_params()

        params = self.state.run["iterations"][0]["params"]
        self.assertEqual(len(params), 1)
        self.assertNotIn("id", params[0])


class TestLoadBenchParamsDuplicateNames(LoadBenchParamsTestBase):
    """Two "benchmarks[]" entries sharing the name "oslat" (ids "1"
    and "2") -- the actual rickshaw#872-class bug, reproduced end to end
    at the load_bench_params()/dump_params() boundary without spinning up
    a real crucible run."""

    def setUp(self):
        super().setUp()
        instance_1 = self._write_params_file([[
            {"arg": "duration", "val": "10", "role": "all"},
            {"arg": "direction", "val": "east", "role": "all"},
        ]])
        instance_2 = self._write_params_file([[
            {"arg": "duration", "val": "10", "role": "all"},
            {"arg": "direction", "val": "west", "role": "all"},
        ]])
        self.state.run["bench-dir"] = f"{self.bench_dir},{self.bench_dir}"
        self.state.run["bench-params"] = f"{instance_1},{instance_2}"
        self.state.run["bench-ids"] = "oslat:1,oslat:2"

        self.state.load_bench_params()
        self.params = self.state.run["iterations"][0]["params"]

    def test_every_param_from_an_ambiguous_instance_is_auto_scoped(self):
        self.assertEqual(len(self.params), 4)
        for param in self.params:
            self.assertIn("id", param)
            self.assertIn(param["id"], ("1", "2"))

    def test_instance_1_params_are_scoped_to_id_1(self):
        directions = {p["id"]: p["val"] for p in self.params if p["arg"] == "direction"}
        self.assertEqual(directions["1"], "east")
        self.assertEqual(directions["2"], "west")

    def test_dump_params_resolves_each_engine_to_only_its_own_instance(self):
        # this is the actual end-user-visible bug: before the fix, engine
        # id 2's rendered command included instance 1's params too (or
        # exclusively, depending on getopt ordering), not just its own.
        cmd_for_id_1 = self.mod.dump_params(self.params, "1", "client", {})
        cmd_for_id_2 = self.mod.dump_params(self.params, "2", "client", {})

        self.assertEqual(cmd_for_id_1, "--duration=10 --direction=east")
        self.assertEqual(cmd_for_id_2, "--duration=10 --direction=west")


class TestLoadBenchParamsExplicitIdOverride(LoadBenchParamsTestBase):
    """A param that already sets its own "id" must keep it -- auto-scoping
    only ever fills in a missing "id", never replaces an explicit one."""

    def setUp(self):
        super().setUp()
        # Instance "1-2" driving both engines with shared params, except
        # one param that deliberately overrides itself to id "1" only.
        instance_1 = self._write_params_file([[
            {"arg": "duration", "val": "10", "role": "all"},
        ]])
        instance_2 = self._write_params_file([[
            {"arg": "duration", "val": "10", "role": "all"},
            {"arg": "extra-only-for-one", "val": "yes", "role": "all", "id": "1"},
        ]])
        self.state.run["bench-dir"] = f"{self.bench_dir},{self.bench_dir}"
        self.state.run["bench-params"] = f"{instance_1},{instance_2}"
        self.state.run["bench-ids"] = "oslat:1,oslat:2"

        self.state.load_bench_params()
        self.params = self.state.run["iterations"][0]["params"]

    def test_explicit_id_is_preserved_not_overwritten_by_auto_scoping(self):
        extra = next(p for p in self.params if p["arg"] == "extra-only-for-one")
        self.assertEqual(extra["id"], "1")

    def test_explicit_id_still_only_reaches_its_own_engine(self):
        cmd_for_id_1 = self.mod.dump_params(self.params, "1", "client", {})
        cmd_for_id_2 = self.mod.dump_params(self.params, "2", "client", {})
        self.assertIn("--extra-only-for-one=yes", cmd_for_id_1)
        self.assertNotIn("--extra-only-for-one", cmd_for_id_2)


if __name__ == "__main__":
    unittest.main()
