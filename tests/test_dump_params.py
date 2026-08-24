#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-run.py's dump_params()/render_param().

Covers rickshaw#867: a param value containing a space must be rendered with
enough quoting to survive both shell execution and the shlex.split() round
trip used to rebuild the argv list shipped to the engine, while every
existing (space-free) value renders byte-for-byte identically to before.

Also covers the "id"-scoping fix for multiple "benchmarks[]" array entries
sharing the same benchmark name (e.g. two concurrent "oslat" engine
pairs with different params): dump_params() now treats a param's "id" as
a "+"-joined set of engine ids it applies to (falling back to the
pre-existing single-value behavior when there's no "+"), so that
load_bench_params()'s automatic per-instance id-scoping (see
test_load_bench_params_duplicate_names.py) actually reaches the right
engine.

toolbox is mocked out rather than required, since rickshaw-run.py imports
from it at module scope and CI does not check toolbox out for this job.
"""

import importlib.machinery
import importlib.util
import logging
import shlex
import sys
import types
import unittest


def import_rickshaw_run():
    """Load rickshaw-run.py as a module with toolbox mocked out."""
    mock_fileio = types.ModuleType("toolbox.fileio")
    mock_fileio.open_write_text_file = lambda *a, **k: None

    mock_json = types.ModuleType("toolbox.json")
    mock_json.load_json_file = lambda *a, **k: (None, "not used in this test")
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

    mod_name = "rickshaw_run_under_test_dump_params"
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
        import os
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

    mod.logger = logging.getLogger("test_dump_params")
    return mod


class TestRenderParam(unittest.TestCase):
    def setUp(self):
        self.mod = import_rickshaw_run()

    def test_bare_flag_with_no_value(self):
        self.assertEqual(self.mod.render_param("some-flag", ""), "--some-flag")

    def test_simple_value_unquoted(self):
        # every currently-shipped value (ON/OFF, paths, profile names) must
        # render byte-for-byte identically to before this change
        self.assertEqual(self.mod.render_param("rw", "read"), "--rw=read")
        self.assertEqual(self.mod.render_param("iodepth", "4"), "--iodepth=4")

    def test_space_containing_value_is_quoted(self):
        token = self.mod.render_param("exec_prerun", "echo starting test")
        self.assertEqual(token, "--exec_prerun='echo starting test'")

    def test_quoted_value_round_trips_through_shlex_split(self):
        token = self.mod.render_param("exec_prerun", "echo starting test")
        self.assertEqual(shlex.split(token), ["--exec_prerun=echo starting test"])

    def test_none_renders_as_bare_flag(self):
        self.assertEqual(self.mod.render_param("some-flag", None), "--some-flag")

    def test_falsy_non_string_values_are_not_silently_dropped(self):
        # a code-review catch: val=0/False are "falsy" in Python but are
        # real, meaningful values a param author may explicitly want to
        # send -- only None/"" mean "no value"
        self.assertEqual(self.mod.render_param("interval", 0), "--interval=0")
        self.assertEqual(self.mod.render_param("enabled", False), "--enabled=False")


class TestDumpParams(unittest.TestCase):
    def setUp(self):
        self.mod = import_rickshaw_run()

    def test_simple_params_render_unchanged(self):
        params = [
            {"arg": "iodepth", "val": "4", "role": "client"},
            {"arg": "rw", "val": "read", "role": "client"},
        ]
        result = self.mod.dump_params(params, "1", "client", {})
        self.assertEqual(result, "--iodepth=4 --rw=read")

    def test_null_value_renders_as_bare_flag_not_the_string_none(self):
        # a code-review catch: a present-but-null JSON "val" must render as
        # a bare flag, matching the pre-existing (falsy) behavior -- not get
        # stringified into the literal text "None" before being checked
        params = [{"arg": "foo", "val": None, "role": "client"}]
        result = self.mod.dump_params(params, "1", "client", {})
        self.assertEqual(result, "--foo")

    def test_zero_value_is_not_silently_dropped(self):
        params = [{"arg": "foo", "val": 0, "role": "client"}]
        result = self.mod.dump_params(params, "1", "client", {})
        self.assertEqual(result, "--foo=0")

    def test_space_containing_value_survives_the_pipeline(self):
        # the concrete rickshaw#867 regression case: exec_prerun's value must
        # come back out exactly as it went in after render -> shlex.split
        params = [
            {"arg": "exec_prerun", "val": "echo starting test", "role": "client"},
        ]
        rendered = self.mod.dump_params(params, "1", "client", {})
        argv = shlex.split(rendered)
        self.assertEqual(argv, ["--exec_prerun=echo starting test"])

    def test_client_id_substitution_still_applies(self):
        params = [{"arg": "id", "val": "client-%client-id%", "role": "client"}]
        result = self.mod.dump_params(params, "3", "client", {})
        self.assertEqual(result, "--id=client-3")

    def test_role_filtering_excludes_non_matching_role(self):
        params = [
            {"arg": "client-only", "val": "1", "role": "client"},
            {"arg": "server-only", "val": "1", "role": "server"},
        ]
        result = self.mod.dump_params(params, "1", "client", {})
        self.assertEqual(result, "--client-only=1")

    def test_benchmark_filtering_excludes_non_matching_benchmark(self):
        params = [
            {"arg": "fio-only", "val": "1", "role": "client", "benchmark": "fio"},
            {"arg": "uperf-only", "val": "1", "role": "client", "benchmark": "uperf"},
        ]
        result = self.mod.dump_params(params, "1", "client", {"1": "fio"})
        self.assertEqual(result, "--fio-only=1")

    def test_single_id_matches_only_that_engine_id(self):
        params = [{"arg": "mode", "val": "foo", "role": "all", "id": "1"}]
        self.assertEqual(self.mod.dump_params(params, "1", "client", {}), "--mode=foo")
        self.assertEqual(self.mod.dump_params(params, "2", "client", {}), "")

    def test_plus_joined_id_set_matches_any_member(self):
        # load_bench_params() auto-scopes a benchmark instance's params to
        # a "+"-joined set of its own ids (e.g. "1+2" for ids="1-2") --
        # any of those engine ids must match, not just the first.
        params = [{"arg": "mode", "val": "foo", "role": "all", "id": "1+2"}]
        self.assertEqual(self.mod.dump_params(params, "1", "client", {}), "--mode=foo")
        self.assertEqual(self.mod.dump_params(params, "2", "client", {}), "--mode=foo")
        self.assertEqual(self.mod.dump_params(params, "3", "client", {}), "")

    def test_absent_id_still_applies_to_every_engine_id(self):
        # pre-existing behavior for the common (non-ambiguous) case must
        # be unchanged: a param with no "id" key at all is never
        # id-filtered, regardless of cs_id.
        params = [{"arg": "mode", "val": "foo", "role": "all"}]
        self.assertEqual(self.mod.dump_params(params, "1", "client", {}), "--mode=foo")
        self.assertEqual(self.mod.dump_params(params, "2", "client", {}), "--mode=foo")


if __name__ == "__main__":
    unittest.main()
