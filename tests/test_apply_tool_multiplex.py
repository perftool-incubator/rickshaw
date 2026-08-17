#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-run.py's RunState.apply_tool_multiplex() -- the
bridge that lets a tool's flat tool-params.json params get validated/
transformed/preset-applied by multiplex.py's --flat mode, identical in
spirit to benchmarks but without ever needing sets/include/cartesian-
product (tools always have exactly one implicit param set and, per
schema/tool-params.json, never more than one value per param).

toolbox is mocked out rather than required, since rickshaw-run.py imports
from it at module scope and CI does not check toolbox out for this job.
multiplex.py itself is not invoked -- run_cmd is mocked so these tests
exercise only apply_tool_multiplex()'s own logic, not multiplex.py's --flat
mode internals (which are that project's own responsibility).
"""

import importlib.machinery
import importlib.util
import json
import logging
import os
import re
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock


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

    def fake_save_json_file(filename, data, schema_file=None):
        with open(filename, "w") as f:
            json.dump(data, f)

    mock_json.load_json_file = fake_load_json_file
    mock_json.save_json_file = fake_save_json_file
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
    mock_run.run_cmd = MagicMock()

    mock_toolbox = types.ModuleType("toolbox")
    mock_toolbox.fileio = mock_fileio
    mock_toolbox.json = mock_json
    mock_toolbox.jsonsettings = mock_jsonsettings
    mock_toolbox.logging = mock_logging_mod
    mock_toolbox.roadblock = mock_roadblock
    mock_toolbox.run = mock_run

    mod_name = "rickshaw_run_under_test_multiplex"
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

    mod.logger = logging.getLogger("test_apply_tool_multiplex")
    return mod


def fake_multiplex(params):
    """Build a run_cmd side_effect simulating multiplex.py --flat: reads the
    --input file this code wrote, writes back `params` (a flat list of
    {arg, val} dicts) to the --output path, and returns rc=0.
    """

    def _side_effect(cmd):
        match = re.search(r"--output (\S+)", cmd)
        output_file = match.group(1)
        with open(output_file, "w") as f:
            json.dump(params, f)
        return (cmd, "", 0)

    return _side_effect


class TestApplyToolMultiplex(unittest.TestCase):
    def setUp(self):
        self.mod = import_rickshaw_run()
        self.state = self.mod.RunState()
        self.state.config_dir = tempfile.mkdtemp()
        self.tool_dir = tempfile.mkdtemp()
        self.run_cmd = self.mod.run_cmd
        self.run_cmd.reset_mock()

    def _write_multiplex_json(self):
        with open(os.path.join(self.tool_dir, "multiplex.json"), "w") as f:
            json.dump({"validations": {}}, f)

    def test_no_multiplex_json_is_noop(self):
        tool_entry = {"tool-id": "kernel", "params": [{"arg": "interval", "val": "10"}]}
        self.state.apply_tool_multiplex(tool_entry, self.tool_dir)
        self.assertEqual(tool_entry["params"], [{"arg": "interval", "val": "10"}])
        self.run_cmd.assert_not_called()

    def test_flat_round_trip(self):
        self._write_multiplex_json()
        self.run_cmd.side_effect = fake_multiplex(
            [{"arg": "interval", "val": "10"}]
        )
        tool_entry = {"tool-id": "kernel", "params": [{"arg": "interval", "val": "10"}]}
        self.state.apply_tool_multiplex(tool_entry, self.tool_dir)

        self.assertEqual(tool_entry["params"], [{"arg": "interval", "val": "10"}])

        cmd = self.run_cmd.call_args[0][0]
        self.assertIn("--flat", cmd)

        # the input sent to multiplex is the tool's params, flat and
        # unmodified -- no wrap into a sets/global-options document, no
        # injected "vals"/"role" (--flat mode owns that internally now)
        input_file = re.search(r"--input (\S+)", cmd).group(1)
        with open(input_file) as f:
            flat_input = json.load(f)
        self.assertEqual(flat_input, [{"arg": "interval", "val": "10"}])

    def test_disabled_param_passed_through_unfiltered(self):
        # Filtering a disabled param is now multiplex --flat mode's own
        # responsibility (param_enabled() inside apply_flat_params()) --
        # rickshaw no longer pre-filters client-side, matching how the
        # benchmark path has always relied on multiplex for this.
        self._write_multiplex_json()
        self.run_cmd.side_effect = fake_multiplex([])
        tool_entry = {
            "tool-id": "kernel",
            "params": [{"arg": "interval", "val": "10", "enabled": "no"}],
        }
        self.state.apply_tool_multiplex(tool_entry, self.tool_dir)

        cmd = self.run_cmd.call_args[0][0]
        input_file = re.search(r"--input (\S+)", cmd).group(1)
        with open(input_file) as f:
            flat_input = json.load(f)
        self.assertEqual(
            flat_input,
            [{"arg": "interval", "val": "10", "enabled": "no"}],
        )

    def test_multiplex_failure_exits(self):
        self._write_multiplex_json()
        self.run_cmd.return_value = ("cmd", "some error", 1)
        tool_entry = {"tool-id": "kernel", "params": [{"arg": "interval", "val": "10"}]}
        with self.assertRaises(SystemExit):
            self.state.apply_tool_multiplex(tool_entry, self.tool_dir)

    def test_empty_set_fail_without_defaults_preset_exits(self):
        # Regression lock-in: EC_EMPTY_SET_FAIL (rc=6) is a real, intentional
        # backward-compatibility trap -- a tool entry with zero params (legal
        # today per schema/tool-params.json, meaning "use the *-start
        # script's own bash defaults") starts hard-failing here unless the
        # tool's multiplex.json defines a "defaults" preset. This test locks
        # in that rickshaw-run treats it as fatal (via the generic rc!=0
        # handling) rather than "fixing it away" silently in the future.
        self._write_multiplex_json()
        self.run_cmd.return_value = ("cmd", "empty param set", 6)
        tool_entry = {"tool-id": "kernel", "params": []}
        with self.assertRaises(SystemExit):
            self.state.apply_tool_multiplex(tool_entry, self.tool_dir)


if __name__ == "__main__":
    unittest.main()
