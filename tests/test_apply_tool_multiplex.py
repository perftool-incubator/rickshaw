#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-run.py's RunState.apply_tool_multiplex() -- the
bridge that lets a tool's flat tool-params.json params get validated/
transformed/preset-applied by multiplex.py, identical to benchmarks, without
ever risking real parameter multiplication (crucible#653).

toolbox is mocked out rather than required, since rickshaw-run.py imports
from it at module scope and CI does not check toolbox out for this job.
multiplex.py itself is not invoked -- run_cmd is mocked so these tests
exercise only the wrap/unwrap logic in apply_tool_multiplex(), not
multiplex.py's own internals (which are that project's own responsibility).
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


def fake_multiplex(sets):
    """Build a run_cmd side_effect simulating multiplex.py: reads the --input
    file this code wrote, writes back `sets` (a list of param lists) to the
    --output path, and returns rc=0. Pass rc= to simulate a failure instead.
    """

    def _side_effect(cmd):
        match = re.search(r"--output (\S+)", cmd)
        output_file = match.group(1)
        with open(output_file, "w") as f:
            json.dump(sets, f)
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

    def test_wraps_and_unwraps_round_trip(self):
        self._write_multiplex_json()
        self.run_cmd.side_effect = fake_multiplex(
            [[{"arg": "interval", "val": "10", "role": "all"}]]
        )
        tool_entry = {"tool-id": "kernel", "params": [{"arg": "interval", "val": "10"}]}
        self.state.apply_tool_multiplex(tool_entry, self.tool_dir)

        # role must not leak into the unwrapped result
        self.assertEqual(tool_entry["params"], [{"arg": "interval", "val": "10"}])

        # verify the wrapped input actually sent to multiplex used a
        # one-element vals array and role "all", not multiplex's "client" default
        cmd = self.run_cmd.call_args[0][0]
        input_file = re.search(r"--input (\S+)", cmd).group(1)
        with open(input_file) as f:
            wrapped = json.load(f)
        self.assertEqual(
            wrapped["sets"][0]["params"],
            [{"arg": "interval", "vals": ["10"], "role": "all"}],
        )

    def test_disabled_param_excluded_from_wrap(self):
        self._write_multiplex_json()
        self.run_cmd.side_effect = fake_multiplex([[]])
        tool_entry = {
            "tool-id": "kernel",
            "params": [{"arg": "interval", "val": "10", "enabled": "no"}],
        }
        self.state.apply_tool_multiplex(tool_entry, self.tool_dir)

        cmd = self.run_cmd.call_args[0][0]
        input_file = re.search(r"--input (\S+)", cmd).group(1)
        with open(input_file) as f:
            wrapped = json.load(f)
        self.assertEqual(wrapped["sets"][0]["params"], [])

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

    def test_more_than_one_combination_is_rejected(self):
        self._write_multiplex_json()
        self.run_cmd.side_effect = fake_multiplex(
            [
                [{"arg": "interval", "val": "10", "role": "all"}],
                [{"arg": "interval", "val": "20", "role": "all"}],
            ]
        )
        tool_entry = {"tool-id": "kernel", "params": [{"arg": "interval", "val": "10"}]}
        with self.assertRaises(SystemExit):
            self.state.apply_tool_multiplex(tool_entry, self.tool_dir)


if __name__ == "__main__":
    unittest.main()
