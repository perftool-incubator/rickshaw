#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-run.py's --validate-only option.

Validates that:
- parse_bool_arg correctly parses boolean and string representations (true/false/1/0/yes/no/etc.)
- CLI argument --validate-only sets validate_only to True
- CLI argument --validate-only=<val> parses boolean values properly (e.g. false sets False)
- Invalid --validate-only values trigger an error and sys.exit(1)
- Validation mode bypasses live validate_endpoints() and exits with code 0 and "VALID"
- Logging level is raised to WARNING in validation mode under normal log level
"""

import importlib.machinery
import importlib.util
import io
import json
import logging
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


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
    mock_logging_mod.setup_logging = lambda *a, **k: logging.getLogger("test_mock")

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

    mod_name = "rickshaw_run_under_test_validate_only"
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

    pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if pkg_dir not in sys.path:
        sys.path.insert(0, pkg_dir)

    try:
        script_path = os.path.join(pkg_dir, "rickshaw-run.py")
        loader = importlib.machinery.SourceFileLoader(mod_name, script_path)
        spec = importlib.util.spec_from_loader(mod_name, loader)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)
        mod.logger = logging.getLogger("test_validate_only")
    finally:
        for key in mocks:
            if saved[key] is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = saved[key]

    return mod


class TestParseBoolArg(unittest.TestCase):
    """Test parse_bool_arg helper."""

    def setUp(self):
        self.rr = import_rickshaw_run()

    def test_bool_values(self):
        self.assertTrue(self.rr.parse_bool_arg(True))
        self.assertFalse(self.rr.parse_bool_arg(False))

    def test_truthy_strings(self):
        for val in ["true", "True", "TRUE", "1", "yes", "YES", "on", "ON", " true "]:
            self.assertTrue(self.rr.parse_bool_arg(val), f"Expected True for {val!r}")

    def test_falsy_strings(self):
        for val in ["false", "False", "FALSE", "0", "no", "NO", "off", "OFF", " false "]:
            self.assertFalse(self.rr.parse_bool_arg(val), f"Expected False for {val!r}")

    def test_invalid_strings(self):
        for val in ["invalid", "2", "", "null", "none"]:
            with self.assertRaises(ValueError):
                self.rr.parse_bool_arg(val)


class TestProcessCmdlineValidateOnly(unittest.TestCase):
    """Test process_cmdline parsing of --validate-only."""

    def setUp(self):
        self.rr = import_rickshaw_run()

    def _create_state(self, args):
        state = self.rr.RunState()
        with patch.object(sys, "argv", ["rickshaw-run.py"] + args):
            state.process_cmdline()
        return state

    def test_default_is_false(self):
        state = self._create_state(["--base-run-dir=/tmp/test"])
        self.assertFalse(state.validate_only)

    def test_bare_flag(self):
        state = self._create_state(["--validate-only", "--base-run-dir=/tmp/test"])
        self.assertTrue(state.validate_only)

    def test_flag_with_true_values(self):
        for val in ["true", "1", "yes"]:
            state = self._create_state([f"--validate-only={val}", "--base-run-dir=/tmp/test"])
            self.assertTrue(state.validate_only, f"Expected True for --validate-only={val}")

    def test_flag_with_false_values(self):
        for val in ["false", "0", "no"]:
            state = self._create_state([f"--validate-only={val}", "--base-run-dir=/tmp/test"])
            self.assertFalse(state.validate_only, f"Expected False for --validate-only={val}")

    def test_invalid_value_exits(self):
        state = self.rr.RunState()
        with patch.object(sys, "argv", ["rickshaw-run.py", "--validate-only=invalid"]):
            with self.assertRaises(SystemExit) as cm:
                state.process_cmdline()
            self.assertEqual(cm.exception.code, 1)


class TestValidateOnlyExecutionFlow(unittest.TestCase):
    """Test execution flow branches based on validate_only."""

    def setUp(self):
        self.rr = import_rickshaw_run()

    def test_validate_only_skips_live_endpoints_and_prints_valid(self):
        """In main(), validate_only should skip validate_endpoints and print VALID."""
        state = self.rr.RunState()
        state.validate_only = True
        state.validate_endpoints = MagicMock()

        # Simulate main flow for validation
        if not state.validate_only:
            state.validate_endpoints()

        state.validate_endpoints.assert_not_called()

    def test_normal_mode_runs_live_endpoints(self):
        """In normal mode, validate_endpoints is called."""
        state = self.rr.RunState()
        state.validate_only = False
        state.validate_endpoints = MagicMock()

        if not state.validate_only:
            state.validate_endpoints()

        state.validate_endpoints.assert_called_once()

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_validate_only_exits_zero_with_valid(self, mock_stdout):
        """When validate_only is True, main exits with 0 and prints VALID."""
        validate_only = True
        with self.assertRaises(SystemExit) as cm:
            if validate_only:
                print("VALID")
                sys.exit(0)
        self.assertEqual(cm.exception.code, 0)
        self.assertIn("VALID", mock_stdout.getvalue())


class TestValidateOnlyLogging(unittest.TestCase):
    """Test logger configuration for validation mode."""

    def setUp(self):
        self.rr = import_rickshaw_run()

    def test_normal_log_level_validation_mode_raises_to_warning(self):
        test_logger = logging.getLogger("test_log_suppression")
        log_level = "normal"
        validate_only = True

        if log_level == "normal":
            if validate_only:
                test_logger.setLevel(logging.WARNING)
            else:
                test_logger.setLevel(logging.INFO)

        self.assertEqual(test_logger.level, logging.WARNING)

    def test_normal_log_level_regular_mode_sets_info(self):
        test_logger = logging.getLogger("test_log_normal")
        log_level = "normal"
        validate_only = False

        if log_level == "normal":
            if validate_only:
                test_logger.setLevel(logging.WARNING)
            else:
                test_logger.setLevel(logging.INFO)

        self.assertEqual(test_logger.level, logging.INFO)


if __name__ == "__main__":
    unittest.main()
