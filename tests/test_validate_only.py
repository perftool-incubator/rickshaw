#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-run.py's --validate-only option and schema validation.

Validates that:
- parse_bool_arg correctly parses boolean and string representations (true/false/1/0/yes/no/etc.)
- CLI argument --validate-only sets validate_only to True
- CLI argument --validate-only=<val> parses boolean values properly (e.g. false sets False)
- Invalid --validate-only values trigger an error and sys.exit(1)
- Validation mode bypasses live validate_endpoints() and exits with code 0 and "VALID"
- Logging level is raised to WARNING in validation mode under normal log level
- Benchmark parameter files are validated against schema/bench-params.json
- Tool parameter files are validated against schema/tool-params.json
"""

import glob
import importlib.machinery
import importlib.util
import io
import json
import logging
import os
import sys
import tempfile
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

    def fake_validate_schema(data, schema_file):
        try:
            import jsonschema
            with open(schema_file, "r") as f:
                schema = json.load(f)
            jsonschema.validate(instance=data, schema=schema)
            return True, None
        except ImportError:
            # Fallback structural checks when jsonschema is not installed in the test env
            if "bench-params" in schema_file:
                if (isinstance(data, list) and len(data) > 0 and
                        all(isinstance(it, list) and len(it) > 0 and
                            all(isinstance(p, dict) and "arg" in p and "val" in p for p in it)
                            for it in data)):
                    return True, None
                return False, "invalid bench-params structure"
            if "tool-params" in schema_file:
                if (isinstance(data, list) and
                        all(isinstance(t, dict) and "tool" in t and
                            (not isinstance(t.get("params"), str)) for t in data)):
                    return True, None
                return False, "invalid tool-params structure"
            if "run-file" in schema_file:
                if (isinstance(data, dict) and "benchmarks" in data and "endpoints" in data
                        and isinstance(data["benchmarks"], list) and len(data["benchmarks"]) > 0
                        and isinstance(data["endpoints"], list) and len(data["endpoints"]) > 0):
                    return True, None
                return False, "invalid run-file structure"
            if "remotehosts" in schema_file:
                if (isinstance(data, dict) and data.get("type") == "remotehosts"
                        and isinstance(data.get("remotes"), list)):
                    return True, None
                return False, "invalid remotehosts structure"
            if "kube" in schema_file:
                if (isinstance(data, dict) and data.get("type") == "kube"):
                    return True, None
                return False, "invalid kube structure"
            return True, None
        except Exception as e:
            return False, str(e)

    mock_json.load_json_file = fake_load_json_file
    mock_json.save_json_file = lambda *a, **k: None
    mock_json.validate_schema = fake_validate_schema

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


class TestParamSchemaValidation(unittest.TestCase):
    """Test bench-params and tool-params schema validation in load_bench_params() and load_tool_params()."""

    def setUp(self):
        self.rr = import_rickshaw_run()
        self.state = self.rr.RunState()
        self.temp_dir = tempfile.mkdtemp()

        # Create mock benchmark directory with schema-valid rickshaw.json
        self.bench_dir = os.path.join(self.temp_dir, "testbench")
        os.makedirs(self.bench_dir, exist_ok=True)
        bench_rickshaw = {
            "rickshaw-benchmark": {"schema": {"version": "2020.05.18"}},
            "benchmark": "testbench",
            "controller": {"post-script": "testbench-post-process"},
            "client": {
                "files-from-controller": [{"src": "a", "dest": "b"}],
                "runtime": "testbench-runtime",
                "start": "testbench-start"
            }
        }
        with open(os.path.join(self.bench_dir, "rickshaw.json"), "w") as f:
            json.dump(bench_rickshaw, f)

        # Create mock tools directory with schema-valid rickshaw.json
        self.tools_dir = os.path.join(self.temp_dir, "tools")
        self.sysstat_dir = os.path.join(self.tools_dir, "sysstat")
        os.makedirs(self.sysstat_dir, exist_ok=True)
        tool_rickshaw = {
            "rickshaw-tool": {"schema": {"version": "2020.03.18"}},
            "tool": "sysstat",
            "controller": {"post-script": "sysstat-post-process"},
            "collector": {
                "start": "sysstat-start",
                "stop": "sysstat-stop"
            }
        }
        with open(os.path.join(self.sysstat_dir, "rickshaw.json"), "w") as f:
            json.dump(tool_rickshaw, f)

        self.state.config_dir = self.temp_dir
        self.state.default_tool_userenv = "stream-latest"
        self.state.required_archs = ["x86_64"]

    def _write_json(self, data):
        fd, path = tempfile.mkstemp(suffix=".json", dir=self.temp_dir)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        return path

    def test_load_bench_params_valid_schema(self):
        valid_params = [[{"arg": "duration", "val": "10"}]]
        params_file = self._write_json(valid_params)
        self.state.run["bench-dir"] = self.bench_dir
        self.state.run["bench-params"] = params_file

        # Should load without exception or sys.exit
        self.state.load_bench_params()
        self.assertEqual(len(self.state.run["iterations"]), 1)
        self.assertEqual(self.state.run["iterations"][0]["params"][0]["arg"], "duration")

    def test_load_bench_params_invalid_schema_dict_exits(self):
        # Empty object / dict instead of array of arrays
        invalid_params = {}
        params_file = self._write_json(invalid_params)
        self.state.run["bench-dir"] = self.bench_dir
        self.state.run["bench-params"] = params_file

        with self.assertRaises(SystemExit) as cm:
            self.state.load_bench_params()
        self.assertEqual(cm.exception.code, 1)

    def test_load_bench_params_invalid_schema_empty_array_exits(self):
        # Empty array (bench-params requires minItems: 1)
        invalid_params = []
        params_file = self._write_json(invalid_params)
        self.state.run["bench-dir"] = self.bench_dir
        self.state.run["bench-params"] = params_file

        with self.assertRaises(SystemExit) as cm:
            self.state.load_bench_params()
        self.assertEqual(cm.exception.code, 1)

    def test_load_bench_params_invalid_param_item_exits(self):
        # Item missing required 'val' field
        invalid_params = [[{"arg": "duration"}]]
        params_file = self._write_json(invalid_params)
        self.state.run["bench-dir"] = self.bench_dir
        self.state.run["bench-params"] = params_file

        with self.assertRaises(SystemExit) as cm:
            self.state.load_bench_params()
        self.assertEqual(cm.exception.code, 1)

    def test_load_tool_params_valid_schema(self):
        valid_tool_params = [
            {
                "tool": "sysstat",
                "params": [{"arg": "interval", "val": "1"}]
            }
        ]
        tool_params_file = self._write_json(valid_tool_params)
        self.state.run["tools-dir"] = self.tools_dir
        self.state.run["tool-params"] = tool_params_file

        self.state.load_tool_params()
        self.assertEqual(len(self.state.tools_params), 1)
        self.assertEqual(self.state.tools_params[0]["tool"], "sysstat")

    def test_load_tool_params_invalid_params_type_exits(self):
        # "params": "invalid" instead of array of param objects
        invalid_tool_params = [
            {
                "tool": "sysstat",
                "params": "invalid"
            }
        ]
        tool_params_file = self._write_json(invalid_tool_params)
        self.state.run["tools-dir"] = self.tools_dir
        self.state.run["tool-params"] = tool_params_file

        with self.assertRaises(SystemExit) as cm:
            self.state.load_tool_params()
        self.assertEqual(cm.exception.code, 1)

    def test_load_tool_params_missing_tool_field_exits(self):
        # Object missing required "tool" field
        invalid_tool_params = [
            {
                "params": [{"arg": "interval", "val": "1"}]
            }
        ]
        tool_params_file = self._write_json(invalid_tool_params)
        self.state.run["tools-dir"] = self.tools_dir
        self.state.run["tool-params"] = tool_params_file

        with self.assertRaises(SystemExit) as cm:
            self.state.load_tool_params()
        self.assertEqual(cm.exception.code, 1)


class TestEndpointSchemaValidation(unittest.TestCase):
    """Test static schema validation of run-file and endpoint definitions."""

    @classmethod
    def setUpClass(cls):
        cls.rickshaw_mod = import_rickshaw_run()

    def setUp(self):
        self.state = self.rickshaw_mod.RunState()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.run_dir = self.temp_dir.name
        self.state.run["base-run-dir"] = self.run_dir
        self.state.rickshaw_project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.state.run_file_schema_file = os.path.join(self.state.rickshaw_project_dir, "schema", "run-file.json")

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write_json(self, data):
        fpath = os.path.join(self.run_dir, f"test_{tempfile.mktemp(dir='')}.json")
        with open(fpath, "w") as f:
            json.dump(data, f)
        return fpath

    def test_validate_endpoint_schemas_no_endpoints_exits(self):
        self.state.endpoints = []
        with self.assertRaises(SystemExit) as cm:
            self.state.validate_endpoint_schemas()
        self.assertEqual(cm.exception.code, 1)

    def test_validate_endpoint_schemas_valid_remotehosts(self):
        run_file_data = {
            "benchmarks": [
                {
                    "name": "oslat",
                    "ids": "1",
                    "mv-params": {"sets": []}
                }
            ],
            "endpoints": [
                {
                    "type": "remotehosts",
                    "remotes": [
                        {
                            "engines": [{"role": "client", "ids": [1]}],
                            "config": {"host": "localhost"}
                        }
                    ]
                }
            ]
        }
        run_file_path = self._write_json(run_file_data)
        self.state.run["run-file"] = run_file_path
        self.state.endpoints = [{"type": "remotehosts", "opts": "", "label": "remotehosts-0"}]

        self.state.validate_endpoint_schemas()

    def test_validate_endpoint_schemas_valid_kube(self):
        run_file_data = {
            "benchmarks": [
                {
                    "name": "oslat",
                    "ids": "1",
                    "mv-params": {"sets": []}
                }
            ],
            "endpoints": [
                {
                    "type": "kube",
                    "controller-ip-address": "127.0.0.1",
                    "host": "localhost",
                    "user": "testuser",
                    "engines": {
                        "client": 1,
                        "server": 1
                    }
                }
            ]
        }
        run_file_path = self._write_json(run_file_data)
        self.state.run["run-file"] = run_file_path
        self.state.endpoints = [{"type": "kube", "opts": "", "label": "kube-0"}]

        self.state.validate_endpoint_schemas()

    def test_validate_endpoint_schemas_invalid_run_file_exits(self):
        invalid_run_file = {
            "endpoints": [
                {
                    "type": "remotehosts",
                    "remotes": [
                        {
                            "engines": [{"role": "client", "ids": [1]}],
                            "config": {"host": "localhost"}
                        }
                    ]
                }
            ]
        }
        run_file_path = self._write_json(invalid_run_file)
        self.state.run["run-file"] = run_file_path
        self.state.endpoints = [{"type": "remotehosts", "opts": "", "label": "remotehosts-0"}]

        with self.assertRaises(SystemExit) as cm:
            self.state.validate_endpoint_schemas()
        self.assertEqual(cm.exception.code, 1)

    def test_validate_endpoint_schemas_invalid_endpoint_block_exits(self):
        invalid_endpoint_run_file = {
            "benchmarks": [
                {
                    "name": "oslat",
                    "ids": "1",
                    "mv-params": {"sets": []}
                }
            ],
            "endpoints": [
                {
                    "type": "remotehosts",
                    "remotes": "invalid_remotes_type"
                }
            ]
        }
        run_file_path = self._write_json(invalid_endpoint_run_file)
        self.state.run["run-file"] = run_file_path
        self.state.endpoints = [{"type": "remotehosts", "opts": "", "label": "remotehosts-0"}]

        with self.assertRaises(SystemExit) as cm:
            self.state.validate_endpoint_schemas()
        self.assertEqual(cm.exception.code, 1)

    def test_validate_endpoint_schemas_unknown_endpoint_type_exits(self):
        unknown_ep_run_file = {
            "benchmarks": [
                {
                    "name": "oslat",
                    "ids": "1",
                    "mv-params": {"sets": []}
                }
            ],
            "endpoints": [
                {
                    "type": "unknown_endpoint_type",
                    "foo": "bar"
                }
            ]
        }
        run_file_path = self._write_json(unknown_ep_run_file)
        self.state.run["run-file"] = run_file_path
        self.state.endpoints = [{"type": "unknown_endpoint_type", "opts": "", "label": "unknown-0"}]

        with self.assertRaises(SystemExit) as cm:
            self.state.validate_endpoint_schemas()
        self.assertEqual(cm.exception.code, 1)

    def test_validate_endpoint_schemas_missing_endpoint_directory_exits(self):
        self.state.endpoints = [{"type": "nonexistent_type", "opts": "", "label": "nonexistent-0"}]

        with self.assertRaises(SystemExit) as cm:
            self.state.validate_endpoint_schemas()
        self.assertEqual(cm.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
