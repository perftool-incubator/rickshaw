#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-run.py tags parsing and _process_from_file().

Validates that:
- CLI argument --tags parses name:val pairs into state.run["tags"]
- CLI argument --tags with empty string ("") does not fail and leaves tags empty
- CLI argument --tags with trailing/leading/extra commas skips empty tokens
- _process_from_file() does not append --tags when blockbreaker outputs empty string
- _process_from_file() appends --tags when tags are present
- Invalid tag formatting triggers sys.exit(1)
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

    mod_name = "rickshaw_run_under_test_tags"
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
        mod.logger = logging.getLogger("test_tags")
    finally:
        for key in mocks:
            if saved[key] is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = saved[key]

    return mod


class TestTagsCliParsing(unittest.TestCase):
    """Tests for CLI --tags argument parsing in RunState.process_cmdline()."""

    def setUp(self):
        self.mod = import_rickshaw_run()
        self.state = self.mod.RunState()
        # Suppress logging output during test runs
        logging.getLogger().setLevel(logging.CRITICAL)

    def test_tags_single_pair(self):
        with patch.object(sys, "argv", ["rickshaw-run.py", "--tags", "env:prod"]):
            self.state.process_cmdline()
        self.assertEqual(self.state.run.get("tags"), [{"name": "env", "val": "prod"}])

    def test_tags_multiple_pairs(self):
        with patch.object(sys, "argv", ["rickshaw-run.py", "--tags", "env:prod,user:alice,arch:x86_64"]):
            self.state.process_cmdline()
        self.assertEqual(
            self.state.run.get("tags"),
            [
                {"name": "env", "val": "prod"},
                {"name": "user", "val": "alice"},
                {"name": "arch", "val": "x86_64"},
            ],
        )

    def test_tags_empty_string(self):
        with patch.object(sys, "argv", ["rickshaw-run.py", "--tags", ""]):
            self.state.process_cmdline()
        self.assertEqual(self.state.run.get("tags"), [])

    def test_tags_with_empty_tokens(self):
        with patch.object(sys, "argv", ["rickshaw-run.py", "--tags", ",env:prod,,arch:x86_64,"]):
            self.state.process_cmdline()
        self.assertEqual(
            self.state.run.get("tags"),
            [
                {"name": "env", "val": "prod"},
                {"name": "arch", "val": "x86_64"},
            ],
        )

    def test_tags_invalid_format_exits(self):
        with patch.object(sys, "argv", ["rickshaw-run.py", "--tags", "invalidtag"]):
            with self.assertRaises(SystemExit) as cm:
                self.state.process_cmdline()
            self.assertEqual(cm.exception.code, 1)


class TestProcessFromFileTags(unittest.TestCase):
    """Tests for tags handling in RunState._process_from_file()."""

    def setUp(self):
        self.mod = import_rickshaw_run()
        self.state = self.mod.RunState()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state.defaults["base-run-dir"] = self.temp_dir.name
        self.state.run["base-run-dir"] = self.temp_dir.name
        os.makedirs(os.path.join(self.temp_dir.name, "config"), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir.name, "subprojects", "benchmarks", "oslat"), exist_ok=True)
        self.orig_crucible_home = os.environ.get("CRUCIBLE_HOME")
        os.environ["CRUCIBLE_HOME"] = self.temp_dir.name
        logging.getLogger().setLevel(logging.CRITICAL)

    def tearDown(self):
        if self.orig_crucible_home is not None:
            os.environ["CRUCIBLE_HOME"] = self.orig_crucible_home
        else:
            os.environ.pop("CRUCIBLE_HOME", None)
        self.temp_dir.cleanup()

    def test_process_from_file_with_empty_tags_output(self):
        """When blockbreaker returns empty output for tags, --tags is not appended."""
        run_file_path = os.path.join(self.temp_dir.name, "run-file.json")
        with open(run_file_path, "w") as f:
            json.dump({"tags": {}, "run-params": {}}, f)

        self.state.run["run-file"] = run_file_path
        args = ["--from-file", run_file_path]

        def mock_run_cmd(cmd):
            if "--config benchmarks" in cmd:
                return ("cmd", "oslat", 0)
            elif "--config tool-params" in cmd:
                return ("cmd", "[]", 0)
            elif "--config tags" in cmd:
                return ("cmd", "", 0)
            elif "--config endpoints" in cmd:
                return ("cmd", "remotehosts,hosts=localhost", 0)
            return ("cmd", "", 0)

        with patch.object(self.mod, "run_cmd", side_effect=mock_run_cmd):
            self.state._process_from_file(args)

        self.assertNotIn("--tags", args)

    def test_process_from_file_with_tags_output(self):
        """When blockbreaker returns tags output, --tags is appended to args."""
        run_file_path = os.path.join(self.temp_dir.name, "run-file.json")
        with open(run_file_path, "w") as f:
            json.dump({"tags": {"env": "prod"}, "run-params": {}}, f)

        self.state.run["run-file"] = run_file_path
        args = ["--from-file", run_file_path]

        def mock_run_cmd(cmd):
            if "--config benchmarks" in cmd:
                return ("cmd", "oslat", 0)
            elif "--config tool-params" in cmd:
                return ("cmd", "[]", 0)
            elif "--config tags" in cmd:
                return ("cmd", "env:prod", 0)
            elif "--config endpoints" in cmd:
                return ("cmd", "remotehosts,hosts=localhost", 0)
            return ("cmd", "", 0)

        with patch.object(self.mod, "run_cmd", side_effect=mock_run_cmd):
            self.state._process_from_file(args)

        self.assertIn("--tags", args)
        tags_idx = args.index("--tags")
        self.assertEqual(args[tags_idx + 1], "env:prod")


class TestRsTagsEnvParsing(unittest.TestCase):
    """Tests for RS_TAGS environment variable handling in RunState.process_environ()."""

    def setUp(self):
        self.mod = import_rickshaw_run()
        self.state = self.mod.RunState()
        logging.getLogger().setLevel(logging.CRITICAL)

    def test_rs_tags_empty_string(self):
        with patch.dict(os.environ, {"RS_TAGS": ""}):
            self.state.process_environ()
        self.assertEqual(self.state.run.get("tags"), [])

    def test_rs_tags_valid_string(self):
        with patch.dict(os.environ, {"RS_TAGS": "env:ci,team:perf"}):
            self.state.process_environ()
        self.assertEqual(
            self.state.run.get("tags"),
            [{"name": "env", "val": "ci"}, {"name": "team", "val": "perf"}],
        )

    def test_rs_tags_with_empty_tokens(self):
        with patch.dict(os.environ, {"RS_TAGS": ",env:ci,,team:perf,"}):
            self.state.process_environ()
        self.assertEqual(
            self.state.run.get("tags"),
            [{"name": "env", "val": "ci"}, {"name": "team", "val": "perf"}],
        )

    def test_rs_tags_invalid_format_exits(self):
        with patch.dict(os.environ, {"RS_TAGS": "invalidtag"}):
            with self.assertRaises(SystemExit) as cm:
                self.state.process_environ()
            self.assertEqual(cm.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
