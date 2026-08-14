#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for engine/engine_lib.py's argv-based command handling
(rickshaw#867's engine-side half): tool/bench commands are shipped as plain
JSON argv lists rather than pre-rendered shell strings, and shlex.join() is
applied exactly once, immediately before each run_command() call.

Also covers the wait_for double-quoting hazard found while designing this:
process_bench_roadblocks()'s unbounded-timeout path must hand roadblock a
literal Python list (which toolbox.roadblock passes straight to
subprocess.Popen with no re-parsing), not a pre-rendered string that would
need re-splitting.

fabric, invoke, and toolbox are mocked out rather than required, since
engine_lib.py imports from them at module scope and CI does not have real
engine dependencies available for this job.
"""

import importlib.machinery
import importlib.util
import json
import logging
import os
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock


def import_engine_lib():
    """Load engine/engine_lib.py as a module with its dependencies mocked out."""
    mock_fabric = types.ModuleType("fabric")
    mock_fabric.Connection = mock.MagicMock()

    mock_invoke = types.ModuleType("invoke")
    mock_invoke.run = mock.MagicMock()

    def _fake_load_json_file(json_file, uselzma=False):
        import lzma
        try:
            opener = lzma.open if uselzma else open
            with opener(json_file, "rt") as fh:
                return json.load(fh), None
        except (OSError, ValueError) as exc:
            return None, str(exc)

    mock_json = types.ModuleType("toolbox.json")
    mock_json.load_json_file = _fake_load_json_file

    mock_jsonsettings = types.ModuleType("toolbox.jsonsettings")
    mock_jsonsettings.get_json_setting = lambda *a, **k: (None, 0)

    mock_messages = types.ModuleType("toolbox.messages")
    mock_messages.ROADBLOCK_EXITS = {
        "success": 0, "input": 2, "timeout": 3,
        "abort": 4, "heartbeat_timeout": 5, "abort_waiting": 6,
    }
    mock_messages.evaluate_roadblock_result = lambda *a, **k: {
        "is_timeout": False, "is_abort": False, "messages": None,
    }
    mock_messages.prepare_user_msgs_file = lambda *a, **k: None
    mock_messages.resolve_svc_messages = lambda *a, **k: None
    mock_messages.save_received_messages = lambda *a, **k: None

    mock_roadblock = types.ModuleType("toolbox.roadblock")
    mock_roadblock.do_roadblock = lambda *a, **k: (0, None)

    mock_toolbox = types.ModuleType("toolbox")
    mock_toolbox.json = mock_json
    mock_toolbox.jsonsettings = mock_jsonsettings
    mock_toolbox.messages = mock_messages
    mock_toolbox.roadblock = mock_roadblock

    mod_name = "engine_lib_under_test"
    sys.modules.pop(mod_name, None)

    mocks = {
        "fabric": mock_fabric,
        "invoke": mock_invoke,
        "toolbox": mock_toolbox,
        "toolbox.json": mock_json,
        "toolbox.jsonsettings": mock_jsonsettings,
        "toolbox.messages": mock_messages,
        "toolbox.roadblock": mock_roadblock,
    }
    saved = {key: sys.modules.get(key) for key in mocks}
    sys.modules.update(mocks)

    try:
        script_path = os.path.join(
            os.path.dirname(__file__), "..", "engine", "engine_lib.py"
        )
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

    mod.logger = logging.getLogger("test_engine_lib")
    return mod


class TestParseToolCommands(unittest.TestCase):
    def setUp(self):
        self.mod = import_engine_lib()
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir, ignore_errors=True)

    def _write_json_xz(self, name, data):
        import lzma
        path = os.path.join(self.tmpdir, name)
        with lzma.open(path, "wt") as fh:
            json.dump(data, fh)
        return path

    def test_returns_name_argv_tuples(self):
        path = self._write_json_xz("start.json.xz", {
            "tools": [
                {"name": "kernel-1", "argv": ["kerneltools-start", "--interval=5"],
                 "deployment": "auto", "opt-tag": None},
            ]
        })
        engine = self.mod.Engine()
        result = engine._parse_tool_commands(path)
        self.assertEqual(
            result, [("kernel-1", ["kerneltools-start", "--interval=5"])]
        )

    def test_missing_file_raises_engine_error(self):
        engine = self.mod.Engine()
        with self.assertRaises(self.mod.EngineError):
            engine._parse_tool_commands(os.path.join(self.tmpdir, "missing.json.xz"))


class TestStartTools(unittest.TestCase):
    def setUp(self):
        self.mod = import_engine_lib()
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir, ignore_errors=True)
        self.orig_cwd = os.getcwd()
        os.chdir(self.tmpdir)
        self.addCleanup(os.chdir, self.orig_cwd)

    def test_argv_is_shlex_joined_before_invocation(self):
        engine = self.mod.Engine()
        engine.disable_tools = False
        engine.tool_start_cmds = os.path.join(self.tmpdir, "start.json.xz")
        engine.tool_stop_cmds = os.path.join(self.tmpdir, "stop.json.xz")
        for f in (engine.tool_start_cmds, engine.tool_stop_cmds):
            open(f, "a").close()

        argv = ["kerneltools-start", "--record-opts=-e cycles -c 1000"]
        engine._parse_tool_commands = mock.MagicMock(
            return_value=[("kernel-1", argv)]
        )

        with mock.patch.object(self.mod, "run_command") as mock_run:
            engine.start_tools()

        mock_run.assert_called_once()
        (cmd_str,), _ = mock_run.call_args
        self.assertIn("'--record-opts=-e cycles -c 1000'", cmd_str)
        self.assertTrue(cmd_str.startswith("cd tool-data/kernel-1 && "))


class TestLoadBenchCmds(unittest.TestCase):
    def setUp(self):
        self.mod = import_engine_lib()
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir, ignore_errors=True)

    def test_reads_json_entries(self):
        import lzma
        path = os.path.join(self.tmpdir, "start.json.xz")
        entries = [{"test": "1-1", "argv": ["fio", "--iodepth=4"]}]
        with lzma.open(path, "wt") as fh:
            json.dump(entries, fh)

        engine = self.mod.Engine()
        result = engine._load_bench_cmds(path)
        self.assertEqual(result, entries)

    def test_missing_file_returns_empty_list(self):
        engine = self.mod.Engine()
        result = engine._load_bench_cmds(os.path.join(self.tmpdir, "missing.json.xz"))
        self.assertEqual(result, [])


class TestRunBenchCmd(unittest.TestCase):
    def setUp(self):
        self.mod = import_engine_lib()

    def test_shlex_joins_argv_before_running(self):
        engine = self.mod.Engine()
        engine.cs_type = "client"
        engine.abort = False
        engine.quit = False
        with mock.patch.object(self.mod, "run_command") as mock_run:
            mock_run.return_value.return_code = 0
            engine.run_bench_cmd("client", "client", ["fio", "--exec_prerun=echo starting test"])
        mock_run.assert_called_once_with("fio '--exec_prerun=echo starting test'")

    def test_empty_argv_is_a_no_op(self):
        engine = self.mod.Engine()
        engine.cs_type = "client"
        engine.abort = False
        engine.quit = False
        with mock.patch.object(self.mod, "run_command") as mock_run:
            rc = engine.run_bench_cmd("client", "client", [])
        mock_run.assert_not_called()
        self.assertEqual(rc, 0)

    def test_non_matching_type_is_a_no_op(self):
        engine = self.mod.Engine()
        engine.cs_type = "server"
        with mock.patch.object(self.mod, "run_command") as mock_run:
            rc = engine.run_bench_cmd("client", "client", ["fio"])
        mock_run.assert_not_called()
        self.assertEqual(rc, 0)


class TestCliRunBenchCmd(unittest.TestCase):
    def setUp(self):
        self.mod = import_engine_lib()

    def test_trailing_args_collected_as_argv_and_joined(self):
        with mock.patch.object(self.mod, "run_command") as mock_run:
            mock_run.return_value.return_code = 0
            with self.assertRaises(SystemExit) as ctx:
                self.mod.cli_run_bench_cmd(
                    "client", "client", "client", "False", "False", "0",
                    "fio", "--exec_prerun=echo starting test",
                )
        mock_run.assert_called_once_with("fio '--exec_prerun=echo starting test'")
        self.assertEqual(ctx.exception.code, 0)

    def test_no_trailing_args_is_a_no_op(self):
        with mock.patch.object(self.mod, "run_command") as mock_run:
            self.mod.cli_run_bench_cmd("client", "client", "client", "False", "False", "0")
        mock_run.assert_not_called()

    def test_dispatch_splat_matches_sys_argv_shape(self):
        # sys.argv[2:] arrives as separate literal elements (no shell, since
        # process_bench_roadblocks() now passes wait_for as a real list to
        # subprocess.Popen) -- confirm cli_run_bench_cmd(*args) still accepts
        # an arbitrary number of trailing argv tokens via the same splat the
        # dispatch table already uses
        args = ["client", "client", "client", "False", "False", "0",
                "fio", "--iodepth=4", "--rw=read"]
        with mock.patch.object(self.mod, "run_command") as mock_run:
            mock_run.return_value.return_code = 0
            with self.assertRaises(SystemExit):
                self.mod.cli_run_bench_cmd(*args)
        mock_run.assert_called_once_with("fio --iodepth=4 --rw=read")


class TestWaitForUsesArgvList(unittest.TestCase):
    """The core regression test for the wait_for double-quoting hazard:
    once start_argv can legitimately contain a shlex.quote()-protected,
    single-quote-containing token, wait_for must be built as a literal list
    handed straight to roadblock -- never re-rendered into a string that
    would need re-splitting."""

    def setUp(self):
        self.mod = import_engine_lib()
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir, ignore_errors=True)
        self.orig_cwd = os.getcwd()
        self.addCleanup(os.chdir, self.orig_cwd)

    def test_wait_for_is_a_list_containing_start_argv_verbatim(self):
        engine = self.mod.Engine()
        engine.cs_type = "client"
        engine.cs_id = "1"
        engine.cs_label = "client-1"
        engine.cs_buddy = "server-1"
        engine.cs_dir = self.tmpdir
        engine.roadblock_msgs_dir = os.path.join(self.tmpdir, "rb-msgs")
        os.makedirs(engine.roadblock_msgs_dir, exist_ok=True)
        engine.default_timeout = 300
        engine.max_sample_failures = 3
        engine.abort = False
        engine.quit = False
        engine.endpoint_label = ""
        engine._copy_files_to_sample_dir = lambda *_a, **_k: None

        # a value that needed shlex.quote()-protection -- exactly the shape
        # that broke the old single-quote-wrapped wait_for string
        start_argv = ["fio", "--exec_prerun=echo starting test"]

        bench_cmds = {
            "bench-start-cmds.json.xz": [{"test": "1-1", "argv": start_argv}],
            "bench-infra-cmds.json.xz": [{"test": "1-1", "argv": []}],
            "bench-runtime-cmds.json.xz": [{"test": "1-1", "argv": ["probe"]}],
            "bench-stop-cmds.json.xz": [{"test": "1-1", "argv": []}],
        }
        engine._load_bench_cmds = lambda filename: bench_cmds[filename]

        do_roadblock_calls = []

        def fake_do_roadblock(label, timeout, messages=None, wait_for=None, do_abort=False):
            do_roadblock_calls.append({"label": label, "wait_for": wait_for})
            return 0

        engine.do_roadblock = fake_do_roadblock

        fake_runtime_result = mock.MagicMock(return_code=0, stdout="-1")
        with mock.patch.object(self.mod, "run_command", return_value=fake_runtime_result):
            engine.process_bench_roadblocks()

        wait_for_calls = [c for c in do_roadblock_calls if c["wait_for"] is not None]
        self.assertEqual(len(wait_for_calls), 1)
        wait_for_cmd = wait_for_calls[0]["wait_for"]

        self.assertIsInstance(wait_for_cmd, list)
        self.assertNotIsInstance(wait_for_cmd, str)
        self.assertEqual(wait_for_cmd[:6], [
            "python3", "/usr/local/bin/engine_lib.py", "run_bench_cmd",
            "client", "client", "client",
        ])
        # start_argv's tokens appear verbatim, with their embedded quote
        # character intact -- never re-parsed through shlex.split()
        self.assertEqual(wait_for_cmd[-len(start_argv):], start_argv)


if __name__ == "__main__":
    unittest.main()
