#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for endpoints.py's process_bench_roadblocks() data-structure
initialization (rickshaw#867's third consumer).

This is a separate, endpoint-side copy of "figure out how many iterations
and samples there are" from engine_lib.py's own process_bench_roadblocks()
-- it only needs the "test" id per entry, not the argv, but it independently
read the old flat-text bench-cmds file directly (bypassing engine_lib.py
entirely) and was missed during the initial argv/JSON conversion, causing a
live end-to-end run to crash with FileNotFoundError against the old
extensionless filename. This test locks in the fix: reading the same
'start.json.xz' file tool/bench commands now use everywhere else.

toolbox, roadblock, and the fabric/invoke/paramiko third-party deps are all
mocked out rather than required, since endpoints.py imports from them at
module scope and CI does not check any of them out or install them for this
test job.
"""

import importlib.machinery
import importlib.util
import json
import lzma
import os
import shutil
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock


def import_endpoints():
    """Load endpoints.py as a module with all of its external deps mocked out."""
    mock_fabric = types.ModuleType("fabric")
    mock_fabric.Connection = MagicMock

    mock_invoke = types.ModuleType("invoke")
    mock_invoke.run = MagicMock()

    mock_ssh_exception = types.ModuleType("paramiko.ssh_exception")
    mock_ssh_exception.AuthenticationException = Exception
    mock_ssh_exception.NoValidConnectionsError = Exception

    mock_paramiko = types.ModuleType("paramiko")
    mock_paramiko.ssh_exception = mock_ssh_exception

    def _fake_load_json_file(json_file, uselzma=False):
        try:
            opener = lzma.open if uselzma else open
            with opener(json_file, "rt") as fh:
                return json.load(fh), None
        except (OSError, ValueError) as exc:
            return None, str(exc)

    mock_toolbox_json = types.ModuleType("toolbox.json")
    mock_toolbox_json.load_json_file = _fake_load_json_file

    mock_toolbox_messages = types.ModuleType("toolbox.messages")
    mock_toolbox_messages.create_roadblock_msg = lambda *a, **k: None
    mock_toolbox_messages.prepare_user_msgs_file = lambda *a, **k: None
    mock_toolbox_messages.evaluate_roadblock_result = lambda *a, **k: None
    mock_toolbox_messages.save_received_messages = lambda *a, **k: None
    mock_toolbox_messages.ROADBLOCK_EXITS = {
        "success": 0,
        "input": 2,
        "timeout": 3,
        "abort": 4,
        "heartbeat_timeout": 5,
        "abort_waiting": 6,
    }

    mock_toolbox_roadblock = types.ModuleType("toolbox.roadblock")
    mock_toolbox_roadblock.do_roadblock = MagicMock(return_value=(0, None))

    mock_toolbox = types.ModuleType("toolbox")
    mock_toolbox.json = mock_toolbox_json
    mock_toolbox.messages = mock_toolbox_messages
    mock_toolbox.roadblock = mock_toolbox_roadblock

    mock_roadblock_engine_mod = types.ModuleType("roadblock")
    mock_roadblock_engine_mod.VERBOSE_DEBUG_LEVEL = 5

    mod_name = "endpoints_under_test_process_bench_roadblocks"
    sys.modules.pop(mod_name, None)

    mocks = {
        "fabric": mock_fabric,
        "invoke": mock_invoke,
        "paramiko": mock_paramiko,
        "paramiko.ssh_exception": mock_ssh_exception,
        "toolbox": mock_toolbox,
        "toolbox.json": mock_toolbox_json,
        "toolbox.messages": mock_toolbox_messages,
        "toolbox.roadblock": mock_toolbox_roadblock,
        "roadblock": mock_roadblock_engine_mod,
    }
    saved = {key: sys.modules.get(key) for key in mocks}
    sys.modules.update(mocks)

    with tempfile.TemporaryDirectory() as tmp_home:
        toolbox_python_dir = os.path.join(tmp_home, "python")
        os.makedirs(toolbox_python_dir)
        roadblock_dir = os.path.join(tmp_home, "roadblock")
        os.makedirs(roadblock_dir)
        open(os.path.join(roadblock_dir, "roadblock.py"), "w").close()

        saved_env = {
            "TOOLBOX_HOME": os.environ.get("TOOLBOX_HOME"),
            "ROADBLOCK_HOME": os.environ.get("ROADBLOCK_HOME"),
        }
        os.environ["TOOLBOX_HOME"] = tmp_home
        os.environ["ROADBLOCK_HOME"] = roadblock_dir

        try:
            script_path = os.path.join(
                os.path.dirname(__file__), "..", "endpoints", "endpoints.py"
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
            for key, val in saved_env.items():
                if val is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = val

    return mod


class TestProcessBenchRoadblocksInit(unittest.TestCase):
    def setUp(self):
        self.mod = import_endpoints()
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir, ignore_errors=True)
        self.msgs_dir = self.tmpdir

    def _write_start_json(self, entries):
        client_dir = os.path.join(self.tmpdir, "client", "1")
        os.makedirs(client_dir, exist_ok=True)
        path = os.path.join(client_dir, "start.json.xz")
        with lzma.open(path, "wt") as fh:
            json.dump(entries, fh)

    def test_reads_new_json_format_and_stops_before_iteration_loop(self):
        self._write_start_json([
            {"test": "1-1", "argv": ["fio", "--iodepth=4"]},
            {"test": "2-1", "argv": ["fio", "--iodepth=8"]},
        ])

        # 0 for setup-bench-begin, then a non-zero rc for setup-bench-end so
        # the function returns immediately after the fix's own logic runs,
        # without needing to fake the rest of the (unrelated, unchanged)
        # iteration loop
        self.mod.do_roadblock = MagicMock(side_effect=[0, 1])

        with self.assertLogs(logger=self.mod.logger, level="INFO") as cm:
            rc = self.mod.process_bench_roadblocks(
                callbacks={},
                roadblock_id="run-1",
                endpoint_label="kube-1",
                roadblock_password="secret",
                max_sample_failures=1,
                roadblock_messages_dir=self.msgs_dir,
                roadblock_timeouts={"default": 60},
                engine_commands_dir=self.tmpdir,
                endpoint_dir=self.tmpdir,
                roadblock_connection_watchdog=True,
            )

        self.assertEqual(rc, 1)
        joined = "\n".join(cm.output)
        self.assertIn("iteration_sample=1-1 iteration_id=1 sample_id=1", joined)
        self.assertIn("iteration_sample=2-1 iteration_id=2 sample_id=1", joined)
        self.assertIn("Total tests: 2", joined)

    def test_missing_file_logs_error_and_returns_nonzero(self):
        # no start.json.xz written at all
        self.mod.do_roadblock = MagicMock(return_value=0)

        with self.assertLogs(logger=self.mod.logger, level="ERROR"):
            rc = self.mod.process_bench_roadblocks(
                callbacks={},
                roadblock_id="run-1",
                endpoint_label="kube-1",
                roadblock_password="secret",
                max_sample_failures=1,
                roadblock_messages_dir=self.msgs_dir,
                roadblock_timeouts={"default": 60},
                engine_commands_dir=self.tmpdir,
                endpoint_dir=self.tmpdir,
                roadblock_connection_watchdog=True,
            )

        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
