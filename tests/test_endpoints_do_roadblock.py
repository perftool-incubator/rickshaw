#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for endpoints.py's do_roadblock() thin wrapper around
toolbox.roadblock.do_roadblock() (PERFNFV-462).

toolbox, roadblock, and the fabric/invoke/paramiko third-party deps are all
mocked out rather than required, since endpoints.py imports from them at
module scope and CI does not check any of them out or install them for this
test job.
"""

import importlib.machinery
import importlib.util
import os
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock


class FakeRunResult:
    def __init__(self, exited=0, stdout="", stderr=""):
        self.exited = exited
        self.stdout = stdout
        self.stderr = stderr


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

    mock_toolbox_json = types.ModuleType("toolbox.json")

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

    mod_name = "endpoints_under_test"
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

    # endpoints.py's module-level TOOLBOX_HOME/ROADBLOCK_HOME guards check
    # that these paths exist on disk before appending them to sys.path --
    # the actual imports are satisfied by the sys.modules mocks above, so
    # these just need to pass the existence checks.
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

    mod.run_local = MagicMock(return_value=FakeRunResult())
    return mod


class TestDoRoadblock(unittest.TestCase):
    def setUp(self):
        self.mod = import_endpoints()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.msgs_dir = self.tmpdir.name

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_missing_label_raises_value_error(self):
        with self.assertRaises(ValueError):
            self.mod.do_roadblock(roadblock_id="run-1", msgs_dir=self.msgs_dir)
        self.mod._tb_do_roadblock.assert_not_called()

    def test_delegates_to_toolbox_with_expected_args(self):
        rc = self.mod.do_roadblock(
            roadblock_id="run-1",
            label="endpoint-deploy-begin",
            timeout=120,
            messages="/tmp/msgs.json",
            wait_for="some-cmd",
            abort=False,
            follower_id="client-1",
            redis_password="secret",
            msgs_dir=self.msgs_dir,
            connection_watchdog=True,
        )
        self.assertEqual(rc, 0)
        self.mod._tb_do_roadblock.assert_called_once_with(
            roadblock_id="run-1",
            label="endpoint-deploy-begin",
            role="follower",
            follower_id="client-1",
            leader_id="controller",
            timeout=120,
            redis_server="localhost",
            redis_password="secret",
            messages="/tmp/msgs.json",
            abort=False,
            connection_watchdog=True,
            msgs_dir=self.msgs_dir,
            wait_for="some-cmd",
        )

    def test_none_timeout_defaults_to_300(self):
        self.mod.do_roadblock(
            roadblock_id="run-1",
            label="endpoint-deploy-begin",
            follower_id="client-1",
            msgs_dir=self.msgs_dir,
        )
        _, kwargs = self.mod._tb_do_roadblock.call_args
        self.assertEqual(kwargs["timeout"], 300)

    def test_returns_only_rc_not_messages_data(self):
        self.mod._tb_do_roadblock.return_value = (4, {"some": "messages"})
        rc = self.mod.do_roadblock(
            roadblock_id="run-1",
            label="client-start-begin",
            follower_id="client-1",
            msgs_dir=self.msgs_dir,
        )
        self.assertEqual(rc, 4)

    def test_pings_redis_before_delegating(self):
        self.mod.do_roadblock(
            roadblock_id="run-1",
            label="client-start-begin",
            follower_id="client-1",
            msgs_dir=self.msgs_dir,
        )
        self.mod.run_local.assert_called_once_with("ping -w 10 -c 4 localhost")

    def test_no_crash_when_msgs_log_file_missing(self):
        # toolbox.roadblock.do_roadblock's mock doesn't actually write the
        # message log file to disk, so this exercises the path where the
        # wrapper's post-call stream-logging must not assume it exists.
        rc = self.mod.do_roadblock(
            roadblock_id="run-1",
            label="client-start-end",
            follower_id="client-1",
            msgs_dir=self.msgs_dir,
        )
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
