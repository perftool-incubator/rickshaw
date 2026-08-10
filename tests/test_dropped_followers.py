#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-run.py's dropped-follower tracking:
RunState.remove_followers()/remove_dropped_followers()/remove_engine_followers()
and RunState.evaluate_test_roadblock()'s timeout/heartbeat_timeout attribution
(PERFNFV-464).

toolbox is mocked out rather than required, since rickshaw-run.py imports
from it at module scope and CI does not check toolbox out for this job.
"""

import importlib.machinery
import importlib.util
import logging
import os
import sys
import types
import unittest

# Mirrors toolbox.roadblock.ROADBLOCK_EXITS -- kept in sync manually since
# toolbox isn't checked out for this test job.
ROADBLOCK_EXITS = {
    "success": 0,
    "input": 2,
    "timeout": 3,
    "abort": 4,
    "heartbeat_timeout": 5,
    "abort_waiting": 6,
}


def import_rickshaw_run():
    """Load rickshaw-run.py as a module with toolbox mocked out."""
    mock_fileio = types.ModuleType("toolbox.fileio")
    mock_fileio.open_write_text_file = lambda *a, **k: None

    mock_json = types.ModuleType("toolbox.json")
    mock_json.load_json_file = lambda *a, **k: (None, None)
    mock_json.save_json_file = lambda *a, **k: None
    mock_json.validate_schema = lambda *a, **k: (True, None)

    mock_jsonsettings = types.ModuleType("toolbox.jsonsettings")
    mock_jsonsettings.get_json_setting = lambda *a, **k: None

    mock_logging_mod = types.ModuleType("toolbox.logging")
    mock_logging_mod.setup_logging = lambda *a, **k: None

    mock_roadblock = types.ModuleType("toolbox.roadblock")
    mock_roadblock.do_roadblock = lambda *a, **k: (0, None)
    mock_roadblock.ROADBLOCK_EXITS = ROADBLOCK_EXITS

    mock_run = types.ModuleType("toolbox.run")
    mock_run.run_cmd = lambda *a, **k: None

    mock_toolbox = types.ModuleType("toolbox")
    mock_toolbox.fileio = mock_fileio
    mock_toolbox.json = mock_json
    mock_toolbox.jsonsettings = mock_jsonsettings
    mock_toolbox.logging = mock_logging_mod
    mock_toolbox.roadblock = mock_roadblock
    mock_toolbox.run = mock_run

    mod_name = "rickshaw_run_under_test"
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

    # main() normally sets this via setup_logging(); the methods under test
    # log through it directly, so it must be non-None before they're called.
    mod.logger = logging.getLogger("test_dropped_followers")

    return mod


class TestRemoveFollowers(unittest.TestCase):
    def setUp(self):
        self.mod = import_rickshaw_run()
        self.state = self.mod.RunState()
        self.state.active_followers = ["client-1", "client-2", "client-3"]

    def test_remove_dropped_followers_logs_and_removes(self):
        self.state.remove_dropped_followers(["client-2"], roadblock_label="infra-start-end")
        self.assertEqual(sorted(self.state.active_followers), ["client-1", "client-3"])
        self.assertEqual(
            self.state.dropped_followers_log,
            [{"follower": "client-2", "roadblock": "infra-start-end"}],
        )

    def test_remove_engine_followers_is_silent(self):
        self.state.remove_engine_followers(["client-2"])
        self.assertEqual(sorted(self.state.active_followers), ["client-1", "client-3"])
        self.assertEqual(self.state.dropped_followers_log, [])

    def test_remove_dropped_followers_ignores_already_removed(self):
        self.state.remove_dropped_followers(["client-2"], roadblock_label="infra-start-end")
        self.state.remove_dropped_followers(["client-2"], roadblock_label="server-start-end")
        self.assertEqual(
            self.state.dropped_followers_log,
            [{"follower": "client-2", "roadblock": "infra-start-end"}],
        )

    def test_remove_dropped_followers_empty_list_is_noop(self):
        self.state.remove_dropped_followers([], roadblock_label="infra-start-end")
        self.assertEqual(sorted(self.state.active_followers), ["client-1", "client-2", "client-3"])
        self.assertEqual(self.state.dropped_followers_log, [])


class TestEvaluateTestRoadblock(unittest.TestCase):
    def setUp(self):
        self.mod = import_rickshaw_run()
        self.ROADBLOCK_EXITS = self.mod.ROADBLOCK_EXITS
        self.state = self.mod.RunState()
        self.state.active_followers = ["client-1"]
        self.sample_info = {"attempt-fail": 0, "failures": 0, "complete": 0, "iteration-id": 1}

    def test_plain_timeout_removes_and_attributes(self):
        abort, quit_flag = self.state.evaluate_test_roadblock(
            "1-1-1:infra-start-end", self.ROADBLOCK_EXITS["timeout"], self.sample_info, ["client-1"], 0, 0
        )
        self.assertEqual(quit_flag, 1)
        self.assertNotIn("client-1", self.state.active_followers)
        self.assertEqual(
            self.state.dropped_followers_log,
            [{"follower": "client-1", "roadblock": "1-1-1:infra-start-end"}],
        )

    def test_heartbeat_timeout_removes_and_attributes(self):
        # Regression test: heartbeat_timeout (rc=5) used to fall through to
        # the generic "unknown state" branch, which never attributed the
        # drop -- the caller's separate unconditional remove_dropped_followers()
        # call was the only thing that fired, recording roadblock=None and
        # failing schema validation on the resulting rickshaw-run.json.
        abort, quit_flag = self.state.evaluate_test_roadblock(
            "1-1-1:client-start-end", self.ROADBLOCK_EXITS["heartbeat_timeout"], self.sample_info, ["client-1"], 0, 0
        )
        self.assertEqual(quit_flag, 1)
        self.assertNotIn("client-1", self.state.active_followers)
        self.assertEqual(
            self.state.dropped_followers_log,
            [{"follower": "client-1", "roadblock": "1-1-1:client-start-end"}],
        )

    def test_abort_marks_sample_failure(self):
        self.state.run["max-sample-failures"] = 5
        abort, quit_flag = self.state.evaluate_test_roadblock(
            "1-1-1:client-stop-begin", self.ROADBLOCK_EXITS["abort"], self.sample_info, [], 0, 0
        )
        self.assertEqual(abort, 1)
        self.assertEqual(quit_flag, 0)
        self.assertEqual(self.sample_info["failures"], 1)
        self.assertEqual(self.sample_info["attempt-fail"], 1)
        self.assertEqual(self.state.dropped_followers_log, [])

    def test_abort_at_max_failures_marks_sample_complete(self):
        self.state.run["max-sample-failures"] = 1
        self.state.evaluate_test_roadblock(
            "1-1-1:client-stop-begin", self.ROADBLOCK_EXITS["abort"], self.sample_info, [], 0, 0
        )
        self.assertEqual(self.sample_info["complete"], 1)

    def test_success_is_a_noop(self):
        abort, quit_flag = self.state.evaluate_test_roadblock(
            "1-1-1:infra-start-end", self.ROADBLOCK_EXITS["success"], self.sample_info, [], 0, 0
        )
        self.assertEqual((abort, quit_flag), (0, 0))
        self.assertEqual(self.state.dropped_followers_log, [])


if __name__ == "__main__":
    unittest.main()
