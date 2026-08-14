#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-run.py's RunState.build_tool_cmd().

Tools render params the same way benchmarks do -- '--arg=value' tokens via
render_param(), quoted only when needed -- rather than the old bash-syntax
'declare -a ARGS=(...)' construction. "collector.param_regex" (rickshaw#867)
lets a tool param declare a placeholder value (e.g. "ON"/"OFF") for
multiplex validation purposes while build_tool_cmd() rewrites the rendered
command via sed-style regex before splitting it back into an argv list --
sharing the same grammar as benchmark.json's client/server "param_regex".

toolbox is mocked out rather than required, since rickshaw-run.py imports
from it at module scope and CI does not check toolbox out for this job.
"""

import importlib.machinery
import importlib.util
import logging
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

    mod_name = "rickshaw_run_under_test_build_tool_cmd"
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

    mod.logger = logging.getLogger("test_build_tool_cmd")
    return mod


class TestBuildToolCmd(unittest.TestCase):
    def setUp(self):
        self.mod = import_rickshaw_run()
        self.state = self.mod.RunState()

    def _collector(self, **overrides):
        collector = {"start": "kerneltools-start", "stop": "kerneltools-stop"}
        collector.update(overrides)
        return collector

    def test_no_param_regex_configured_renders_normally(self):
        self.state.tools_configs = {"kernel": {"collector": self._collector()}}
        tool_entry = {
            "tool": "kernel",
            "tool-id": "kernel",
            "params": [{"arg": "interval", "val": "5"}],
        }
        tool = self.state.build_tool_cmd(tool_entry, "start")
        self.assertEqual(tool["argv"], ["kerneltools-start", "--interval=5"])

    def test_bare_flag_with_no_value(self):
        self.state.tools_configs = {"kernel": {"collector": self._collector()}}
        tool_entry = {
            "tool": "kernel",
            "tool-id": "kernel",
            "params": [{"arg": "some-flag", "val": ""}],
        }
        tool = self.state.build_tool_cmd(tool_entry, "start")
        self.assertEqual(tool["argv"], ["kerneltools-start", "--some-flag"])

    def test_zero_value_is_not_silently_dropped(self):
        # a code-review catch: an int/JSON-number val of 0 is "falsy" in
        # Python but is a real, meaningful value -- must not collapse to a
        # bare flag
        self.state.tools_configs = {"kernel": {"collector": self._collector()}}
        tool_entry = {
            "tool": "kernel",
            "tool-id": "kernel",
            "params": [{"arg": "interval", "val": 0}],
        }
        tool = self.state.build_tool_cmd(tool_entry, "start")
        self.assertEqual(tool["argv"], ["kerneltools-start", "--interval=0"])

    def test_disabled_param_is_skipped(self):
        self.state.tools_configs = {"kernel": {"collector": self._collector()}}
        tool_entry = {
            "tool": "kernel",
            "tool-id": "kernel",
            "params": [
                {"arg": "interval", "val": "5"},
                {"arg": "skip-me", "val": "1", "enabled": "no"},
            ],
        }
        tool = self.state.build_tool_cmd(tool_entry, "start")
        self.assertEqual(tool["argv"], ["kerneltools-start", "--interval=5"])

    def test_value_with_space_survives_round_trip(self):
        self.state.tools_configs = {"kernel": {"collector": self._collector()}}
        tool_entry = {
            "tool": "kernel",
            "tool-id": "kernel",
            "params": [{"arg": "record-opts", "val": "-e cycles -c 1000"}],
        }
        tool = self.state.build_tool_cmd(tool_entry, "start")
        self.assertEqual(
            tool["argv"], ["kerneltools-start", "--record-opts=-e cycles -c 1000"]
        )

    def test_param_regex_on_strips_value_keeps_bare_flag(self):
        # converged grammar: identical text to benchmarks' shipped ON/OFF
        # patterns (fio, trafficgen), now that both sides use the same
        # '--arg=value' rendering
        collector = self._collector(param_regex=[
            r"s/(\s--[^\s]+)=ON/$1/g",
            r"s/\s--[^\s]+=OFF//g",
        ])
        self.state.tools_configs = {"kernel": {"collector": collector}}
        tool_entry = {
            "tool": "kernel",
            "tool-id": "kernel",
            "params": [
                {"arg": "subtools", "val": "perf"},
                {"arg": "perf-gen-local-report", "val": "ON"},
            ],
        }
        tool = self.state.build_tool_cmd(tool_entry, "stop")
        self.assertEqual(
            tool["argv"],
            ["kerneltools-stop", "--subtools=perf", "--perf-gen-local-report"],
        )

    def test_param_regex_off_removes_flag_and_value(self):
        collector = self._collector(param_regex=[
            r"s/(\s--[^\s]+)=ON/$1/g",
            r"s/\s--[^\s]+=OFF//g",
        ])
        self.state.tools_configs = {"kernel": {"collector": collector}}
        tool_entry = {
            "tool": "kernel",
            "tool-id": "kernel",
            "params": [
                {"arg": "subtools", "val": "perf"},
                {"arg": "perf-gen-local-report", "val": "OFF"},
            ],
        }
        tool = self.state.build_tool_cmd(tool_entry, "stop")
        self.assertEqual(tool["argv"], ["kerneltools-stop", "--subtools=perf"])

    def test_param_regex_is_generic_across_multiple_flags(self):
        # the shipped patterns match any '--flag' name, not just one hardcoded
        # param, so adding a second ON/OFF-style tool param never requires a
        # rickshaw.json param_regex edit
        collector = self._collector(param_regex=[
            r"s/(\s--[^\s]+)=ON/$1/g",
            r"s/\s--[^\s]+=OFF//g",
        ])
        self.state.tools_configs = {"kernel": {"collector": collector}}
        tool_entry = {
            "tool": "kernel",
            "tool-id": "kernel",
            "params": [
                {"arg": "foo-flag", "val": "ON"},
                {"arg": "subtools", "val": "perf"},
                {"arg": "bar-flag", "val": "OFF"},
                {"arg": "perf-gen-local-report", "val": "ON"},
            ],
        }
        tool = self.state.build_tool_cmd(tool_entry, "stop")
        self.assertEqual(
            tool["argv"],
            [
                "kerneltools-stop",
                "--foo-flag",
                "--subtools=perf",
                "--perf-gen-local-report",
            ],
        )

    def test_param_regex_leaving_unbalanced_quote_raises(self):
        # a \S+-anchored pattern (the fio 'jobfile'-class fragility found
        # during design review) can leave a dangling unbalanced quote if it
        # ever matches into a quoted (space-containing) value -- this must
        # fail loudly via sys.exit, not silently mis-tokenize
        collector = self._collector(param_regex=[
            r"s/\s--record-opts=(\S+)//",
        ])
        self.state.tools_configs = {"kernel": {"collector": collector}}
        tool_entry = {
            "tool": "kernel",
            "tool-id": "kernel",
            "params": [{"arg": "record-opts", "val": "-e cycles -c 1000"}],
        }
        with self.assertRaises(SystemExit):
            self.state.build_tool_cmd(tool_entry, "start")

    def test_blacklisted_endpoint_returns_none(self):
        collector = self._collector(blacklist=[
            {"endpoint": "remotehosts", "collector-types": ["client", "server"]},
        ])
        self.state.tools_configs = {"kernel": {"collector": collector}}
        tool_entry = {"tool": "kernel", "tool-id": "kernel", "params": []}
        tool = self.state.build_tool_cmd(tool_entry, "start", endpoint_type="remotehosts")
        self.assertIsNone(tool)


if __name__ == "__main__":
    unittest.main()
