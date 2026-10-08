# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Tests for remotehosts tool tag inheritance and host-level aggregation."""

import contextlib
import importlib.machinery
import importlib.util
import logging
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock


def import_remotehosts():
    """Load remotehosts.py with endpoint and toolbox dependencies stubbed."""
    mock_endpoints = types.ModuleType("endpoints")
    mock_fabric = types.ModuleType("fabric")
    mock_fabric.Connection = object
    mock_jsonschema = types.ModuleType("jsonschema")
    mock_requests = types.ModuleType("requests")
    mock_paramiko = types.ModuleType("paramiko")
    mock_paramiko.__path__ = []
    mock_ssh_exception = types.ModuleType("paramiko.ssh_exception")
    mock_ssh_exception.AuthenticationException = Exception
    mock_ssh_exception.NoValidConnectionsError = Exception
    mock_paramiko.ssh_exception = mock_ssh_exception

    mock_toolbox = types.ModuleType("toolbox")
    mock_toolbox.__path__ = []
    mock_toolbox_json = types.ModuleType("toolbox.json")
    mock_toolbox.json = mock_toolbox_json

    mocks = {
        "endpoints": mock_endpoints,
        "fabric": mock_fabric,
        "jsonschema": mock_jsonschema,
        "paramiko": mock_paramiko,
        "paramiko.ssh_exception": mock_ssh_exception,
        "requests": mock_requests,
        "toolbox": mock_toolbox,
        "toolbox.json": mock_toolbox_json,
    }
    saved_modules = {name: sys.modules.get(name) for name in mocks}
    saved_path = list(sys.path)
    sys.modules.update(mocks)

    module_name = "remotehosts_tool_tags_under_test"
    sys.modules.pop(module_name, None)
    script_path = Path(__file__).resolve().parent.parent / "endpoints" / "remotehosts" / "remotehosts.py"

    with tempfile.TemporaryDirectory() as toolbox_home:
        os.makedirs(os.path.join(toolbox_home, "python"))
        previous_toolbox_home = os.environ.get("TOOLBOX_HOME")
        os.environ["TOOLBOX_HOME"] = toolbox_home
        try:
            loader = importlib.machinery.SourceFileLoader(module_name, str(script_path))
            spec = importlib.util.spec_from_loader(module_name, loader)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
        finally:
            sys.modules.pop(module_name, None)
            for name, previous in saved_modules.items():
                if previous is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = previous
            sys.path[:] = saved_path
            if previous_toolbox_home is None:
                os.environ.pop("TOOLBOX_HOME", None)
            else:
                os.environ["TOOLBOX_HOME"] = previous_toolbox_home

    return module


def make_remote(host, settings=None):
    config = {"host": host}
    if settings is not None:
        config["settings"] = settings
    return {"config": config, "engines": []}


class TestRemotehostsToolTags(unittest.TestCase):
    def setUp(self):
        self.remotehosts = import_remotehosts()
        self.remotehosts.args = SimpleNamespace(
            endpoint_index=0,
            endpoint_label="remotehosts-0",
            validate=False,
        )
        self.remotehosts.logger = logging.getLogger("test_remotehosts_tool_tags")
        self.remotehosts.endpoint_defaults["controller-ip-address"] = "192.0.2.1"
        self.remotehosts.endpoint_defaults["disable-tools"] = True

    def normalize(self, endpoint):
        rickshaw = {"userenvs": {"default": {"benchmarks": "test-userenv"}}}
        return self.remotehosts.normalize_endpoint_settings(endpoint, rickshaw)

    def test_endpoint_defaults_are_kept_once_per_host(self):
        remotes = [make_remote("node-a") for _ in range(1000)]
        endpoint = {
            "settings": {
                "tool-opt-in-tags": ["endpoint-in"],
                "tool-opt-out-tags": ["endpoint-out"],
            },
            "remotes": remotes,
        }

        self.normalize(endpoint)
        self.assertTrue(all(
            "tool-opt-in-tags" not in remote["config"]["settings"]
            and "tool-opt-out-tags" not in remote["config"]["settings"]
            for remote in remotes
        ))

        host_tags = self.remotehosts.build_remote_host_tool_tags(endpoint)
        self.assertEqual(host_tags["node-a"]["tool-opt-in-tags"], ["endpoint-in"])
        self.assertEqual(host_tags["node-a"]["tool-opt-out-tags"], ["endpoint-out"])

    def test_remote_overrides_combine_without_reintroducing_defaults(self):
        endpoint = {
            "settings": {
                "tool-opt-in-tags": ["endpoint-in"],
                "tool-opt-out-tags": ["endpoint-out"],
            },
            "remotes": [
                make_remote("node-a", {
                    "tool-opt-in-tags": ["first"],
                    "tool-opt-out-tags": [],
                }),
                make_remote("node-a"),
                make_remote("node-a", {"tool-opt-in-tags": ["second"]}),
                make_remote("node-b"),
                make_remote("node-c", {"tool-opt-in-tags": []}),
                make_remote("node-c", {"tool-opt-in-tags": ["third"]}),
            ],
        }
        self.normalize(endpoint)

        host_tags = self.remotehosts.build_remote_host_tool_tags(endpoint)
        self.assertEqual(host_tags["node-a"]["tool-opt-in-tags"], ["first", "second"])
        self.assertEqual(host_tags["node-a"]["tool-opt-out-tags"], [])
        self.assertEqual(host_tags["node-b"]["tool-opt-in-tags"], ["endpoint-in"])
        self.assertEqual(host_tags["node-b"]["tool-opt-out-tags"], ["endpoint-out"])
        self.assertEqual(host_tags["node-c"]["tool-opt-in-tags"], ["third"])
        self.assertEqual(host_tags["node-c"]["tool-opt-out-tags"], ["endpoint-out"])

    def test_unique_remote_config_uses_effective_tags_once_per_host(self):
        endpoint = {
            "settings": {"tool-opt-in-tags": ["endpoint-in"]},
            "remotes": [
                make_remote("node-a", {"tool-opt-in-tags": ["first"]}),
                make_remote("node-a"),
                make_remote("node-a", {"tool-opt-in-tags": ["second"]}),
            ],
        }
        self.normalize(endpoint)

        self.remotehosts.endpoints.remote_connection = MagicMock(
            side_effect=lambda *args, **kwargs: contextlib.nullcontext(object())
        )
        self.remotehosts.endpoints.run_remote = MagicMock(
            return_value=SimpleNamespace(exited=0, stdout="x86_64")
        )
        self.remotehosts.endpoints.log_settings = MagicMock()
        self.remotehosts.settings = {"run-file": {"endpoints": [endpoint]}}

        self.assertEqual(self.remotehosts.build_unique_remote_configs(), 0)
        unique_remote = self.remotehosts.settings["engines"]["remotes"]["node-a"]
        self.assertEqual(unique_remote["tool-opt-in-tags"], ["first", "second"])
        self.assertEqual(unique_remote["run-file-idx"], [0, 1, 2])


if __name__ == "__main__":
    unittest.main()
