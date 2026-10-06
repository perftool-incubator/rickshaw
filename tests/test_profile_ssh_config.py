#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Regression tests for generated SSH identity-profile host stanzas."""

import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


RICKSHAW_ROOT = Path(__file__).resolve().parents[1]
ENDPOINT_BASE = RICKSHAW_ROOT / "endpoints" / "base"
SSH = shutil.which("ssh")


@unittest.skipUnless(SSH, "OpenSSH client is required")
class TestProfileSSHJumpConfig(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir()
        self.ssh_config = self.root / "ssh_config"
        self.ssh_wrapper = self.bin_dir / "ssh"
        self.ssh_wrapper.write_text(
            "#!/bin/sh\n"
            f"exec {shlex.quote(SSH)} -F {shlex.quote(str(self.ssh_config))} \"$@\"\n"
        )
        self.ssh_wrapper.chmod(0o755)
        self.known_hosts = self.root / "managed_known_hosts"
        self.agent_socket = self.root / "filtered_agent.sock"
        self.env = os.environ.copy()
        self.env["HOME"] = str(self.home)
        self.env["PATH"] = f"{self.bin_dir}{os.pathsep}{self.env['PATH']}"
        self.env["CRUCIBLE_SSH_KNOWN_HOSTS_FILE"] = str(self.known_hosts)
        self.env["SSH_AUTH_SOCK"] = str(self.agent_socket)

    def _resolved_jump_settings(self, proxy_jump, host):
        self.ssh_config.write_text(
            f"Host target.example\n    ProxyJump {proxy_jump}\n"
        )
        command = (
            f"source {shlex.quote(str(ENDPOINT_BASE))}; "
            "profile_ssh_config root@target.example"
        )
        result = subprocess.run(
            ["bash", "-c", command],
            check=True,
            capture_output=True,
            text=True,
            env=self.env,
        )
        generated_config = Path(result.stdout.strip().splitlines()[-1])
        self.addCleanup(generated_config.unlink, missing_ok=True)

        resolved = subprocess.run(
            [SSH, "-G", "-F", str(generated_config), host],
            check=True,
            capture_output=True,
            text=True,
            env=self.env,
        )
        settings = {}
        for line in resolved.stdout.splitlines():
            key, _, value = line.partition(" ")
            settings.setdefault(key.lower(), []).append(value)
        return settings

    def _assert_managed_jump_settings(self, proxy_jump, host, port):
        settings = self._resolved_jump_settings(proxy_jump, host)
        self.assertEqual(settings["host"], [host])
        self.assertEqual(settings["port"], [str(port)])
        self.assertEqual(settings["identityfile"], ["none"])
        self.assertEqual(settings["stricthostkeychecking"], ["accept-new"])
        self.assertEqual(settings["userknownhostsfile"], [str(self.known_hosts)])
        self.assertEqual(settings["globalknownhostsfile"], ["/dev/null"])
        self.assertEqual(settings["forwardagent"], ["no"])

    def test_bracketed_ipv6_jump_with_user_and_port(self):
        self._assert_managed_jump_settings(
            "jump@[2001:db8::1]:2222", "2001:db8::1", 2222
        )

    def test_bracketed_ipv6_jump_without_user_or_port(self):
        self._assert_managed_jump_settings(
            "[2001:db8::2]", "2001:db8::2", 22
        )

    def test_hostname_jump_with_user_and_port(self):
        self._assert_managed_jump_settings(
            "jump@jump.example:2200", "jump.example", 2200
        )


if __name__ == "__main__":
    unittest.main()
