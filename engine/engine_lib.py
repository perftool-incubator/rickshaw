#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import argparse
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

TOOLBOX_HOME = os.environ.get("TOOLBOX_HOME", "/opt/toolbox")
sys.path.append(str(Path(TOOLBOX_HOME) / "python"))

ROADBLOCK_HOME = os.environ.get("ROADBLOCK_HOME")
if ROADBLOCK_HOME:
    sys.path.append(ROADBLOCK_HOME)

from fabric import Connection
from invoke import run as invoke_run
from toolbox.json import load_json_file
from toolbox.jsonsettings import get_json_setting
from toolbox.messages import (
    ROADBLOCK_EXITS,
    evaluate_roadblock_result,
    prepare_user_msgs_file,
    resolve_svc_messages,
    save_received_messages,
)
from toolbox.roadblock import do_roadblock as tb_do_roadblock

logger = logging.getLogger("engine")

RUNTIME_PADDING = 180


def run_command(cmd, hide=False, **kwargs):
    """Run a local command via invoke and log the command, output, and rc."""
    logger.info("Running command: %s", cmd)
    result = invoke_run(cmd, warn=True, hide=hide, **kwargs)
    if result.stdout and result.stdout.strip():
        logger.info("Command stdout:\n%s", result.stdout.rstrip())
    if result.stderr and result.stderr.strip():
        logger.info("Command stderr:\n%s", result.stderr.rstrip())
    if not result.stdout and not result.stderr:
        logger.info("Command produced no output")
    logger.info("Command return code: %d", result.return_code)
    return result


class EngineError(Exception):
    pass


class Engine:
    """Rickshaw engine lifecycle manager.

    Manages the full engine lifecycle inside a benchmark/tool container:
    initialization, config retrieval, tool management, benchmark execution
    with roadblock synchronization, and data archival.
    """

    def __init__(self):
        self.rickshaw_host = ""
        self.base_run_dir = ""
        self.endpoint_run_dir = ""
        self.cs_label = ""
        self.roadblock_id = ""
        self.roadblock_passwd = ""
        self.disable_tools = False
        self.endpoint = ""
        self.osruntime = ""
        self.max_sample_failures = 3
        self.engine_script_start_timeout = 0

        self.cs_type = ""
        self.cs_id = ""
        self.cs_buddy = ""
        self.tool_name = ""
        self.endpoint_label = ""

        self.cs_dir = ""
        self.roadblock_msgs_dir = ""
        self.sysinfo_dir = ""
        self.tool_start_cmds = ""
        self.tool_stop_cmds = ""
        self.config_dir = ""
        self.engine_config_dir = ""
        self.tool_cmds_dir = ""
        self.run_dir = ""
        self.archives_dir = ""

        self.abort = False
        self.quit = False

        self.default_timeout = 300
        self.collect_sysinfo_timeout = 300
        self.roadblock_connection_watchdog = True
        self.roadblock_log_level = "normal"

        self.ssh_id_file = os.environ.get(
            "ssh_id_file",
            os.path.join(
                os.environ.get("ENGINES_SHARED_DIR", "/shared-engines-dir"),
                "rickshaw_ssh_id",
            ),
        )

        self.json_settings_file = os.environ.get(
            "json_settings_file",
            os.path.join(
                os.environ.get("ENGINES_SHARED_DIR", "/shared-engines-dir"),
                "rickshaw-settings.json.xz",
            ),
        )

        self._settings_data = None

    # ---- Settings --------------------------------------------------------

    def load_settings(self):
        """Load rickshaw-settings.json.xz and cache the parsed data."""
        self._settings_data, err = load_json_file(
            self.json_settings_file, uselzma=True
        )
        if self._settings_data is None:
            raise EngineError(
                "Failed to load settings from %s: %s"
                % (self.json_settings_file, err)
            )

    def load_setting(self, query):
        """Look up a single setting by dot-notation path."""
        if self._settings_data is None:
            self.load_settings()
        value, rc = get_json_setting(self._settings_data, query)
        if rc != 0:
            raise EngineError("Settings query '%s' failed" % query)
        return value

    def load_roadblock_settings(self):
        self.default_timeout = int(
            self.load_setting("roadblock.timeouts.default")
        )
        self.collect_sysinfo_timeout = int(
            self.load_setting("roadblock.timeouts.collect-sysinfo")
        )
        self.roadblock_connection_watchdog = self.load_setting(
            "roadblock.connection-watchdog"
        )
        self.roadblock_log_level = self.load_setting("roadblock.log-level")

    # ---- Argument parsing ------------------------------------------------

    def parse_args(self, argv=None):
        """Parse engine configuration from CLI arguments and environment.

        Bootstrap parses the original CLI arguments, exports them as
        environment variables, and execs engine.py with no arguments.
        So we read from env vars with CLI args as overrides.
        """
        parser = argparse.ArgumentParser(description="Rickshaw engine")
        parser.add_argument("--rickshaw-host", default="")
        parser.add_argument("--base-run-dir", default="")
        parser.add_argument("--endpoint-run-dir", default="")
        parser.add_argument("--cs-label", default="")
        parser.add_argument("--roadblock-server", dest="rickshaw_host_alt", default="")
        parser.add_argument("--roadblock-passwd", default="")
        parser.add_argument("--roadblock-id", default="")
        parser.add_argument("--disable-tools", default="")
        parser.add_argument("--endpoint", default="")
        parser.add_argument("--osruntime", default="")
        parser.add_argument("--max-sample-failures", default="")
        parser.add_argument("--engine-script-start-timeout", default="")

        args, _ = parser.parse_known_args(argv)

        def _env_or_arg(arg_val, *env_keys):
            if arg_val:
                return arg_val
            for key in env_keys:
                val = os.environ.get(key, "")
                if val:
                    return val
            return ""

        self.rickshaw_host = _env_or_arg(
            args.rickshaw_host or args.rickshaw_host_alt,
            "rickshaw_host", "roadblock_server",
        )
        self.base_run_dir = _env_or_arg(
            args.base_run_dir, "base_run_dir",
        )
        self.endpoint_run_dir = _env_or_arg(
            args.endpoint_run_dir, "endpoint_run_dir",
        )
        self.cs_label = _env_or_arg(args.cs_label, "cs_label")
        self.roadblock_passwd = _env_or_arg(
            args.roadblock_passwd, "roadblock_passwd",
        )
        self.roadblock_id = _env_or_arg(args.roadblock_id, "roadblock_id")
        self.endpoint = _env_or_arg(args.endpoint, "endpoint")
        self.osruntime = _env_or_arg(args.osruntime, "osruntime")

        disable_tools_str = _env_or_arg(
            args.disable_tools, "disable_tools",
        )
        self.disable_tools = disable_tools_str == "1"

        max_sf = _env_or_arg(
            args.max_sample_failures, "max_sample_failures",
        )
        if max_sf:
            self.max_sample_failures = int(max_sf)

        est = _env_or_arg(
            args.engine_script_start_timeout, "engine_script_start_timeout",
        )
        if est:
            self.engine_script_start_timeout = int(est)
            logger.info(
                "engine_script_start_timeout set to %d",
                self.engine_script_start_timeout,
            )
        else:
            self.engine_script_start_timeout = int(
                self.load_setting("roadblock.timeouts.engine-start")
            )

    # ---- Environment validation ------------------------------------------

    def validate_env(self):
        logger.info("Validating core environment")

        if not self.rickshaw_host:
            raise EngineError("rickshaw host not set")
        logger.info("rickshaw_host=%s", self.rickshaw_host)

        if not self.roadblock_id:
            logger.warning("No roadblock ID provided")
        if not self.roadblock_passwd:
            logger.warning("No roadblock password provided")

        if not self.cs_label:
            raise EngineError("--cs-label was not defined")

        profiler_re = re.compile(
            r"^profiler-([a-zA-Z0-9]+)-([0-9]+)-([a-zA-Z0-9-]+)-([0-9]+)$"
        )
        cs_re = re.compile(r"^([a-zA-Z0-9]+)-([a-zA-Z0-9]+)$")

        m = profiler_re.match(self.cs_label)
        if m:
            self.tool_name = re.sub(
                r"^[^-]+-[^-]+-[0-9]+-(.+)-[0-9]+$", r"\1", self.cs_label
            )
            logger.info(
                "Engine label '%s' is a profiler for tool: %s",
                self.cs_label,
                self.tool_name,
            )
        elif cs_re.match(self.cs_label):
            logger.info("Engine label '%s' is valid", self.cs_label)
        else:
            raise EngineError("cs_label '%s' is not valid" % self.cs_label)

        if not self.endpoint_run_dir:
            raise EngineError("--endpoint-run-dir was not defined")
        logger.info("endpoint_run_dir=%s", self.endpoint_run_dir)
        logger.info(
            "engine_script_start_timeout=%d", self.engine_script_start_timeout
        )

    # ---- Environment setup -----------------------------------------------

    def setup_env(self):
        os.environ["RS_CS_LABEL"] = self.cs_label
        parts = self.cs_label.split("-")
        self.cs_type = parts[0]
        self.cs_id = parts[1] if len(parts) > 1 else ""

        self.cs_dir = tempfile.mkdtemp()
        logger.info("cs_dir: %s", self.cs_dir)

        self.tool_start_cmds = os.path.join(self.cs_dir, "tool-start.json.xz")
        self.tool_stop_cmds = os.path.join(self.cs_dir, "tool-stop.json.xz")
        self.roadblock_msgs_dir = os.path.join(self.cs_dir, "roadblock-msgs")
        self.sysinfo_dir = os.path.join(self.cs_dir, "sysinfo")
        os.makedirs(self.roadblock_msgs_dir, exist_ok=True)
        os.makedirs(self.sysinfo_dir, exist_ok=True)

        self.config_dir = os.path.join(self.base_run_dir, "config")
        self.engine_config_dir = os.path.join(self.config_dir, "engine")
        self.tool_cmds_dir = os.path.join(
            self.config_dir, "tool-cmds", self.cs_type
        )
        self.run_dir = os.path.join(self.base_run_dir, "run")
        self.archives_dir = os.path.join(self.run_dir, "engine", "archives")

    # ---- SSH/file transfer -----------------------------------------------

    def _connect(self):
        """Create a Fabric Connection with a pre-configured paramiko client.

        We create the paramiko SSHClient ourselves so we can set
        AutoAddPolicy before connecting. Then we assign the connected
        client to the Fabric Connection so get()/put() use it.
        """
        import paramiko

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=self.rickshaw_host,
            key_filename=self.ssh_id_file,
            look_for_keys=False,
            allow_agent=False,
            timeout=30,
        )

        conn = Connection(self.rickshaw_host)
        conn.client = client
        conn.transport = client.get_transport()
        return conn

    def _sftp_get_recursive(self, sftp, remote_path, local_path):
        """Recursively download a remote directory via SFTP."""
        import stat as stat_module
        remote_attr = sftp.stat(remote_path)
        if not stat_module.S_ISDIR(remote_attr.st_mode):
            sftp.get(remote_path, local_path)
            return

        os.makedirs(local_path, exist_ok=True)
        for entry in sftp.listdir_attr(remote_path):
            remote_entry = remote_path.rstrip("/") + "/" + entry.filename
            local_entry = os.path.join(local_path, entry.filename)
            if stat_module.S_ISDIR(entry.st_mode):
                self._sftp_get_recursive(sftp, remote_entry, local_entry)
            else:
                sftp.get(remote_entry, local_entry)

    def scp_from_controller(self, remote_path, local_path, max_retries=10):
        resolved_local = local_path
        if os.path.isdir(resolved_local):
            resolved_local = os.path.join(
                resolved_local, os.path.basename(remote_path)
            )

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "SCP %s:%s -> %s (attempt %d/%d)",
                    self.rickshaw_host, remote_path, resolved_local,
                    attempt, max_retries,
                )
                conn = self._connect()
                try:
                    sftp = conn.client.open_sftp()
                    try:
                        import stat as stat_module
                        remote_attr = sftp.stat(remote_path)
                        if stat_module.S_ISDIR(remote_attr.st_mode):
                            self._sftp_get_recursive(
                                sftp, remote_path, resolved_local
                            )
                        else:
                            conn.get(remote_path, local=resolved_local)
                    finally:
                        sftp.close()
                finally:
                    conn.close()
                logger.info("SCP succeeded")
                return
            except Exception as exc:
                logger.warning(
                    "SCP attempt %d/%d failed: [%s] %s",
                    attempt, max_retries, type(exc).__name__, exc,
                )
                if attempt < max_retries:
                    time.sleep(attempt)
        raise EngineError(
            "SCP failed after %d attempts: %s" % (max_retries, remote_path)
        )

    def archive_to_controller(self, src_dir, dest_path):
        for attempt in range(1, 11):
            try:
                logger.info(
                    "Archiving %s to %s:%s (attempt %d)",
                    src_dir, self.rickshaw_host, dest_path, attempt,
                )
                conn = self._connect()
                try:
                    with tempfile.NamedTemporaryFile(suffix=".tgz") as tmp:
                        run_command(
                            "tar czf %s -C %s ." % (tmp.name, src_dir),
                            hide=True,
                        )
                        conn.put(tmp.name, dest_path)
                finally:
                    conn.close()
                logger.info("Archive succeeded")
                return
            except Exception as exc:
                logger.warning(
                    "Archive attempt %d failed: [%s] %s",
                    attempt, type(exc).__name__, exc,
                )
                time.sleep(attempt)
        raise EngineError("Archive failed after 10 attempts")

    # ---- Roadblock -------------------------------------------------------

    def do_roadblock(self, label, timeout, messages=None, wait_for=None,
                     do_abort=False):
        logger.info("Roadblock: %s (timeout=%d)", label, timeout)

        rc, msgs_data = tb_do_roadblock(
            roadblock_id=self.roadblock_id,
            label=label,
            role="follower",
            follower_id=self.cs_label,
            leader_id="controller",
            timeout=timeout,
            redis_server=self.rickshaw_host,
            redis_password=self.roadblock_passwd,
            messages=messages,
            abort=do_abort,
            connection_watchdog=(
                self.roadblock_connection_watchdog in (True, "enabled", "true")
            ),
            log_level=self.roadblock_log_level,
            msgs_dir=self.roadblock_msgs_dir,
            wait_for=wait_for,
        )
        return rc

    def roadblock_exit_on_error(self, rc):
        if rc != ROADBLOCK_EXITS["success"]:
            logger.error("Roadblock failed with rc=%d, exiting", rc)
            sys.exit(rc)

    def abort_error(self, msg, sync_label):
        logger.error(msg)
        msg_file = os.path.join(
            tempfile.mkdtemp(), "abort-msg.json"
        )
        with open(msg_file, "w") as fp:
            json.dump(
                [
                    {
                        "recipient": {"type": "all", "id": "all"},
                        "user-object": {"error": msg},
                    }
                ],
                fp,
            )
        self.do_roadblock(sync_label, 300, messages=msg_file, do_abort=True)
        self.abort = True

    # ---- Data retrieval --------------------------------------------------

    def get_data(self):
        logger.info("Retrieving data from controller")

        if self.cs_type in ("client", "server"):
            cs_files_list = "%s-%s-files-list" % (self.cs_type, self.cs_id)
        else:
            cs_files_list = "%s-files-list" % self.cs_type

        self.scp_from_controller(
            os.path.join(self.engine_config_dir, cs_files_list), cs_files_list
        )

        with open(cs_files_list) as fp:
            lines = [l.strip() for l in fp if l.strip()]

        i = 0
        while i < len(lines):
            src_match = re.match(r"^src=(.+)$", lines[i])
            if not src_match:
                self.abort_error(
                    "source file not found in files-list", "get-data-end"
                )
                return
            src = src_match.group(1)
            i += 1
            if i >= len(lines):
                self.abort_error(
                    "dest file not found in files-list", "get-data-end"
                )
                return
            dest_match = re.match(r"^dest=(.+)$", lines[i])
            if not dest_match:
                self.abort_error(
                    "dest file not found in files-list", "get-data-end"
                )
                return
            dest = dest_match.group(1)
            i += 1
            self.scp_from_controller(src, dest)

        if self.cs_type in ("client", "server"):
            self.scp_from_controller(
                os.path.join(self.engine_config_dir, "pairing.json"),
                "pairing.json",
            )
            buddy_source = "ID-matching (fallback)"
            if os.path.isfile("pairing.json"):
                with open("pairing.json") as fp:
                    pairing = json.load(fp)
                self.cs_buddy = pairing.get(self.cs_label, "")
                if self.cs_buddy:
                    buddy_source = "pairing manifest"
            if not self.cs_buddy:
                if self.cs_type == "client":
                    self.cs_buddy = "server-%s" % self.cs_id
                else:
                    self.cs_buddy = "client-%s" % self.cs_id
            logger.info(
                "Engine buddy: %s (source: %s)", self.cs_buddy, buddy_source
            )

        if self.cs_type in ("client", "server"):
            self.scp_from_controller(
                os.path.join(
                    self.engine_config_dir,
                    "bench-cmds",
                    self.cs_type,
                    self.cs_id,
                    "start",
                ),
                "bench-start-cmds",
            )
        else:
            self.scp_from_controller(
                os.path.join(
                    self.engine_config_dir, "bench-cmds", "client", "1", "start"
                ),
                "bench-start-cmds",
            )

        if self.cs_type == "client":
            self.scp_from_controller(
                os.path.join(
                    self.engine_config_dir,
                    "bench-cmds",
                    self.cs_type,
                    self.cs_id,
                    "infra",
                ),
                "bench-infra-cmds",
            )
            if self.cs_id == "1":
                self.scp_from_controller(
                    os.path.join(
                        self.engine_config_dir,
                        "bench-cmds",
                        self.cs_type,
                        self.cs_id,
                        "runtime",
                    ),
                    "bench-runtime-cmds",
                )
        elif self.cs_type == "server":
            self.scp_from_controller(
                os.path.join(
                    self.engine_config_dir,
                    "bench-cmds",
                    self.cs_type,
                    self.cs_id,
                    "stop",
                ),
                "bench-stop-cmds",
            )

        if self.cs_type in ("client", "server"):
            self.scp_from_controller(
                os.path.join(
                    self.tool_cmds_dir, self.cs_id, "start.json.xz"
                ),
                self.tool_start_cmds,
            )
            self.scp_from_controller(
                os.path.join(
                    self.tool_cmds_dir, self.cs_id, "stop.json.xz"
                ),
                self.tool_stop_cmds,
            )
        else:
            self.scp_from_controller(
                os.path.join(self.tool_cmds_dir, "start.json.xz"),
                self.tool_start_cmds,
            )
            self.scp_from_controller(
                os.path.join(self.tool_cmds_dir, "stop.json.xz"),
                self.tool_stop_cmds,
            )

    # ---- System info -----------------------------------------------------

    def collect_sysinfo(self):
        logger.info("Collecting sysinfo")
        packrat = shutil.which("packrat")
        if packrat:
            result = run_command("%s %s" % (packrat, self.sysinfo_dir))
            if result.return_code != 0:
                self.abort_error(
                    "Critical error from packrat during sysinfo collection",
                    "collect-sysinfo-end",
                )
        else:
            logger.info("Packrat is not available")

    # ---- Tool lifecycle --------------------------------------------------

    def _parse_tool_commands(self, cmds_file):
        data, err = load_json_file(cmds_file, uselzma=True)
        if data is None:
            raise EngineError(
                "Failed to load tool commands from %s: %s" % (cmds_file, err)
            )
        return [(t["name"], t["command"]) for t in data.get("tools", [])]

    def start_tools(self, one_tool=None):
        logger.info("Starting tools")
        os.makedirs("tool-data", exist_ok=True)

        if self.disable_tools:
            logger.info("Not starting tools because --disable-tools=1")
            return

        for cmds_file in (self.tool_start_cmds, self.tool_stop_cmds):
            if not os.path.exists(cmds_file):
                self.abort_error(
                    "Tool cmd file %s not found" % cmds_file,
                    "start-tools-end",
                )
                return

        tools = self._parse_tool_commands(self.tool_start_cmds)
        total = 0
        for tool_name, tool_command in tools:
            if one_tool and one_tool != tool_name:
                logger.info(
                    "Skipping tool '%s' (engine runs '%s' only)",
                    tool_name,
                    one_tool,
                )
                continue

            total += 1
            tool_dir = os.path.join("tool-data", tool_name)
            os.makedirs(tool_dir, exist_ok=True)
            logger.info("Starting tool '%s'", tool_name)
            run_command("cd %s && %s" % (tool_dir, tool_command))

        if total == 0:
            logger.info("No tools configured for this engine")

    # ---- Benchmark execution ---------------------------------------------

    def run_bench_cmd(self, matching_type, cmd_type, cmd, force=False):
        if self.cs_type != matching_type:
            return 0
        if not force and (self.abort or self.quit):
            logger.info(
                "Skipping %s command (abort=%s, quit=%s)",
                cmd_type,
                self.abort,
                self.quit,
            )
            return 0
        if not cmd:
            logger.info("No %s command to run", cmd_type)
            return 0
        logger.info("Running %s command", cmd_type)
        result = run_command(cmd)
        return result.return_code

    def _load_bench_cmds(self, filename):
        if not os.path.exists(filename):
            return []
        with open(filename) as fp:
            return [line.strip() for line in fp if line.strip()]

    def _roadblock_and_evaluate(self, rb_name, timeout, iter_idx,
                                sample_data, msgs_file=None, do_abort=False):
        rc = self.do_roadblock(
            rb_name, timeout, messages=msgs_file, do_abort=do_abort
        )
        result = evaluate_roadblock_result(
            rc,
            rb_name,
            self.roadblock_msgs_dir,
            engine_label=self.cs_label if self.cs_type in ("client", "server") else None,
            buddy_label=self.cs_buddy if self.cs_type in ("client", "server") else None,
        )

        if result["is_timeout"]:
            self.quit = True
        if result["is_abort"] and not self.abort:
            sample_data[iter_idx]["attempt-fail"] = True
            sample_data[iter_idx]["failures"] += 1
            logger.info(
                "Sample failures now: %d", sample_data[iter_idx]["failures"]
            )
            if sample_data[iter_idx]["failures"] >= self.max_sample_failures:
                sample_data[iter_idx]["complete"] = True
                logger.error(
                    "Max failures (%d) reached for iteration %d",
                    sample_data[iter_idx]["failures"],
                    sample_data[iter_idx]["iteration-id"],
                )
            self.abort = True

        if result["messages"]:
            roadblock_label = rb_name.split(":")[1] if ":" in rb_name else rb_name
            save_received_messages(
                result["messages"], sample_data[iter_idx]["rx-msgs-dir"], roadblock_label
            )

    def process_bench_roadblocks(self):
        logger.info("Starting process_bench_roadblocks()")

        rc = self.do_roadblock("setup-bench-begin", self.default_timeout)
        self.roadblock_exit_on_error(rc)

        bench_start_cmds = self._load_bench_cmds("bench-start-cmds")
        bench_infra_cmds = self._load_bench_cmds("bench-infra-cmds")
        bench_runtime_cmds = self._load_bench_cmds("bench-runtime-cmds")
        bench_stop_cmds = self._load_bench_cmds("bench-stop-cmds")

        if not bench_start_cmds:
            self.abort_error("bench-start-cmds not found", "setup-bench-end")
            return

        total_tests = len(bench_start_cmds)
        sample_data = []
        for i, line in enumerate(bench_start_cmds):
            iter_samp = line.split()[0]
            parts = iter_samp.split("-")
            iter_id = int(parts[0])
            samp_id = int(parts[1])
            sample_data.append({
                "iteration-id": iter_id,
                "sample-id": samp_id,
                "failures": 0,
                "complete": False,
                "attempt-num": 0,
                "attempt-fail": False,
                "rx-msgs-dir": "",
            })
            logger.info(
                "Test %d: iteration=%d sample=%d", i, iter_id, samp_id
            )

        logger.info("Total tests: %d", total_tests)

        rc = self.do_roadblock("setup-bench-end", self.default_timeout)
        self.roadblock_exit_on_error(rc)

        for i in range(total_tests):
            if self.quit:
                break

            sd = sample_data[i]
            iter_id = sd["iteration-id"]
            samp_id = sd["sample-id"]
            iter_samp = bench_start_cmds[i].split()[0]

            iter_samp_dir = os.path.join(
                self.cs_dir,
                "iteration-%d" % iter_id,
                "sample-%d" % samp_id,
            )
            cs_tx_msgs_dir = os.path.join(iter_samp_dir, "msgs", "tx")
            cs_rx_msgs_dir = os.path.join(iter_samp_dir, "msgs", "rx")
            sd["rx-msgs-dir"] = cs_rx_msgs_dir

            if self.cs_type == "client":
                start_cmd = bench_start_cmds[i].split(None, 1)[1] if len(bench_start_cmds[i].split()) > 1 else ""
                runtime_cmd = bench_runtime_cmds[i].split(None, 1)[1] if i < len(bench_runtime_cmds) and len(bench_runtime_cmds[i].split()) > 1 else ""
                infra_cmd = bench_infra_cmds[i].split(None, 1)[1] if i < len(bench_infra_cmds) and len(bench_infra_cmds[i].split()) > 1 else ""
                stop_cmd = ""
            elif self.cs_type == "server":
                start_cmd = bench_start_cmds[i].split(None, 1)[1] if len(bench_start_cmds[i].split()) > 1 else ""
                stop_cmd = bench_stop_cmds[i].split(None, 1)[1] if i < len(bench_stop_cmds) and len(bench_stop_cmds[i].split()) > 1 else ""
                runtime_cmd = ""
                infra_cmd = ""
            else:
                start_cmd = runtime_cmd = infra_cmd = stop_cmd = ""

            self.abort = False

            while (not self.quit
                   and not self.abort
                   and not sd["complete"]
                   and sd["failures"] < self.max_sample_failures):

                sd["attempt-fail"] = False
                sd["attempt-num"] += 1

                logger.info(
                    "Starting iteration %d sample %d (test %d/%d) attempt %d/%d",
                    iter_id, samp_id, i + 1, total_tests,
                    sd["attempt-num"], self.max_sample_failures,
                )

                for d in (iter_samp_dir, cs_tx_msgs_dir, cs_rx_msgs_dir):
                    os.makedirs(d, exist_ok=True)
                sd["rx-msgs-dir"] = cs_rx_msgs_dir

                if self.cs_type in ("client", "server"):
                    self._copy_files_to_sample_dir(iter_samp_dir)

                orig_dir = os.getcwd()
                os.chdir(iter_samp_dir)

                default_recipients = None
                if self.cs_type in ("client", "server"):
                    recipients = [("follower", self.cs_buddy)]
                    if self.endpoint_label:
                        recipients.insert(
                            0, ("follower", self.endpoint_label)
                        )
                    default_recipients = recipients

                timeout = self.default_timeout
                force_server_stop = False
                unbounded_roadblock = False
                test_id = "%d-%d-%d" % (iter_id, samp_id, sd["attempt-num"])
                rb_prefix = "%s:" % test_id

                # ---- infra-start ----
                rb_name = rb_prefix + "infra-start-begin"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                abort_rc = self.run_bench_cmd("client", "infra", infra_cmd)
                do_abort_arg = abort_rc != 0
                if do_abort_arg:
                    self.abort = True

                rb_name = rb_prefix + "infra-start-end"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(
                    rb_name, timeout, i, sample_data, msgs_file,
                    do_abort=do_abort_arg,
                )

                # ---- server-start ----
                rb_name = rb_prefix + "server-start-begin"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                abort_rc = self.run_bench_cmd("server", "server", start_cmd)
                do_abort_arg = abort_rc != 0
                if do_abort_arg:
                    self.abort = True
                force_server_stop = True

                rb_name = rb_prefix + "server-start-end"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(
                    rb_name, timeout, i, sample_data, msgs_file,
                    do_abort=do_abort_arg,
                )

                # ---- endpoint-start ----
                rb_name = rb_prefix + "endpoint-start-begin"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                rb_name = rb_prefix + "endpoint-start-end"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)
                if self.cs_type == "client":
                    resolve_svc_messages(cs_rx_msgs_dir)

                # ---- client-start (with runtime estimation) ----
                if (not self.abort
                    and not self.quit
                    and self.cs_type == "client"
                    and self.cs_id == "1"
                    and runtime_cmd):
                    result = run_command(runtime_cmd)
                    runtime_output = result.stdout.strip()

                    if result.return_code == 0 and runtime_output:
                        if runtime_output == "-1":
                            logger.info("Workload requested unbounded roadblock timeout")
                            unbounded_roadblock = True
                            timeout_msg = json.dumps({
                                "recipient": {"type": "all", "id": "all"},
                                "user-object": {"timeout": "unbounded"},
                            })
                            with open(os.path.join(cs_tx_msgs_dir, "timeout"), "w") as fp:
                                fp.write(timeout_msg)
                        else:
                            tmp_timeout = int(runtime_output) + RUNTIME_PADDING
                            logger.info("Updated timeout: %d", tmp_timeout)
                            timeout_msg = json.dumps({
                                "recipient": {"type": "all", "id": "all"},
                                "user-object": {"timeout": str(tmp_timeout)},
                            })
                            with open(os.path.join(cs_tx_msgs_dir, "timeout"), "w") as fp:
                                fp.write(timeout_msg)
                            timeout = tmp_timeout

                rb_name = rb_prefix + "client-start-begin"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                if not unbounded_roadblock and timeout == self.default_timeout:
                    msgs_log = os.path.join(
                        self.roadblock_msgs_dir, "%s.json" % rb_name
                    )
                    if os.path.isfile(msgs_log):
                        rb_msgs, _ = load_json_file(msgs_log)
                        if rb_msgs and "received" in rb_msgs:
                            for msg in rb_msgs["received"]:
                                t = (
                                    msg.get("payload", {})
                                    .get("message", {})
                                    .get("user-object", {})
                                    .get("timeout")
                                )
                                if t is not None:
                                    if t == "unbounded":
                                        logger.info("Client engine running unbounded workload")
                                        unbounded_roadblock = True
                                    else:
                                        timeout = int(t)
                                        logger.info("Found new timeout: %d", timeout)
                                    break

                if unbounded_roadblock:
                    rb_name = rb_prefix + "client-start-end"
                    msgs_file = prepare_user_msgs_file(
                        cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                    )
                    wait_for_cmd = (
                        "python3 /usr/local/bin/engine_lib.py"
                        " run_bench_cmd '%s' 'client' 'client' '%s' '%s' '0' '%s'"
                        % (self.cs_type, self.abort, self.quit, start_cmd)
                    )
                    rc = self.do_roadblock(
                        rb_name, timeout, messages=msgs_file,
                        wait_for=wait_for_cmd,
                    )
                    result = evaluate_roadblock_result(
                        rc, rb_name, self.roadblock_msgs_dir,
                        engine_label=self.cs_label,
                        buddy_label=self.cs_buddy,
                    )
                    if result["is_timeout"]:
                        self.quit = True
                    if result["is_abort"]:
                        self.abort = True
                else:
                    abort_rc = self.run_bench_cmd("client", "client", start_cmd)
                    do_abort_arg = abort_rc != 0
                    if do_abort_arg:
                        self.abort = True

                    rb_name = rb_prefix + "client-start-end"
                    msgs_file = prepare_user_msgs_file(
                        cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                    )
                    self._roadblock_and_evaluate(
                        rb_name, timeout, i, sample_data, msgs_file,
                        do_abort=do_abort_arg,
                    )

                if timeout != self.default_timeout:
                    timeout = self.default_timeout
                    logger.info("Reset timeout to %d", timeout)

                # ---- client-stop ----
                rb_name = rb_prefix + "client-stop-begin"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                rb_name = rb_prefix + "client-stop-end"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                # ---- endpoint-stop ----
                rb_name = rb_prefix + "endpoint-stop-begin"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                rb_name = rb_prefix + "endpoint-stop-end"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                # ---- server-stop ----
                rb_name = rb_prefix + "server-stop-begin"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                self.run_bench_cmd(
                    "server", "server", stop_cmd, force=force_server_stop
                )
                abort_rc = 0
                do_abort_arg = self.abort

                rb_name = rb_prefix + "server-stop-end"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(
                    rb_name, timeout, i, sample_data, msgs_file,
                    do_abort=do_abort_arg,
                )

                # ---- infra-stop ----
                rb_name = rb_prefix + "infra-stop-begin"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                rb_name = rb_prefix + "infra-stop-end"
                msgs_file = prepare_user_msgs_file(
                    cs_tx_msgs_dir, iter_samp_dir, rb_name, default_recipients
                )
                self._roadblock_and_evaluate(rb_name, timeout, i, sample_data, msgs_file)

                os.chdir(orig_dir)

                if (not sd["attempt-fail"]
                    and not self.abort
                    and not self.quit):
                    sd["complete"] = True
                    logger.info(
                        "Completed iteration %d sample %d (test %d/%d)"
                        " attempt %d/%d successfully",
                        iter_id, samp_id, i + 1, total_tests,
                        sd["attempt-num"], self.max_sample_failures,
                    )
                else:
                    fail_dir = "%s-fail-%d" % (iter_samp_dir, sd["attempt-num"])
                    if os.path.exists(iter_samp_dir):
                        os.rename(iter_samp_dir, fail_dir)
                    logger.warning(
                        "Completed iteration %d sample %d (test %d/%d)"
                        " attempt %d/%d unsuccessfully",
                        iter_id, samp_id, i + 1, total_tests,
                        sd["attempt-num"], self.max_sample_failures,
                    )
                    self.abort = False

    def _copy_files_to_sample_dir(self, iter_samp_dir):
        for entry in os.listdir("."):
            if not os.path.isfile(entry):
                continue
            if entry.endswith("-stderrout.txt"):
                continue
            if entry.endswith("-files-list"):
                continue
            dest = os.path.join(iter_samp_dir, entry)
            if not os.path.exists(dest):
                shutil.copy2(entry, dest)

    # ---- Init message processing -----------------------------------------

    def process_init_messages(self):
        msgs_log = os.path.join(
            self.roadblock_msgs_dir, "engine-init-end.json"
        )
        if not os.path.isfile(msgs_log):
            logger.info("No engine-init-end messages found")
            return

        msgs_data, _ = load_json_file(msgs_log)
        if not msgs_data or "received" not in msgs_data:
            return

        for msg in msgs_data["received"]:
            env_vars = (
                msg.get("payload", {})
                .get("message", {})
                .get("user-object", {})
                .get("env-vars")
            )
            if env_vars and isinstance(env_vars, dict):
                logger.info("Found env vars: %s", env_vars)
                for key, value in env_vars.items():
                    os.environ[key] = str(value)
                    logger.info("export %s=%s", key, value)
                self.endpoint_label = os.environ.get("endpoint_label", "")

    def save_env(self):
        with open("engine-env.txt", "w") as fp:
            for key, value in sorted(os.environ.items()):
                fp.write("%s=%s\n" % (key, value))

    # ---- Data archival ---------------------------------------------------

    def send_data(self):
        logger.info("Sending data to controller")
        dest_path = os.path.join(
            self.archives_dir, "%s-data.tgz" % self.cs_label
        )
        self.archive_to_controller(self.cs_dir, dest_path)

    # ---- Cleanup ---------------------------------------------------------

    def cleanup(self):
        logger.info("Cleaning up %s", self.cs_dir)
        if self.cs_dir and os.path.isdir(self.cs_dir):
            shutil.rmtree(self.cs_dir, ignore_errors=True)


# ==== CLI entry point for wait-for commands ===============================

def cli_stop_tools(working_dir, tool_cmds_file, disabled, one_tool=""):
    """Standalone entry point for roadblock wait-for: stop tools."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("stop_tools")
    log.info("Running stop_tools()")

    if disabled == "1" or disabled == "True":
        log.info("Tools disabled, skipping stop")
        return

    tool_data_dir = os.path.join(working_dir, "tool-data")
    if not os.path.isdir(tool_data_dir):
        log.warning("tool-data directory not found at %s", tool_data_dir)
        return

    data, err = load_json_file(tool_cmds_file, uselzma=True)
    if data is None:
        log.error("Failed to load %s: %s", tool_cmds_file, err)
        return

    env_file = os.path.join(working_dir, "engine-env.txt")

    for tool in data.get("tools", []):
        tool_name = tool["name"]
        tool_command = tool["command"]

        if one_tool and one_tool != tool_name:
            log.info("Skipping tool '%s' (engine runs '%s' only)", tool_name, one_tool)
            continue

        tool_dir = os.path.join(tool_data_dir, tool_name)
        if not os.path.isdir(tool_dir):
            continue

        log.info("Stopping tool '%s'", tool_name)
        run_command("cd %s && %s" % (tool_dir, tool_command))

        if os.path.isfile(env_file):
            shutil.copy2(env_file, tool_dir)


def cli_send_data(ssh_id_file, src_dir, dest_host, dest_path):
    """Standalone entry point for roadblock wait-for: archive data."""
    import paramiko
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("send_data")
    log.info("Archiving %s to %s:%s", src_dir, dest_host, dest_path)

    for attempt in range(1, 11):
        log.info("Archive attempt %d", attempt)
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=dest_host,
                key_filename=ssh_id_file,
                look_for_keys=False,
                allow_agent=False,
                timeout=30,
            )
            conn = Connection(dest_host)
            conn.client = client
            conn.transport = client.get_transport()
            try:
                with tempfile.NamedTemporaryFile(suffix=".tgz") as tmp:
                    run_command(
                        "tar czf %s -C %s ." % (tmp.name, src_dir),
                        hide=True,
                    )
                    conn.put(tmp.name, dest_path)
            finally:
                conn.close()
            log.info("Archive succeeded")
            return
        except Exception as exc:
            log.warning(
                "Archive attempt %d failed: [%s] %s",
                attempt, type(exc).__name__, exc,
            )
            time.sleep(attempt)
    log.error("Archive failed after 10 attempts")
    sys.exit(1)


def cli_run_bench_cmd(cs_type, matching_type, cmd_type, abort_str, quit_str,
                      force_str, cmd):
    """Standalone entry point for roadblock wait-for: run bench command."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if cs_type != matching_type:
        return
    abort = abort_str not in ("0", "False", "false", "")
    quit_flag = quit_str not in ("0", "False", "false", "")
    force = force_str not in ("0", "False", "false", "")
    if not force and (abort or quit_flag):
        return
    if not cmd:
        return
    result = run_command(cmd)
    sys.exit(result.return_code)


if __name__ == "__main__":
    dispatch = {
        "stop_tools": lambda args: cli_stop_tools(*args),
        "send_data": lambda args: cli_send_data(*args),
        "run_bench_cmd": lambda args: cli_run_bench_cmd(*args),
    }
    if len(sys.argv) < 2:
        print("Usage: engine_lib.py <command> [args...]", file=sys.stderr)
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd in dispatch:
        dispatch[cmd](sys.argv[2:])
    else:
        print("Unknown command: %s" % cmd, file=sys.stderr)
        sys.exit(1)
