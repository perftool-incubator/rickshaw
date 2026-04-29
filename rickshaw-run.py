#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python
#
# rickshaw-run orchestrates benchmark execution: parses args, validates
# endpoints, sources container images, deploys engines via roadblock
# synchronization, and organizes results.

import json
import os
import platform
import random
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import uuid as uuid_module
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

TOOLBOX_HOME = os.environ.get("TOOLBOX_HOME")
if TOOLBOX_HOME:
    sys.path.append(str(Path(TOOLBOX_HOME) / "python"))

RICKSHAW_HOME = os.environ.get("RICKSHAW_HOME")
if RICKSHAW_HOME:
    sys.path.append(RICKSHAW_HOME)

from toolbox.fileio import open_write_text_file
from toolbox.json import load_json_file, save_json_file
from toolbox.jsonsettings import get_json_setting
from toolbox.logging import setup_logging
from toolbox.roadblock import do_roadblock as toolbox_do_roadblock, ROADBLOCK_EXITS
from toolbox.run import run_cmd

logger = None

UTILITIES = ["packrat"]


def generate_uuid():
    return str(uuid_module.uuid1()).upper()


def find_index(arr, field, value):
    for i, item in enumerate(arr):
        if field in item and str(item[field]) == str(value):
            return i
    return -1


def find_files(path):
    skip = {".git", "docs", "__pycache__", ".github"}
    skip_ext = {".md"}
    results = []
    if os.path.isdir(path):
        for entry in sorted(os.listdir(path)):
            if entry in skip or entry.startswith("."):
                continue
            if any(entry.endswith(ext) for ext in skip_ext):
                continue
            entry_path = os.path.join(path, entry)
            if os.path.isdir(entry_path):
                results.extend(find_files(entry_path))
            elif os.path.exists(entry_path):
                results.append(entry_path)
    elif os.path.exists(path):
        results.append(path)
    return results


def file_newer_than(filepath, epoch_sec):
    try:
        return os.path.getmtime(filepath) > epoch_sec
    except OSError:
        return False


def add_endpoint(endpoints, ep_type, opts):
    num = sum(1 for e in endpoints if e["type"] == ep_type) + 1
    endpoints.append({
        "type": ep_type,
        "opts": opts,
        "label": f"{ep_type}-{num}",
    })


def dump_endpoint_types(endpoints):
    return [e["type"] for e in endpoints]


def dump_endpoint_labels(endpoints):
    return [e["label"] for e in endpoints]


def dir_entries(dirpath, pattern=None):
    if not os.path.exists(dirpath):
        raise FileNotFoundError(f"The directory does not exist: {dirpath}")
    entries = os.listdir(dirpath)
    if pattern:
        entries = [e for e in entries if re.search(pattern, e)]
    return entries


def dump_params(params, cs_id, engine, ids_to_benchmark):
    default_role = "client"
    benchmark = ids_to_benchmark.get(str(cs_id)) if cs_id is not None else None
    params_str = ""

    for param in params:
        arg = param.get("arg", "")
        val = param.get("val", "")
        bench = param.get("benchmark", "")
        role = param.get("role", default_role)
        param_id = param.get("id")

        if param_id is not None and cs_id is not None and str(param_id) != str(cs_id):
            continue
        if benchmark is not None and bench != benchmark:
            continue
        if role != engine and role != "all":
            continue

        if val:
            if cs_id is not None:
                val = val.replace("%client-id%", str(cs_id))
            params_str += f" --{arg}={val}"
        else:
            params_str += f" --{arg}"

    return params_str.lstrip()


def perl_s_regex(cmd, regex_str):
    """Apply a Perl-style s/pattern/replacement/[flags] regex to cmd."""
    m = re.match(r'^s(.)(.*?)\1(.*?)\1([gi]*)$', regex_str)
    if not m:
        return cmd
    pattern, replacement, flags_str = m.group(2), m.group(3), m.group(4)
    replacement = re.sub(r'\$(\d+)', r'\\\1', replacement)
    count = 0 if "g" in flags_str else 1
    re_flags = re.IGNORECASE if "i" in flags_str else 0
    return re.sub(pattern, replacement, cmd, count=count, flags=re_flags)


class RunState:
    def __init__(self):
        self.run = {}
        self.defaults = {
            "num-samples": 1,
            "tool-group": "default",
            "test-order": "sample",
            "base-run-dir": tempfile.mkdtemp(),
            "id": generate_uuid(),
            "max-sample-failures": 1,
            "run-file": "",
            "roadblock-password": "flubber",
        }

        self.rickshaw_project_dir = str(Path(__file__).resolve().parent)
        self.bench_schema_file = os.path.join(self.rickshaw_project_dir, "schema", "benchmark.json")
        self.tool_schema_file = os.path.join(self.rickshaw_project_dir, "schema", "tool.json")
        self.run_schema_file = os.path.join(self.rickshaw_project_dir, "schema", "rickshaw-run.json")
        self.utility_schema_file = os.path.join(self.rickshaw_project_dir, "schema", "utility.json")
        self.bench_params_schema_file = os.path.join(self.rickshaw_project_dir, "schema", "bench-params.json")
        self.tool_params_schema_file = os.path.join(self.rickshaw_project_dir, "schema", "tool-params.json")
        self.rickshaw_settings_schema_file = os.path.join(self.rickshaw_project_dir, "schema", "rickshaw-settings.json")
        self.source_images_input_schema_file = os.path.join(self.rickshaw_project_dir, "schema", "source-images-input.json")
        self.source_images_output_schema_file = os.path.join(self.rickshaw_project_dir, "schema", "source-images-output.json")

        self.config_dir = ""
        self.engine_config_dir = ""
        self.engine_bench_cmds_dir = ""
        self.tool_cmds_dir = ""
        self.run_dir = ""
        self.workshop_build_dir = ""
        self.base_endpoint_run_dir = ""
        self.engine_run_dir = ""
        self.engine_logs_dir = ""
        self.engine_archives_dir = ""
        self.engine_run_script = ""
        self.engine_library_script = ""
        self.engine_roadblock_script = ""
        self.engine_roadblock_config = ""
        self.engine_roadblock_module = ""
        self.iterations_dir = ""
        self.roadblock_msgs_dir = ""
        self.roadblock_logs_dir = ""
        self.roadblock_followers_dir = ""

        self.jsonsettings = {}
        self.registries_settings = None
        self.use_workshop = 0
        self.workshop_script = "workshop.py"
        self.workshop_force_builds = ""
        self.bench_configs = {}
        self.bench_dirs = {}
        self.benchmark_to_ids = {}
        self.ids_to_benchmark = {}
        self.endpoints = []
        self.image_ids = {}
        self.tools_configs = {}
        self.tools_params = []
        self.utility_configs = {}
        self.clients_servers = {}
        self.rb_cs_ids = []
        self.active_followers = []
        self.active_collector_types = {}
        self.tests = []
        self.cs_conf_file = ""

        self.rb_connection_watchdog = ""
        self.rb_log_level = ""
        self.default_rb_timeout = 0
        self.collect_sysinfo_timeout = 0
        self.endpoint_deploy_timeout = 0
        self.engine_script_start_timeout = 0
        self.base_rb_leader_cmd = ""
        self.endpoint_log_level = ""
        self.endpoint_roadblock_opt = ""
        self.workshop_roadblock_opt = ""
        self.default_tool_userenv = ""

        self.messages_ref = None
        self.abort_via_roadblock = False
        self.abort_test_id = None
        self.endpoint_processes = []

        self.arch = platform.machine()
        self.available_cpus = os.cpu_count() or 1

        signal.signal(signal.SIGINT, self._sigint_handler)

    def _sigint_handler(self, signum, frame):
        logger.info("Caught a CTRL-C/SIGINT, aborting via roadblock!")
        self.abort_via_roadblock = True

    def _get_setting(self, query, settings):
        """Wrapper for get_json_setting with Perl-compatible return convention."""
        value, rc = get_json_setting(settings, query)
        if isinstance(value, bool):
            value = "true" if value else "false"
        return rc, value

    # ----------------------------------------------------------------
    # Phase 1: CLI and URL parsing
    # ----------------------------------------------------------------

    def process_cmdline(self):
        args = sys.argv[1:]
        while args:
            p = args.pop(0)
            if not p.startswith("--"):
                logger.error("[ERROR] malformed cmdline parameter: %s", p)
                self.usage()
                sys.exit(1)

            p = p[2:]
            if "=" in p:
                arg, val = p.split("=", 1)
            else:
                arg = p
                if not args:
                    logger.error("[ERROR] missing value for --%s", arg)
                    sys.exit(1)
                val = args.pop(0)

            logger.debug("processing argv, arg is: [%s], val is: [%s]", arg, val)

            if arg == "from-file":
                self.run["run-file"] = val
                self._process_from_file(args)
            elif arg == "endpoint":
                m = re.match(r'^(\w+),(.*)$', val)
                if m:
                    add_endpoint(self.endpoints, m.group(1), m.group(2))
                else:
                    logger.error("[ERROR] malformed endpoint: %s", val)
                    sys.exit(1)
            elif arg == "debug":
                pass
            elif arg == "help":
                self.usage()
                sys.exit(0)
            elif re.match(
                r'^(base-run-dir|workshop-dir|workshop-script|packrat-dir|bench-dir|'
                r'roadblock-dir|roadblock-password|tools-dir|engine-dir|'
                r'run-id|id|bench-params|tool-params|'
                r'test-order|tool-group|num-samples|max-sample-failures|name|bench-ids|'
                r'registries-json|external-userenvs-dir|source-images-service-url|'
                r'email|desc)$', arg
            ):
                logger.debug("argument: [%s]", arg)
                self.run[arg] = val
            elif arg == "tags":
                logger.debug("argument: [%s]", arg)
                if "tags" not in self.run:
                    self.run["tags"] = []
                for this_tag in val.split(","):
                    m = re.match(r'(\S+):(\S+)', this_tag)
                    if m:
                        self.run["tags"].append({"name": m.group(1), "val": m.group(2)})
                    else:
                        logger.error("ERROR: format for tag is not valid: %s", this_tag)
                        sys.exit(1)
            else:
                logger.error("[ERROR] argument not valid: [%s]", arg)
                self.usage()
                sys.exit(1)

        for p, default in self.defaults.items():
            if p not in self.run:
                logger.debug("applying default value [%s] for %s", default, p)
                self.run[p] = default

        self.base_rb_leader_cmd = (
            f"--role=leader --redis-server=localhost "
            f"--redis-password={self.run['roadblock-password']}"
        )

    def _process_from_file(self, args):
        run_file = self.run["run-file"]

        bb_cmd = f"python3 {self.rickshaw_project_dir}/util/blockbreaker.py --json {run_file} --config benchmarks"
        logger.debug("about to run: %s", bb_cmd)
        _, output, rc = run_cmd(bb_cmd)
        if rc != 0:
            logger.error("[ERROR] blockbreaker failed with rc=%d for command=[%s]:\n%s", rc, bb_cmd, output)
            sys.exit(1)
        output = output.strip()
        logger.debug("appending arg [--bench-ids] with value [%s] extracted from from-file to argv", output)
        args.extend(["--bench-ids", output])

        benchmarks = []
        for bench_id in output.split(","):
            parts = bench_id.split(":")
            benchmarks.append(parts[0])

        bench_params = ""
        for benchmark in benchmarks:
            benchmark_dir = os.path.join(os.environ.get("CRUCIBLE_HOME", ""), "subprojects", "benchmarks", benchmark)
            if not os.path.isdir(benchmark_dir):
                logger.error("[ERROR] invalid benchmark %s, benchmark directory %s does not exist", benchmark, benchmark_dir)
                sys.exit(1)

            bb_cmd = f"python3 {self.rickshaw_project_dir}/util/blockbreaker.py --json {run_file} --config mv-params --benchmark {benchmark}"
            logger.debug("about to run: %s", bb_cmd)
            _, output, rc = run_cmd(bb_cmd)
            if rc != 0:
                logger.error("[ERROR] blockbreaker failed with rc=%d for command=[%s]:\n%s", rc, bb_cmd, output)
                sys.exit(1)

            bench_mv_params = os.path.join(self.run.get("base-run-dir", self.defaults["base-run-dir"]), "config", f"{benchmark}-mv-params.json")
            os.makedirs(os.path.dirname(bench_mv_params), exist_ok=True)
            with open(bench_mv_params, "w") as f:
                f.write(output)

            bench_params_run_file = os.path.join(self.run.get("base-run-dir", self.defaults["base-run-dir"]), "config", f"{benchmark}-bench-params.json")
            bench_params_run_output = os.path.join(self.run.get("base-run-dir", self.defaults["base-run-dir"]), "config", f"{benchmark}-bench-params.txt")
            multiplex_cmd = f"{os.environ.get('MULTIPLEX_HOME', '')}/multiplex.py --input {bench_mv_params} --output {bench_params_run_file}"
            requirements_file = os.path.join(benchmark_dir, "multiplex.json")
            if os.path.exists(requirements_file):
                multiplex_cmd += f" --requirements {requirements_file}"

            logger.debug("about to run: %s", multiplex_cmd)
            _, output, rc = run_cmd(multiplex_cmd)
            with open(bench_params_run_output, "w") as f:
                f.write(output)
            if rc != 0:
                logger.error("[ERROR] multiplex failed with an error and returned rc=%d", rc)
                logger.error("multiplex output is:\n%s", output)
                sys.exit(1)
            bench_params += f",{bench_params_run_file}"

        bench_params = bench_params.lstrip(",")
        logger.debug("appending arg [--bench-params] with value [%s] extracted from from-file to argv", bench_params)
        args.extend(["--bench-params", bench_params])

        bb_cmd = f"python3 {self.rickshaw_project_dir}/util/blockbreaker.py --json {run_file} --config tool-params"
        logger.debug("about to run: %s", bb_cmd)
        _, output, rc = run_cmd(bb_cmd)
        if rc != 0:
            logger.error("[ERROR] blockbreaker failed with rc=%d for command=[%s]:\n%s", rc, bb_cmd, output)
            sys.exit(1)
        tool_params = os.path.join(self.run.get("base-run-dir", self.defaults["base-run-dir"]), "config", "tool-params.json")
        with open(tool_params, "w") as f:
            f.write(output)
        logger.debug("appending arg [--tool-params] with value [%s] extracted from from-file to argv", tool_params)
        args.extend(["--tool-params", tool_params])

        bb_cmd = f"python3 {self.rickshaw_project_dir}/util/blockbreaker.py --json {run_file} --config tags"
        logger.debug("about to run: %s", bb_cmd)
        _, output, rc = run_cmd(bb_cmd)
        if rc != 0:
            logger.error("[ERROR] blockbreaker failed with rc=%d for command=[%s]:\n%s", rc, bb_cmd, output)
            sys.exit(1)
        output = output.strip()
        logger.debug("appending arg [--tags] with value [%s] extracted from from-file to argv", output)
        args.extend(["--tags", output])

        bb_cmd = f"python3 {self.rickshaw_project_dir}/util/blockbreaker.py --json {run_file} --config endpoints"
        logger.debug("about to run: %s", bb_cmd)
        _, output, rc = run_cmd(bb_cmd)
        if rc != 0:
            logger.error("[ERROR] blockbreaker failed with rc=%d for command=[%s]:\n%s", rc, bb_cmd, output)
            sys.exit(1)
        output = output.strip()
        for endpoint in output.split(" "):
            logger.debug("appending arg [--endpoint] with value [%s] extracted from from-file to argv", endpoint)
            args.extend(["--endpoint", endpoint])

        run_file_json, err = load_json_file(self.run["run-file"])
        if run_file_json is None:
            logger.error("[ERROR] Could not load run-file: %s", self.run["run-file"])
            sys.exit(1)
        if "run-params" in run_file_json:
            for key, val in run_file_json["run-params"].items():
                logger.debug("appending arg [--%s] with value [%s] extracted from run-file run-params", key, val)
                args.extend([f"--{key}", str(val)])

    def usage(self):
        logger.info("usage:")
        logger.info("--registries-json        Path to a JSON file containing container registry information")
        logger.info("--external-userenvs-dir  Path to look for userenv definitions in that are provided by external sources")
        logger.info("--engine-dir             Directory where the engine project exists")
        logger.info("--workshop-dir           Directory where workshop project exists")
        logger.info("--workshop-script        Workshop script filename (default: workshop.py)")
        logger.info("--packrat-dir            Directory where the packrat project exists")
        logger.info("--roadblock-dir          Directory where workshop project exists")
        logger.info("--roadblock-password     Password to use to access the valkey instance")
        logger.info("--bench-dir              Directory where benchmark helper project exists")
        logger.info("--bench-params           File with benchmark parameters to use")
        logger.info("--tools-dir              Directory where *all* tool subprojects exist")
        logger.info("--tool-params            File with tool parameters to use")
        logger.info("--num-samples            The number of sample executions to run for each benchmark iteration")
        logger.info("--max-sample-failures    The total number of benchmark sample executions that are tolerated")
        logger.info("--test-order             's' = run all samples of an iteration first")
        logger.info("                         'i' = run all iterations of a sample first")
        logger.info("                         'r' = run a sample from a random iteration one at a time")

    def decode_repo_urls(self, registry_type):
        reg = self.run.setdefault("registries", {}).setdefault(registry_type, {})
        if "url-details" in reg:
            logger.info("%s registry repository URL (%s) details have already been decoded",
                        registry_type, reg["repo"])
            return

        logger.info("Decoding %s registry repository URL (%s) details", registry_type, reg["repo"])
        details = {}
        m = re.match(r'^(\w+:/){0,1}([^/]+/){0,1}([^/]+/){0,1}([^/]+)$', reg["repo"])
        if not m:
            logger.error("The registry repo does not match the expected pattern: %s", reg["repo"])
            sys.exit(1)

        if m.group(1):
            details["protocol"] = m.group(1)
            logger.debug("protocol:      [%s]", details["protocol"])
        else:
            details["protocol"] = ""

        if m.group(2):
            host = m.group(2).rstrip("/")
            port_m = re.match(r'(\w+)(:\d+)', host)
            if port_m:
                details["host"] = port_m.group(1)
                details["host-port"] = port_m.group(2)
            else:
                details["host"] = host
                details["host-port"] = ""
        else:
            details["host"] = ""

        if m.group(3):
            details["project"] = m.group(3).rstrip("/")
        else:
            details["project"] = ""

        if not details["host"] and details.get("project"):
            details["host"] = details["project"]
        elif details["host"] and not details.get("project"):
            details["project"] = details["host"]
        elif not details["host"] and not details.get("project"):
            logger.error("At least one of the host or the project must be present in %s", reg["repo"])
            sys.exit(1)

        details["source-image-url"] = f"{details['host']}/{details['project']}"
        logger.debug("source-image-url: [%s]", details["source-image-url"])

        if details.get("host-port"):
            details["dest-image-url"] = f"{details['host']}{details['host-port']}/{details['project']}"
        else:
            details["dest-image-url"] = f"{details['host']}/{details['project']}"

        if details.get("protocol"):
            details["dest-image-url"] = details["protocol"] + details["dest-image-url"]

        if m.group(4):
            details["label"] = m.group(4)
            details["source-image-url"] += f"/{details['label']}"
            details["dest-image-url"] += f"/{details['label']}"
        else:
            logger.info("The label/repo was not defined in registry repo: %s", reg["repo"])

        reg["url-details"] = details

    # ----------------------------------------------------------------
    # Phase 2: Config loading
    # ----------------------------------------------------------------

    def load_settings_info(self):
        rickshaw_settings_filename = os.path.join(self.rickshaw_project_dir, "rickshaw-settings.json")
        self.jsonsettings, err = load_json_file(rickshaw_settings_filename)
        if self.jsonsettings is None:
            logger.error("load_settings_info(): failed to load %s: %s", rickshaw_settings_filename, err)
            sys.exit(1)

        registries_migration_needed = False
        if "registries-json" in self.run:
            reg_data, err = load_json_file(self.run["registries-json"])
            if reg_data is None:
                logger.error("load_settings_info(): failed to load %s: %s", self.run["registries-json"], err)
                sys.exit(1)
            self.registries_settings = reg_data

            _, rc = get_json_setting(self.registries_settings, "engines.public.quay.expiration-length")
            if rc == 1:
                logger.info("load_settings_info(): Enabling registries settings migration")
                registries_migration_needed = True

        def _load_required(query, settings, name):
            rc, val = self._get_setting(query, settings)
            if rc != 0:
                logger.error("load_settings_info(): failed to load %s", name)
                sys.exit(1)
            logger.info("load_settings_info(): loaded %s: %s", name, val)
            return val

        def _load_optional(query, settings, default=None):
            rc, val = self._get_setting(query, settings)
            if rc == 0 and val is not None:
                return val
            return default

        self.endpoint_log_level = _load_required("endpoints.log-level", self.jsonsettings, "endpoint log-level config")

        rb_cw = _load_required("roadblock.connection-watchdog", self.jsonsettings, "roadblock connection-watchdog config")
        self.rb_connection_watchdog = "enabled" if rb_cw == "true" else "disabled"

        self.rb_log_level = _load_required("roadblock.log-level", self.jsonsettings, "roadblock log-level config")
        self.default_rb_timeout = int(_load_required("roadblock.timeouts.default", self.jsonsettings, "default roadblock timeout"))
        self.endpoint_deploy_timeout = int(_load_required("roadblock.timeouts.endpoint-deploy", self.jsonsettings, "endpoint-deploy roadblock timeout"))
        self.collect_sysinfo_timeout = int(_load_required("roadblock.timeouts.collect-sysinfo", self.jsonsettings, "collect-sysinfo roadblock timeout"))
        self.engine_script_start_timeout = int(_load_required("roadblock.timeouts.engine-start", self.jsonsettings, "engine script-start roadblock timeout"))
        self.workshop_force_builds = _load_required("workshop.force-builds", self.jsonsettings, "workshop force builds")

        if registries_migration_needed:
            val = _load_optional("quay.refresh-expiration.token-file", self.jsonsettings)
            if val is not None:
                logger.info("load_settings_info(): migrating quay.refresh-expiration.token-file")
                self.registries_settings.setdefault("engines", {}).setdefault("public", {}).setdefault("quay", {}).setdefault("refresh-expiration", {})["token-file"] = val

            val = _load_optional("quay.refresh-expiration.api-url", self.jsonsettings)
            if val is not None:
                logger.info("load_settings_info(): migrating quay.refresh-expiration.api-url")
                self.registries_settings.setdefault("engines", {}).setdefault("public", {}).setdefault("quay", {}).setdefault("refresh-expiration", {})["api-url"] = val

        self.default_tool_userenv = _load_required("userenvs.default.tools", self.jsonsettings, "default tool userenv")

        if registries_migration_needed:
            val = _load_optional("quay.image-expiration", self.jsonsettings)
            if val is not None:
                logger.info("load_settings_info(): migrating quay.image-expiration")
                self.registries_settings.setdefault("engines", {}).setdefault("public", {}).setdefault("quay", {})["expiration-length"] = val

            logger.info("load_settings_info(): modifying %s", self.run["registries-json"])
            with open(self.run["registries-json"], "w") as f:
                json.dump(self.registries_settings, f, indent=4, sort_keys=True)
                f.write("\n")

            logger.info("load_settings_info(): resetting quay values in rickshaw-settings.json")
            rickshaw_settings_filename = os.path.join(self.rickshaw_project_dir, "rickshaw-settings.json")
            jq_cmd = (
                f"jq --indent 4 --argjson token_file null --argjson api_url null --arg expiration_length '2w'"
                f" '.quay.\"refresh-expiration\".\"token-file\" = $token_file | "
                f".quay.\"refresh-expiration\".\"api-url\" = $api_url | "
                f".quay.\"image-expiration\" = $expiration_length'"
                f" {rickshaw_settings_filename}"
            )
            _, output, rc = run_cmd(jq_cmd)
            if rc != 0:
                logger.error("load_settings_info(): failed to generate updated rickshaw settings json:\n%s", output)
                sys.exit(1)
            with open(rickshaw_settings_filename, "w") as f:
                f.write(output)

        self.run["registries"] = {}
        self.run["registries"]["public"] = {}

        self.run["registries"]["public"]["repo"] = _load_required("engines.public.url", self.registries_settings, "public engines repo url")
        self.decode_repo_urls("public")

        rc, val = self._get_setting("engines.private.url", self.registries_settings)
        if rc != 0:
            logger.info("load_settings_info(): no private engine registry defined")
        else:
            self.run["registries"]["private"] = {}
            self.run["registries"]["private"]["repo"] = val
            logger.info("load_settings_info(): loaded private registry repository: %s", val)
            self.decode_repo_urls("private")

        self.run["registries"]["public"]["push-token"] = _load_required("engines.public.push-token", self.registries_settings, "public engines push token")

        if "private" in self.run["registries"]:
            self.run["registries"]["private"]["push-token"] = _load_required("engines.private.tokens.push", self.registries_settings, "private engines push token")
            self.run["registries"]["private"]["pull-token"] = _load_required("engines.private.tokens.pull", self.registries_settings, "private engines pull token")

        self.run["registries"]["public"]["tls-verify"] = _load_optional("engines.public.tls-verify", self.registries_settings, "true")
        if "private" in self.run["registries"]:
            self.run["registries"]["private"]["tls-verify"] = _load_optional("engines.private.tls-verify", self.registries_settings, "true")

        default_quay_expiration = "2w"
        self.run["registries"]["public"]["quay-expiration-length"] = _load_optional(
            "engines.public.quay.expiration-length", self.registries_settings, default_quay_expiration)

        if "private" in self.run["registries"]:
            self.run["registries"]["private"]["quay-expiration-length"] = _load_optional(
                "engines.private.quay.expiration-length", self.registries_settings, default_quay_expiration)

        val = _load_optional("engines.public.quay.refresh-expiration.token-file", self.registries_settings)
        if val is not None:
            self.run["registries"]["public"]["quay-refresh-expiration-token-file"] = val

        if "private" in self.run["registries"]:
            val = _load_optional("engines.private.quay.refresh-expiration.token-file", self.registries_settings)
            if val is not None:
                self.run["registries"]["private"]["quay-refresh-expiration-token-file"] = val

        val = _load_optional("engines.public.quay.refresh-expiration.api-url", self.registries_settings)
        if val is not None:
            self.run["registries"]["public"]["quay-refresh-expiration-api-url"] = val

        self.run["registries"]["public"]["quay-refresh-expiration-require-success"] = _load_optional(
            "engines.public.quay.refresh-expiration.require-success", self.registries_settings, "true")

        if "private" in self.run["registries"]:
            val = _load_optional("engines.private.quay.refresh-expiration.api-url", self.registries_settings)
            if val is not None:
                self.run["registries"]["private"]["quay-refresh-expiration-api-url"] = val

            self.run["registries"]["private"]["quay-refresh-expiration-require-success"] = _load_optional(
                "engines.private.quay.refresh-expiration.require-success", self.registries_settings, "true")

        logger.info("Finished loading json settings")

    def load_bench_params(self):
        self.run["iterations"] = []
        if "bench-dir" not in self.run:
            logger.error("[ERROR] You must use --bench-dir=/path/to-first/benchmark-subproject[,...]")
            sys.exit(1)
        if "bench-params" not in self.run:
            logger.error("[ERROR] You must use --bench-params=/path/to-first/benchmark-params.json[,...]")
            sys.exit(1)

        params_files = self.run["bench-params"].split(",")
        count = 0
        self.run["benchmark"] = ""

        for this_bench_dir in self.run["bench-dir"].split(","):
            bench_config_file = os.path.join(this_bench_dir, "rickshaw.json")
            if not os.path.exists(bench_config_file):
                logger.error("[ERROR] benchmark subproject config file %s was not found", bench_config_file)
                sys.exit(1)

            bench_config_ref, err = load_json_file(bench_config_file)
            if bench_config_ref is None:
                logger.error("Could not open the bench config file: %s", err)
                sys.exit(1)

            benchmark_name = bench_config_ref.get("benchmark")
            if not benchmark_name:
                logger.error("[ERROR] benchmark was not defined in %s", bench_config_file)
                sys.exit(1)

            self.bench_dirs[benchmark_name] = this_bench_dir
            logger.info("Preparing to run %s", benchmark_name)
            self.run["benchmark"] += f",{benchmark_name}"
            self.bench_configs[benchmark_name] = bench_config_ref

            param_sets, err = load_json_file(params_files[count])
            if param_sets is None:
                logger.error("Could not open the bench params file: %s", params_files[count])
                sys.exit(1)

            for iter_id, params in enumerate(param_sets):
                while len(self.run["iterations"]) <= iter_id:
                    self.run["iterations"].append({})
                if "params" not in self.run["iterations"][iter_id]:
                    self.run["iterations"][iter_id]["params"] = []
                for param in params:
                    param["benchmark"] = benchmark_name
                    self.run["iterations"][iter_id]["params"].append(param)

            count += 1

        self.run["benchmark"] = self.run["benchmark"].lstrip(",")
        logger.info("There are %d benchmark types to run", len(self.bench_configs))

    def validate_controller_env(self):
        rb_bin = "roadblocker.py"
        if "roadblock-dir" in self.run and os.path.exists(os.path.join(self.run["roadblock-dir"], rb_bin)):
            self.base_rb_leader_cmd = (
                f"{self.run['roadblock-dir']}/{rb_bin} {self.base_rb_leader_cmd}"
            )
            os.environ["ROADBLOCK_HOME"] = self.run["roadblock-dir"]
            sys.path.append(self.run["roadblock-dir"])
            import toolbox.roadblock as _rb_mod
            try:
                from roadblock import roadblock as RoadblockEngine
                _rb_mod.RoadblockEngine = RoadblockEngine
            except ImportError:
                logger.error("ERROR: could not import roadblock module from %s", self.run["roadblock-dir"])
                sys.exit(1)
        else:
            logger.error("ERROR, roadblock project directory not defined or roadblocker.py not found")
            sys.exit(1)

        self.workshop_script = self.run.get("workshop-script") or os.environ.get("WORKSHOP_SCRIPT") or "workshop.py"
        if "workshop-dir" in self.run and os.path.exists(os.path.join(self.run["workshop-dir"], self.workshop_script)):
            self.use_workshop = 1
            pub_reg = self.run.get("registries", {}).get("public", {})
            if "repo" not in pub_reg:
                logger.error("You must define a container repository to use rickshaw with workshop")
                sys.exit(1)
            if "push-token" not in pub_reg:
                logger.error("You must define a path to a public registry token file to use rickshaw with workshop")
                sys.exit(1)

        if "tools-dir" not in self.run:
            logger.error("[ERROR] You must use --tools-dir=/path/to/tools/base/subproject-dir")
            sys.exit(1)

    def assign_bench_ids(self):
        for benchmark_and_id in self.run.get("bench-ids", "").split(","):
            parts = benchmark_and_id.split(":", 1)
            if len(parts) != 2:
                continue
            bench, ids_str = parts
            id_ranges = []
            for segment in ids_str.split(","):
                for sub in segment.split("+"):
                    id_ranges.append(sub)
            for id_range in id_ranges:
                m = re.match(r'^(\d+)-(\d+)$', id_range)
                if m:
                    for i in range(int(m.group(1)), int(m.group(2)) + 1):
                        self.ids_to_benchmark[str(i)] = bench
                        self.benchmark_to_ids.setdefault(bench, []).append(str(i))
                elif re.match(r'^\d+$', id_range):
                    self.ids_to_benchmark[id_range] = bench
                    self.benchmark_to_ids.setdefault(bench, []).append(id_range)
                else:
                    logger.warning("ID range or number not recognized: %s", id_range)

    def make_run_dirs(self):
        for dirtype in ("base-run-dir", "tools-dir"):
            self.run[dirtype] = str(Path(self.run[dirtype]).resolve())

        os.makedirs(self.run["base-run-dir"], exist_ok=True)
        logger.debug("Base run directory: [%s]", self.run["base-run-dir"])

        self.config_dir = os.path.join(self.run["base-run-dir"], "config")
        os.makedirs(self.config_dir, exist_ok=True)
        self.engine_config_dir = os.path.join(self.config_dir, "engine")
        os.makedirs(self.engine_config_dir, exist_ok=True)
        self.engine_bench_cmds_dir = os.path.join(self.engine_config_dir, "bench-cmds")
        os.makedirs(self.engine_bench_cmds_dir, exist_ok=True)
        self.tool_cmds_dir = os.path.join(self.config_dir, "tool-cmds")
        os.makedirs(self.tool_cmds_dir, exist_ok=True)
        self.run_dir = os.path.join(self.run["base-run-dir"], "run")
        os.makedirs(self.run_dir, exist_ok=True)
        self.workshop_build_dir = os.path.join(self.run_dir, "workshop")
        os.makedirs(self.workshop_build_dir, exist_ok=True)
        self.base_endpoint_run_dir = os.path.join(self.run_dir, "endpoint")
        os.makedirs(self.base_endpoint_run_dir, exist_ok=True)
        self.engine_run_dir = os.path.join(self.run_dir, "engine")
        os.makedirs(self.engine_run_dir, exist_ok=True)
        self.engine_logs_dir = os.path.join(self.engine_run_dir, "logs")
        os.makedirs(self.engine_logs_dir, exist_ok=True)
        self.engine_archives_dir = os.path.join(self.engine_run_dir, "archives")
        os.makedirs(self.engine_archives_dir, exist_ok=True)

        self.engine_run_script = os.path.join(self.engine_config_dir, "engine-script")
        self.engine_library_script = os.path.join(self.engine_config_dir, "engine-script-library")
        self.engine_roadblock_script = os.path.join(self.engine_config_dir, "roadblocker.py")
        self.engine_roadblock_config = os.path.join(self.engine_config_dir, "roadblocker_config.py")
        self.engine_roadblock_module = os.path.join(self.engine_config_dir, "roadblock.py")

        self.iterations_dir = os.path.join(self.run_dir, "iterations")
        os.makedirs(self.iterations_dir, exist_ok=True)
        self.roadblock_msgs_dir = os.path.join(self.run_dir, "roadblock-msgs")
        os.makedirs(self.roadblock_msgs_dir, exist_ok=True)
        self.roadblock_logs_dir = os.path.join(self.run_dir, "roadblock-logs")
        os.makedirs(self.roadblock_logs_dir, exist_ok=True)
        self.roadblock_followers_dir = os.path.join(self.run_dir, "roadblock-followers")
        os.makedirs(self.roadblock_followers_dir, exist_ok=True)

        if not self.endpoints:
            logger.error("ERROR: you must declare endpoints")
            sys.exit(1)

    def save_config_info(self):
        save_json_file(os.path.join(self.config_dir, "rickshaw-run.json"), self.run)
        save_json_file(os.path.join(self.config_dir, "rickshaw-settings.json"), self.jsonsettings)
        logger.info("Finished saving json settings to %s", os.path.join(self.config_dir, "rickshaw-run.json"))

    # ----------------------------------------------------------------
    # Phase 3: Endpoint validation and preparation
    # ----------------------------------------------------------------

    def validate_endpoints(self):
        logger.info("Confirming the endpoints will satisfy the benchmark requirements:")
        deprecated_endpoints = {}
        experimental_endpoints = {}
        jobs = []

        for endpoint in self.endpoints:
            dep_file = os.path.join(self.rickshaw_project_dir, "endpoints", endpoint["type"], "deprecated")
            if os.path.exists(dep_file):
                if endpoint["type"] not in deprecated_endpoints:
                    deprecated_endpoints[endpoint["type"]] = dep_file

            exp_file = os.path.join(self.rickshaw_project_dir, "endpoints", endpoint["type"], "experimental")
            if os.path.exists(exp_file):
                if endpoint["type"] not in experimental_endpoints:
                    experimental_endpoints[endpoint["type"]] = exp_file

            endpoint_project_dir = os.path.join(self.rickshaw_project_dir, "endpoints", endpoint["type"])
            cmd = (
                f"{endpoint_project_dir}/{endpoint['type']}"
                f" --endpoint-label={endpoint['label']}"
                f" --base-run-dir={self.run['base-run-dir']}"
                f" --rickshaw-dir={self.rickshaw_project_dir}"
                f" --validate"
            )

            if endpoint["type"] in ("remotehosts", "kube"):
                os.environ["ROADBLOCK_HOME"] = self.run.get("roadblock-dir", "")
                for opt_part in endpoint["opts"].split(","):
                    cmd += f" --{opt_part}"
                cmd += f" --crucible-dir={os.environ.get('CRUCIBLE_HOME', '')}"
                cmd += f" --log-level={self.endpoint_log_level}"
            else:
                cmd += f" --endpoint-opts={endpoint['opts']}"

            jobs.append({"endpoint": endpoint["label"], "command": cmd})

        endpoint_outputs = {}
        job_errors = 0
        num_workers = min(self.available_cpus, len(jobs)) if jobs else 1

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_ep = {}
            for job in jobs:
                future = executor.submit(run_cmd, job["command"])
                future_to_ep[future] = job["endpoint"]

            for future in as_completed(future_to_ep):
                ep = future_to_ep[future]
                _, output, rc = future.result()
                endpoint_outputs[ep] = output
                if rc != 0:
                    logger.error("[ERROR] Endpoint %s validation returned non-zero exit code %d\n%s", ep, rc, output)
                    job_errors += 1
                else:
                    logger.debug("Endpoint %s validated", ep)

        if job_errors > 0:
            logger.error("[ERROR] %d endpoint validation command(s) failed!", job_errors)
            sys.exit(1)

        for dep_type, dep_file in deprecated_endpoints.items():
            with open(dep_file) as f:
                logger.warning("WARNING: the '%s' endpoint is deprecated:\n%s", dep_type, f.read())

        for exp_type, exp_file in experimental_endpoints.items():
            with open(exp_file) as f:
                logger.warning("WARNING: the '%s' endpoint is experimental:\n%s", exp_type, f.read())

        for endpoint in self.endpoints:
            output = endpoint_outputs.get(endpoint["label"], "")
            validation_dir = os.path.join(self.run["base-run-dir"], "validations")
            os.makedirs(validation_dir, exist_ok=True)
            with open(os.path.join(validation_dir, f"{endpoint['label']}.txt"), "w") as f:
                f.write(output)

            endpoint["userenvs"] = []

            for line in output.split("\n"):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                parts = line.split()
                keyword = parts[0] if parts else ""

                if keyword in ("client", "server"):
                    for id_str in parts[1:]:
                        m = re.match(r'^(\d+)-(\d+)$', id_str)
                        if m:
                            for i in range(int(m.group(1)), int(m.group(2)) + 1):
                                self.clients_servers.setdefault(keyword, {})[str(i)] = {
                                    "endpoint-type": endpoint["type"],
                                    "id": str(i),
                                }
                                rb_id = f"{keyword}-{i}"
                                if rb_id not in self.rb_cs_ids:
                                    self.rb_cs_ids.append(rb_id)
                        elif re.match(r'^\d+$', id_str):
                            self.clients_servers.setdefault(keyword, {})[id_str] = {
                                "endpoint-type": endpoint["type"],
                                "id": id_str,
                            }
                            rb_id = f"{keyword}-{id_str}"
                            if rb_id not in self.rb_cs_ids:
                                self.rb_cs_ids.append(rb_id)

                elif keyword == "profiler":
                    for id_str in parts[1:]:
                        rb_id = f"profiler-{id_str}"
                        if rb_id not in self.rb_cs_ids:
                            self.rb_cs_ids.append(rb_id)

                elif keyword == "engine-types":
                    for etype in parts[1:]:
                        self.active_collector_types[etype] = 1

                elif keyword == "userenv":
                    userenv = parts[1] if len(parts) > 1 else ""
                    endpoint["userenvs"].append(userenv)
                    logger.debug("clients/servers for endpoint %s will have userenv %s",
                                 endpoint["label"], userenv)
                    for bench_name in self.bench_configs:
                        self.image_ids.setdefault(bench_name, {})[userenv] = {"image": ""}

        if len(self.bench_configs) == 1:
            bench_name = list(self.bench_configs.keys())[0]
            all_ids = sorted(self.clients_servers.get("client", {}).keys(), key=int)
            if self.clients_servers.get("server"):
                all_ids.extend(sorted(self.clients_servers["server"].keys(), key=int))
            all_ids = sorted(set(all_ids), key=int)
            if all_ids:
                id_ranges = []
                start = int(all_ids[0])
                end = start
                for id_str in all_ids[1:]:
                    i = int(id_str)
                    if i == end + 1:
                        end = i
                    else:
                        id_ranges.append(f"{start}-{end}" if start != end else str(start))
                        start = end = i
                id_ranges.append(f"{start}-{end}" if start != end else str(start))
                self.run["bench-ids"] = f"{bench_name}:{'+'.join(id_ranges)}"

        self.assign_bench_ids()

        min_id = None
        max_id = None
        for cs_type in ("client", "server"):
            for id_str in self.clients_servers.get(cs_type, {}):
                i = int(id_str)
                if min_id is None or i < min_id:
                    min_id = i
                if max_id is None or i > max_id:
                    max_id = i

        if min_id is not None and min_id != 1:
            logger.error("[ERROR] The minimum client/server ID must be 1, got %d", min_id)
            sys.exit(1)

        self.endpoint_roadblock_opt = (
            f" --roadblock-id={self.run['id']}"
            f" --roadblock-passwd={self.run['roadblock-password']}"
        )
        self.workshop_roadblock_opt = f" --requirements {self.run.get('roadblock-dir', '')}/workshop.json "
        self.run["endpoints"] = self.endpoints
        logger.info("Endpoints validated")

    def load_tool_params(self):
        if "tool-params" not in self.run:
            self.run["tool-params"] = os.path.join(self.rickshaw_project_dir, "config", "tool-params.json")

        json_ref, err = load_json_file(self.run["tool-params"])
        if json_ref is None:
            logger.error("Could not open the tool params file: %s", err)
            sys.exit(1)

        tool_name_count = {}
        for tool_entry in json_ref:
            if tool_entry.get("enabled") == "no":
                continue
            tool_name_count[tool_entry["tool"]] = tool_name_count.get(tool_entry["tool"], 0) + 1

        seen_tool_ids = {}
        for tool_entry in json_ref:
            if tool_entry.get("enabled") == "no":
                continue

            tool_name = tool_entry["tool"]
            is_multi = tool_name_count[tool_name] > 1

            if is_multi and "id" not in tool_entry:
                logger.error("[ERROR] Tool '%s' specified multiple times without unique 'id' fields", tool_name)
                sys.exit(1)

            if not is_multi and "id" in tool_entry:
                logger.error("[ERROR] Tool '%s' has an 'id' field but is only specified once", tool_name)
                sys.exit(1)

            if "id" in tool_entry:
                tid = tool_entry["id"]
                expected_prefix = f"{tool_name}-"
                if not tid.startswith(expected_prefix):
                    logger.error("[ERROR] Tool id '%s' must start with '%s'", tid, expected_prefix)
                    sys.exit(1)
                if len(tid) <= len(expected_prefix):
                    logger.error("[ERROR] Tool id '%s' must have a suffix after '%s'", tid, expected_prefix)
                    sys.exit(1)

            tool_id = tool_entry.get("id", tool_name)
            tool_entry["tool-id"] = tool_id

            if tool_id in seen_tool_ids:
                logger.error("[ERROR] Duplicate tool id '%s'", tool_id)
                sys.exit(1)
            seen_tool_ids[tool_id] = 1

            self.tools_params.append(tool_entry)

        tool_id_map = {}
        for tool_entry in self.tools_params:
            tool_name = tool_entry["tool"]
            tool_id = tool_entry["tool-id"]
            if "userenv" not in tool_entry:
                tool_entry["userenv"] = self.default_tool_userenv

            tool_id_map[tool_id] = tool_name

            if tool_name not in self.tools_configs:
                this_tool_dir = os.path.join(self.run["tools-dir"], tool_name)
                this_tool_config = os.path.join(this_tool_dir, "rickshaw.json")
                tool_cfg, err = load_json_file(this_tool_config)
                if tool_cfg is None:
                    logger.error("Could not open the tool config file: %s", this_tool_config)
                    sys.exit(1)
                if tool_cfg.get("tool") != tool_name:
                    logger.error("In tool config %s, 'tool' does not match '%s'", this_tool_config, tool_name)
                    sys.exit(1)
                self.tools_configs[tool_name] = tool_cfg

            self.bench_dirs[tool_id] = os.path.join(self.run["tools-dir"], tool_name)
            self.image_ids.setdefault(tool_id, {})[tool_entry["userenv"]] = {"image": ""}

        save_json_file(os.path.join(self.config_dir, "tool-id-map.json"), tool_id_map)

    def load_utility_params(self):
        for utility in UTILITIES:
            utility_dir_key = f"{utility}-dir"
            if utility_dir_key in self.run:
                this_utility_config = os.path.join(self.run[utility_dir_key], "rickshaw.json")
                json_ref, err = load_json_file(this_utility_config)
                if json_ref is None:
                    logger.error("Could not open the utility config file: %s", this_utility_config)
                    sys.exit(1)
                if json_ref.get("utility") != utility:
                    logger.error("In utility config %s, 'utility' does not match '%s'", this_utility_config, utility)
                    sys.exit(1)
                self.utility_configs[utility] = json_ref
            else:
                logger.error("Could not determine utility location for '%s'", utility)
                sys.exit(1)

    def build_test_order(self):
        num_iterations = len(self.run.get("iterations", []))
        num_samples = int(self.run.get("num-samples", 1))
        test_order = self.run.get("test-order", "sample")

        if test_order == "s":
            logger.warning("WARNING: The short forms of 'test-order' are deprecated. Please change 's' to 'sample'.")
            test_order = "sample"
            self.run["test-order"] = test_order
        elif test_order == "i":
            logger.warning("WARNING: The short forms of 'test-order' are deprecated. Please change 'i' to 'iteration'.")
            test_order = "iteration"
            self.run["test-order"] = test_order
        elif test_order == "r":
            logger.warning("WARNING: The short forms of 'test-order' are deprecated. Please change 'r' to 'random'.")
            test_order = "random"
            self.run["test-order"] = test_order

        self.tests = []
        if test_order == "sample":
            for iter_id in range(1, num_iterations + 1):
                for samp_id in range(1, num_samples + 1):
                    self.tests.append({"iteration-id": iter_id, "sample-id": samp_id})
        elif test_order == "iteration":
            for samp_id in range(1, num_samples + 1):
                for iter_id in range(1, num_iterations + 1):
                    self.tests.append({"iteration-id": iter_id, "sample-id": samp_id})
        elif test_order == "random":
            total_samples = num_iterations * num_samples
            available_tests = []
            for iid in range(1, num_iterations + 1):
                available_tests.append({
                    "iteration-id": iid,
                    "samples": list(range(1, num_samples + 1)),
                })
            while available_tests:
                idx = random.randint(0, len(available_tests) - 1)
                samp_id = available_tests[idx]["samples"].pop(0)
                self.tests.append({
                    "iteration-id": available_tests[idx]["iteration-id"],
                    "sample-id": samp_id,
                })
                if not available_tests[idx]["samples"]:
                    available_tests.pop(idx)
            if len(self.tests) != total_samples:
                logger.error("[ERROR] Expected %d tests but found %d", total_samples, len(self.tests))
                sys.exit(1)
        else:
            logger.error("[ERROR] Value for --test-order [%s] is not valid", test_order)
            self.usage()
            sys.exit(1)

        save_json_file(os.path.join(self.config_dir, "test-order.json"), self.tests)
        logger.info("Test order (%s): %d tests", test_order, len(self.tests))

    def build_tool_cmd(self, tool_entry, start_stop, endpoint_type=None):
        tool_name = tool_entry["tool"]
        tool_id = tool_entry["tool-id"]

        blacklist = self.tools_configs[tool_name].get("collector", {}).get("blacklist", [])
        for item in blacklist:
            if endpoint_type and endpoint_type == item.get("endpoint"):
                logger.debug("Tool %s is blacklisted for endpoint %s", tool_id, endpoint_type)
                return None

        tool = {
            "name": tool_id,
            "command": "declare -a ARGS=(",
            "deployment": "auto",
            "opt-tag": None,
        }

        if tool_entry.get("deployment", "auto") != "auto":
            tool["deployment"] = tool_entry["deployment"]
            if "opt-tag" in tool_entry:
                tool["opt-tag"] = tool_entry["opt-tag"]

        for tool_param in tool_entry.get("params", []):
            if tool_param.get("enabled") == "no":
                continue
            tool["command"] += f"'--{tool_param['arg']}' '{tool_param.get('val', '')}' "

        tool["command"] = tool["command"].rstrip()
        tool["command"] += ") && "
        tool["command"] += self.tools_configs[tool_name]["collector"][start_stop] + ' "${ARGS[@]}"'

        return tool

    def build_files_list(self, cs_type, cs_id=None, benchmark=None):
        if cs_id is not None:
            cs_file_list = os.path.join(self.engine_config_dir, f"{cs_type}-{cs_id}-files-list")
        else:
            cs_file_list = os.path.join(self.engine_config_dir, f"{cs_type}-files-list")

        with open(cs_file_list, "w") as fh:
            if cs_type in ("client", "server") and benchmark and \
               benchmark in self.bench_configs and \
               "files-from-controller" in self.bench_configs[benchmark].get(cs_type, {}):
                for file_spec in self.bench_configs[benchmark][cs_type]["files-from-controller"]:
                    required = file_spec.get("required", 1)
                    src_file = file_spec["src"]
                    src_file = src_file.replace("%bench-dir%", self.bench_dirs.get(benchmark, "") + "/")
                    src_file = src_file.replace("%run-dir%", self.run_dir + "/")
                    src_file = src_file.replace("%config-dir%", self.config_dir + "/")
                    dest_file = file_spec["dest"]
                    if os.path.exists(src_file):
                        logger.debug("adding %s for engine type '%s'", src_file, cs_type)
                        fh.write(f"src={src_file}\ndest={dest_file}\n")
                    elif not required:
                        logger.debug("skipping %s (not required) for engine type '%s'", src_file, cs_type)
                    else:
                        logger.error("[ERROR] Could not find required file %s for engine type '%s'", src_file, cs_type)
                        sys.exit(1)

            for tool_entry in self.tools_params:
                tool_name = tool_entry["tool"]
                collector_cfg = self.tools_configs.get(tool_name, {}).get("collector", {})
                for file_spec in collector_cfg.get("files-from-controller", []):
                    src_file = file_spec["src"]
                    src_file = src_file.replace("%tool-dir%", f"{self.run['tools-dir']}/{tool_name}/")
                    src_file = src_file.replace("%run-dir%", self.run_dir + "/")
                    src_file = src_file.replace("%config-dir%", self.config_dir + "/")
                    dest_file = file_spec["dest"]
                    if os.path.exists(src_file):
                        logger.debug("adding %s for tool '%s'", src_file, tool_name)
                        fh.write(f"src={src_file}\ndest={dest_file}\n")
                    elif not file_spec.get("required", 1):
                        logger.debug("skipping %s (not required) for tool '%s'", src_file, tool_name)
                    else:
                        logger.error("[ERROR] Could not find required file %s for tool '%s'", src_file, tool_name)
                        sys.exit(1)

            for utility in UTILITIES:
                utility_dir = self.run.get(f"{utility}-dir", "")
                engine_cfg = self.utility_configs.get(utility, {}).get("engine", {})
                for file_spec in engine_cfg.get("files-from-controller", []):
                    src_file = file_spec["src"]
                    src_file = src_file.replace("%utility-dir%", utility_dir)
                    dest_file = file_spec["dest"]
                    if os.path.exists(src_file):
                        logger.debug("adding %s for utility '%s'", src_file, utility)
                        fh.write(f"src={src_file}\ndest={dest_file}\n")
                    elif not file_spec.get("required", 1):
                        logger.debug("skipping %s (not required) for utility '%s'", src_file, utility)
                    else:
                        logger.error("[ERROR] Could not find required file %s for utility '%s'", src_file, utility)
                        sys.exit(1)

    def prepare_bench_tool_engines(self):
        for this_benchmark in self.bench_configs:
            pre_script = self.bench_configs[this_benchmark].get("controller", {}).get("pre-script", "")
            if pre_script:
                logger.info("generating pre-script cmd for benchmark '%s'", this_benchmark)
                params_str = dump_params(
                    self.run["iterations"][0].get("params", []), "1", "client", self.ids_to_benchmark
                )
                cmd = f"{pre_script} {params_str}"
                cmd = cmd.replace("%bench-dir%", self.bench_dirs[this_benchmark])
                cmd = cmd.replace("%run-dir%", self.run_dir)
                logger.debug("controller pre-script command: [%s]", cmd)
                _, output, rc = run_cmd(f"cd {self.run_dir} && {cmd}")
                if rc != 0:
                    logger.error("controller pre-script failed with return code = %d", rc)
                    logger.error("controller pre-script command: %s", cmd)
                    logger.error("controller pre-script output:\n%s", output)
                    sys.exit(rc)

        for src, dst in [
            (os.path.join(self.rickshaw_project_dir, "engine", "engine-script"), self.engine_run_script),
            (os.path.join(self.rickshaw_project_dir, "engine", "engine-script-library"), self.engine_library_script),
            (os.path.join(self.run["roadblock-dir"], "roadblocker.py"), self.engine_roadblock_script),
            (os.path.join(self.run["roadblock-dir"], "roadblocker_config.py"), self.engine_roadblock_config),
            (os.path.join(self.run["roadblock-dir"], "roadblock.py"), self.engine_roadblock_module),
        ]:
            shutil.copy2(src, dst)
            os.chmod(dst, 0o755)

        all_collector_types = sorted(self.active_collector_types.keys())
        logger.info("Active Collector Types: %s", " ".join(all_collector_types))

        collector_tools = {}
        for tool_entry in self.tools_params:
            tool_name = tool_entry["tool"]
            tool_id = tool_entry["tool-id"]
            whitelist = self.tools_configs.get(tool_name, {}).get("collector", {}).get("whitelist", [])
            for item in whitelist:
                endpoint = item.get("endpoint", "")
                if endpoint in dump_endpoint_types(self.endpoints):
                    for collector in item.get("collector-types", []):
                        collector_tools.setdefault(collector, []).append(tool_id)

        logger.info("Generating collector type tool commands")
        for collector in all_collector_types:
            logger.debug("processing collector type [%s]", collector)
            for start_stop in ("start", "stop"):
                collector_tool_cmds_dir = os.path.join(self.tool_cmds_dir, collector)
                os.makedirs(collector_tool_cmds_dir, exist_ok=True)
                tool_cmd_file = os.path.join(collector_tool_cmds_dir, f"{start_stop}.json")
                tool_data = {"tools": []}

                for tool_entry in self.tools_params:
                    tool_name = tool_entry["tool"]
                    tool_id = tool_entry["tool-id"]
                    whitelist = self.tools_configs.get(tool_name, {}).get("collector", {}).get("whitelist", [])
                    for item in whitelist:
                        if collector in item.get("collector-types", []):
                            tool = self.build_tool_cmd(tool_entry, start_stop)
                            if tool is not None:
                                tool_data["tools"].append(tool)
                            break

                save_json_file(tool_cmd_file, tool_data)

        logger.info("Generating per client/server engine tool commands")
        for cs_type, cs_entries in self.clients_servers.items():
            if cs_type == "profiler":
                continue
            for start_stop in ("start", "stop"):
                for cs_id, cs_ref in cs_entries.items():
                    cs_tool_cmds_dir = os.path.join(self.tool_cmds_dir, cs_type, str(cs_id))
                    os.makedirs(cs_tool_cmds_dir, exist_ok=True)
                    tool_cmd_file = os.path.join(cs_tool_cmds_dir, f"{start_stop}.json")
                    tool_data = {"tools": []}

                    for tool_entry in self.tools_params:
                        tool = self.build_tool_cmd(tool_entry, start_stop, cs_ref.get("endpoint-type"))
                        if tool is not None:
                            tool_data["tools"].append(tool)

                    save_json_file(tool_cmd_file, tool_data)

        logger.debug("clients_servers: [%s]", " ".join(self.clients_servers.keys()))
        for cs_type, cs_entries in self.clients_servers.items():
            if cs_type == "profiler":
                continue
            for cs_id, cs_ref in cs_entries.items():
                cmd_type_files = ["start"]
                if cs_type == "server":
                    cmd_type_files.append("stop")
                if cs_type == "client":
                    cmd_type_files.extend(["runtime", "infra"])

                this_cmds_dir = os.path.join(self.engine_bench_cmds_dir, cs_type, str(cs_id))
                os.makedirs(this_cmds_dir, exist_ok=True)

                for cmd_type in cmd_type_files:
                    if cmd_type == "runtime" and int(cs_id) > 1:
                        continue
                    this_cmd_file = os.path.join(this_cmds_dir, cmd_type)
                    with open(this_cmd_file, "w") as fh:
                        for test_ref in self.tests:
                            test_iter = test_ref["iteration-id"]
                            test_samp = test_ref["sample-id"]
                            iter_array_idx = test_iter - 1
                            benchmark = self.ids_to_benchmark.get(str(cs_id))
                            bench_cfg = self.bench_configs.get(benchmark, {})
                            cmd_template = bench_cfg.get(cs_type, {}).get(cmd_type, "")
                            if cmd_template:
                                params_str = dump_params(
                                    self.run["iterations"][iter_array_idx].get("params", []),
                                    cs_id, cs_type, self.ids_to_benchmark
                                )
                                cmd = f"{cmd_template} {params_str}"
                                param_regex_list = bench_cfg.get(cs_type, {}).get("param_regex", [])
                                for r in param_regex_list:
                                    cmd = perl_s_regex(cmd, r)
                                fh.write(f"{test_iter}-{test_samp} {cmd}\n")
                            elif cmd_type != "infra":
                                logger.error("[ERROR] Could not find %s in bench_config", cmd_type)
                                sys.exit(1)
                    os.chmod(this_cmd_file, 0o755)

        for cs_type in list(self.clients_servers.keys()) + all_collector_types:
            if cs_type in ("client", "server"):
                for cs_id in self.clients_servers.get(cs_type, {}):
                    benchmark = self.ids_to_benchmark.get(str(cs_id))
                    self.build_files_list(cs_type, cs_id, benchmark)
            else:
                self.build_files_list(cs_type)

    def source_images(self):
        logger.info("Preparing userenvs:")
        logger.debug("image_ids (before): %s", json.dumps(self.image_ids, indent=2, default=str))

        dedup_image_ids = {}
        dedup_bench_dirs = {}
        for tid in list(self.image_ids.keys()):
            tool_name = tid
            for tool_entry in self.tools_params:
                if tool_entry["tool-id"] == tid:
                    tool_name = tool_entry["tool"]
                    break
            if tid in self.bench_dirs:
                dedup_bench_dirs[tool_name] = self.bench_dirs[tid]
            for userenv in self.image_ids[tid]:
                dedup_image_ids.setdefault(tool_name, {})[userenv] = self.image_ids[tid][userenv]

        for bench in self.bench_dirs:
            if bench not in dedup_bench_dirs and not any(t["tool-id"] == bench for t in self.tools_params):
                dedup_bench_dirs[bench] = self.bench_dirs[bench]

        utility_dirs = {}
        for utility in UTILITIES:
            utility_dir_key = f"{utility}-dir"
            if utility_dir_key in self.run:
                utility_dirs[utility] = self.run[utility_dir_key]

        source_input = {
            "image-ids": dedup_image_ids,
            "registries": self.run.get("registries", {}),
            "registries-json": self.run.get("registries-json", ""),
            "arch": self.arch,
            "bench-dirs": dedup_bench_dirs,
            "utilities": UTILITIES,
            "dirs": {
                "rickshaw": self.rickshaw_project_dir,
                "config": self.config_dir,
                "workshop-build": self.workshop_build_dir,
            },
            "workshop": {
                "dir": self.run.get("workshop-dir", ""),
                "force-builds": self.workshop_force_builds,
                "script": self.workshop_script,
            },
            "roadblock-dir": self.run.get("roadblock-dir", ""),
            "cs-conf-file": self.cs_conf_file,
            "use-workshop": self.use_workshop,
            "utility-dirs": utility_dirs,
        }
        if "external-userenvs-dir" in self.run:
            source_input["external-userenvs-dir"] = self.run["external-userenvs-dir"]

        provenance = self._build_provenance(utility_dirs)
        source_input["provenance"] = provenance

        source_input_file = os.path.join(self.config_dir, "image-source-input.json")
        source_output_file = os.path.join(self.config_dir, "image-source-output.json")
        save_json_file(source_input_file, source_input)

        if "source-images-service-url" not in self.run:
            logger.error("[ERROR] --source-images-service-url is required")
            sys.exit(1)

        source_images_cmd = (
            f"{self.rickshaw_project_dir}/rickshaw-source-images-client"
            f" --input {source_input_file}"
            f" --output {source_output_file}"
            f" --service-url {self.run['source-images-service-url']}"
        )
        logger.info("Running: %s", source_images_cmd)
        rc = subprocess.run(source_images_cmd, shell=True).returncode
        if rc != 0:
            logger.error("rickshaw-source-images-client failed with exit code %d", rc)
            sys.exit(1)

        output_ref, err = load_json_file(source_output_file)
        if output_ref is None:
            output_ref, err = load_json_file(source_output_file + ".xz", uselzma=True)
        if output_ref is None:
            logger.error("Failed to read image source output JSON: %s", source_output_file)
            sys.exit(1)

        for name in output_ref.get("image-ids", {}):
            for userenv in output_ref["image-ids"][name]:
                image = output_ref["image-ids"][name][userenv]["image"]
                if name in self.image_ids:
                    self.image_ids[name][userenv]["image"] = image
                for tool_entry in self.tools_params:
                    if tool_entry["tool"] == name and tool_entry["tool-id"] != name:
                        tool_id = tool_entry["tool-id"]
                        if tool_id in self.image_ids and userenv in self.image_ids[tool_id]:
                            self.image_ids[tool_id][userenv]["image"] = image

        logger.info("Userenv image summary:")
        for bench_or_tool in sorted(self.image_ids.keys()):
            for userenv in sorted(self.image_ids[bench_or_tool].keys()):
                image = self.image_ids[bench_or_tool][userenv]["image"]
                image = re.sub(r'::.*', '', image)
                logger.info("  %s / %s: %s", bench_or_tool, userenv, image)

        logger.debug("image_ids (after): %s", json.dumps(self.image_ids, indent=2, default=str))

    def _build_provenance(self, utility_dirs):
        provenance = {}

        _, build_date, _ = run_cmd("date -u +%Y-%m-%dT%H:%M:%SZ")
        provenance["build-date"] = build_date.strip()

        _, hostname, _ = run_cmd("hostname -f 2>/dev/null || hostname")
        _, ip, _ = run_cmd("ip -4 route get 1.0.0.0 2>/dev/null | grep -oP 'src \\K\\S+' || echo unknown")
        provenance["source"] = {"hostname": hostname.strip(), "ip": ip.strip()}

        _, client_commit, _ = run_cmd(f"git -C {self.rickshaw_project_dir} rev-parse HEAD 2>/dev/null")
        rickshaw_client = {"commit": client_commit.strip(), "dirty": "false"}
        dirty_rc = subprocess.run(
            f"git -C {self.rickshaw_project_dir} diff --quiet 2>/dev/null", shell=True
        ).returncode
        if dirty_rc != 0:
            rickshaw_client["dirty"] = "true"
            _, diff_hash, _ = run_cmd(
                f"git -C {self.rickshaw_project_dir} diff 2>/dev/null | sha256sum | awk '{{print $1}}'"
            )
            rickshaw_client["diff-hash"] = diff_hash.strip()
            _, diff_content, _ = run_cmd(f"git -C {self.rickshaw_project_dir} diff 2>/dev/null")
            rickshaw_client["diff"] = diff_content
        provenance["rickshaw-client"] = rickshaw_client

        repo_paths = {
            "toolbox": os.environ.get("TOOLBOX_HOME", ""),
            "roadblock": self.run.get("roadblock-dir", ""),
            "workshop": self.run.get("workshop-dir", ""),
        }
        seen_paths = set()
        for tid in self.bench_dirs:
            path = self.bench_dirs[tid]
            if path not in seen_paths:
                name = tid
                for tool_entry in self.tools_params:
                    if tool_entry["tool-id"] == tid:
                        name = tool_entry["tool"]
                        break
                repo_paths[name] = path
                seen_paths.add(path)
        for util_name, util_path in utility_dirs.items():
            repo_paths[util_name] = util_path

        repos_provenance = {}
        for name in sorted(repo_paths.keys()):
            path = repo_paths[name]
            if not path or not os.path.isdir(path):
                continue
            _, commit, _ = run_cmd(f"git -C {path} rev-parse HEAD 2>/dev/null")
            commit = commit.strip()
            if not commit:
                logger.warning("WARNING: Could not get commit hash for '%s' at '%s'", name, path)
                continue
            repo_info = {"commit": commit, "dirty": "false"}
            dirty_rc = subprocess.run(f"git -C {path} diff --quiet 2>/dev/null", shell=True).returncode
            if dirty_rc != 0:
                repo_info["dirty"] = "true"
                _, diff_hash, _ = run_cmd(f"git -C {path} diff 2>/dev/null | sha256sum | awk '{{print $1}}'")
                repo_info["diff-hash"] = diff_hash.strip()
                _, diff_content, _ = run_cmd(f"git -C {path} diff 2>/dev/null")
                repo_info["diff"] = diff_content
            repos_provenance[name] = repo_info

        provenance["repos"] = repos_provenance
        return provenance

    # ----------------------------------------------------------------
    # Phase 4: Roadblock helpers and deployment
    # ----------------------------------------------------------------

    def do_roadblock(self, label, timeout, followers):
        followers_file = os.path.join(self.roadblock_followers_dir, f"{label}.txt")
        with open(followers_file, "w") as f:
            for follower in followers:
                f.write(f"{follower}\n")

        logger.info("Roadblock: role=leader uuid=%s:%s timeout=%d", self.run["id"], label, timeout)

        rc, messages_data = toolbox_do_roadblock(
            roadblock_id=self.run["id"],
            label=label,
            role="leader",
            leader_id="controller",
            timeout=timeout,
            redis_server="localhost",
            redis_password=self.run["roadblock-password"],
            followers_file=followers_file,
            abort=self.abort_via_roadblock,
            connection_watchdog=(self.rb_connection_watchdog == "enabled"),
            log_level=self.rb_log_level,
            msgs_dir=self.roadblock_msgs_dir,
        )

        if rc in (ROADBLOCK_EXITS["abort"], ROADBLOCK_EXITS["abort_waiting"]):
            if messages_data and "received" in messages_data:
                for msg in messages_data["received"]:
                    user_obj = msg.get("payload", {}).get("message", {}).get("user-object", {})
                    if "error" in user_obj:
                        sender = msg["payload"]["sender"]["id"]
                        logger.info("\nError from %s:\n%s\n", sender, user_obj["error"])

        self.messages_ref = messages_data

        dropped_followers = []
        if rc not in (ROADBLOCK_EXITS["success"], ROADBLOCK_EXITS["abort"], ROADBLOCK_EXITS["abort_waiting"]):
            msgs_log_file = os.path.join(self.roadblock_msgs_dir, f"{label}.json")
            rb_log_file = os.path.join(self.roadblock_logs_dir, f"{label}.txt")
            if os.path.exists(rb_log_file):
                try:
                    with open(rb_log_file) as f:
                        for line in f:
                            if "These followers" in line:
                                parts = line.split(": ", 1)
                                if len(parts) > 1:
                                    dropped_followers.extend(parts[1].split())
                except OSError:
                    pass
            logger.debug("roadblock dropped followers: %s", " ".join(dropped_followers))

        return rc, dropped_followers

    def roadblock_exit_on_error(self, roadblock_rc):
        if roadblock_rc != 0:
            logger.info("roadblock_exit_on_error()")
            self.wait_for_endpoints()
            sys.exit(roadblock_rc)

    def remove_followers(self, dropped, log_msg=True):
        followers_set = set(self.active_followers)
        for f in dropped:
            if f in followers_set:
                if log_msg:
                    logger.info("Dropping follower '%s' in an attempt to gracefully continue", f)
                followers_set.discard(f)
        self.active_followers = list(followers_set)

    def remove_dropped_followers(self, dropped):
        self.remove_followers(dropped, log_msg=True)

    def remove_engine_followers(self, dropped):
        self.remove_followers(dropped, log_msg=False)

    def evaluate_test_roadblock(self, rb_name, roadblock_rc, sample_info, dropped_followers, abort, quit_flag):
        if roadblock_rc != 0:
            if roadblock_rc == ROADBLOCK_EXITS["timeout"]:
                logger.error("[ERROR] roadblock '%s' timed out, attempting to exit and cleanly finish the run", rb_name)
                self.remove_dropped_followers(dropped_followers)
                quit_flag = 1
            elif roadblock_rc in (ROADBLOCK_EXITS["abort"], ROADBLOCK_EXITS["abort_waiting"]):
                if abort == 0:
                    logger.warning("[WARNING] roadblock '%s' received an abort, stopping sample", rb_name)
                    sample_info["attempt-fail"] = 1
                    sample_info["failures"] += 1
                    logger.info("sample failures is now: %d", sample_info["failures"])
                    if sample_info["failures"] >= int(self.run["max-sample-failures"]):
                        sample_info["complete"] = 1
                        logger.error("[ERROR] A maximum of %d failures for iteration %d has been reached",
                                     sample_info["failures"], sample_info["iteration-id"])
                    abort = 1
            else:
                logger.error("[ERROR] roadblock '%s' has reached an unknown state with RC=%d", rb_name, roadblock_rc)
                abort = 1
                quit_flag = 1

        return abort, quit_flag

    # ----------------------------------------------------------------
    # Phase 5: Deployment, roadblock orchestration, cleanup
    # ----------------------------------------------------------------

    def deploy_endpoints(self):
        logger.info("\nCalculating dynamic roadblock timeout values:")
        self.endpoint_deploy_timeout += len(self.endpoints) * 120
        for engine_type, entries in self.clients_servers.items():
            count = len(entries)
            self.endpoint_deploy_timeout += count * 15
            self.engine_script_start_timeout += count * 15
        logger.info("  endpoint-deploy-timeout adjusted to %d seconds", self.endpoint_deploy_timeout)
        logger.info("  engine-script-timeout adjusted to %d seconds", self.engine_script_start_timeout)

        logger.info("\nDeploying endpoints:\n")
        for endpoint in self.endpoints:
            ep_type = endpoint["type"]
            opts = endpoint["opts"]
            label = endpoint["label"]
            endpoint_image_opt = ""

            if "userenvs" in endpoint:
                for userenv in endpoint["userenvs"]:
                    for bench_or_tool in self.image_ids:
                        chosen_userenv = userenv
                        idx = find_index(self.tools_params, "tool-id", bench_or_tool)
                        if idx > -1:
                            chosen_userenv = self.tools_params[idx]["userenv"]
                        if chosen_userenv not in self.image_ids.get(bench_or_tool, {}):
                            logger.error("ERROR: image for %s / userenv %s not found in image_ids",
                                         bench_or_tool, chosen_userenv)
                            sys.exit(1)
                        image = self.image_ids[bench_or_tool][chosen_userenv]["image"]
                        endpoint_image_opt += f",{bench_or_tool}::{chosen_userenv}::{image}"
                if endpoint_image_opt:
                    endpoint_image_opt = f" --image={endpoint_image_opt.lstrip(',')}"

            bench_ids_opt = ""
            if "bench-ids" in self.run:
                bench_ids_opt = f" --bench-ids={self.run['bench-ids']}"

            this_endpoint_run_dir = os.path.join(self.base_endpoint_run_dir, label)
            os.makedirs(this_endpoint_run_dir, exist_ok=True)
            endpoint_project_dir = os.path.join(self.rickshaw_project_dir, "endpoints", ep_type)

            if not os.path.exists(endpoint_project_dir):
                logger.error("[ERROR] could not find endpoint ./endpoints/%s", ep_type)
                sys.exit(1)

            cmd_log = os.path.join(this_endpoint_run_dir, "endpoint-stderrout.txt")
            cmd = (
                f"./{ep_type}"
                f" --rickshaw-dir={self.rickshaw_project_dir}"
                f" --packrat-dir={self.run.get('packrat-dir', '')}"
                f" --endpoint-label={label}"
                f" --run-id={self.run['id']}"
                f" --base-run-dir={self.run['base-run-dir']}"
                f" --max-sample-failures={self.run['max-sample-failures']}"
                f" --endpoint-deploy-timeout={self.endpoint_deploy_timeout}"
                f" --engine-script-start-timeout={self.engine_script_start_timeout}"
                f"{endpoint_image_opt}"
                f"{self.endpoint_roadblock_opt}"
            )

            if ep_type in ("remotehosts", "kube"):
                os.environ["ROADBLOCK_HOME"] = self.run.get("roadblock-dir", "")
                for arg in opts.split(","):
                    cmd += f" --{arg}"
                cmd += f" --crucible-dir={os.environ.get('CRUCIBLE_HOME', '')}"
                cmd += f" --log-level={self.endpoint_log_level}"
            else:
                cmd += f" --roadblock-dir={self.run.get('roadblock-dir', '')}"
                cmd += f" --endpoint-opts={opts}{bench_ids_opt}"

            logger.info("Going to run endpoint command for %s and log it to %s:\n%s\n", label, cmd_log, cmd)

            if self.endpoint_roadblock_opt:
                log_fh = open(cmd_log, "w")
                proc = subprocess.Popen(
                    cmd, shell=True, stdout=log_fh, stderr=subprocess.STDOUT,
                    cwd=endpoint_project_dir
                )
                self.endpoint_processes.append((proc, log_fh))

    def process_roadblocks(self):
        self.active_followers = dump_endpoint_labels(self.endpoints)

        rc, _ = self.do_roadblock("endpoint-pre-deploy-begin", self.default_rb_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)

        deployment_followers = []
        if self.messages_ref and "received" in self.messages_ref:
            for msg in self.messages_ref["received"]:
                user_obj = msg.get("payload", {}).get("message", {}).get("user-object", {})
                if "deployment-followers" in user_obj:
                    deployment_followers.extend(user_obj["deployment-followers"])

        rc, _ = self.do_roadblock("endpoint-pre-deploy-end", self.default_rb_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)

        deployment_followers.extend(self.active_followers)

        rc, _ = self.do_roadblock("endpoint-deploy-begin", self.endpoint_deploy_timeout, deployment_followers)
        self.roadblock_exit_on_error(rc)

        new_followers = []
        if self.messages_ref and "received" in self.messages_ref:
            for msg in self.messages_ref["received"]:
                user_obj = msg.get("payload", {}).get("message", {}).get("user-object", {})
                if "new-followers" in user_obj:
                    new_followers.extend(user_obj["new-followers"])

        rc, _ = self.do_roadblock("endpoint-deploy-end", self.endpoint_deploy_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)

        self.active_followers.extend(new_followers)
        self.active_followers.extend(self.rb_cs_ids)

        rc, _ = self.do_roadblock("engine-init-begin", self.engine_script_start_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)
        rc, _ = self.do_roadblock("engine-init-end", self.engine_script_start_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)

        rc, _ = self.do_roadblock("get-data-begin", self.default_rb_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)
        rc, _ = self.do_roadblock("get-data-end", self.default_rb_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)

        os.makedirs(os.path.join(self.run_dir, "sysinfo", "endpoint"), exist_ok=True)
        rc, _ = self.do_roadblock("collect-sysinfo-begin", self.collect_sysinfo_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)
        rc, _ = self.do_roadblock("collect-sysinfo-end", self.collect_sysinfo_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)

        rc, _ = self.do_roadblock("start-tools-begin", self.default_rb_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)
        rc, _ = self.do_roadblock("start-tools-end", self.default_rb_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)

        self.process_bench_roadblocks()

        rc, dropped = self.do_roadblock("stop-tools-begin", self.default_rb_timeout, self.active_followers)
        self.remove_dropped_followers(dropped)
        rc, dropped = self.do_roadblock("stop-tools-end", self.default_rb_timeout, self.active_followers)
        self.remove_dropped_followers(dropped)

        rc, dropped = self.do_roadblock("send-data-begin", self.default_rb_timeout, self.active_followers)
        self.remove_dropped_followers(dropped)
        rc, dropped = self.do_roadblock("send-data-end", self.default_rb_timeout, self.active_followers)
        self.remove_dropped_followers(dropped)

        self.remove_engine_followers(new_followers)
        self.remove_engine_followers(self.rb_cs_ids)

        rc, dropped = self.do_roadblock("endpoint-cleanup-begin", self.default_rb_timeout, self.active_followers)
        self.remove_dropped_followers(dropped)
        rc, dropped = self.do_roadblock("endpoint-cleanup-end", self.default_rb_timeout, self.active_followers)
        self.remove_dropped_followers(dropped)

    def process_bench_roadblocks(self):
        rc, _ = self.do_roadblock("setup-bench-begin", self.default_rb_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)

        quit_flag = 0
        sample_data = [None] * len(self.tests)

        rc, _ = self.do_roadblock("setup-bench-end", self.default_rb_timeout, self.active_followers)
        self.roadblock_exit_on_error(rc)

        for tid in range(len(self.tests)):
            if quit_flag:
                break

            iter_id = self.tests[tid]["iteration-id"]
            samp_id = self.tests[tid]["sample-id"]
            idx = tid

            if sample_data[idx] is None:
                sample_data[idx] = {
                    "iteration-id": iter_id,
                    "sample-id": samp_id,
                    "failures": 0,
                    "complete": 0,
                    "attempt-num": 0,
                    "attempt-fail": 0,
                }

            timeout = self.default_rb_timeout
            abort = 0

            while (quit_flag == 0 and abort == 0 and
                   sample_data[idx]["complete"] == 0 and
                   sample_data[idx]["failures"] < int(self.run["max-sample-failures"])):

                sample_data[idx]["attempt-fail"] = 0
                sample_data[idx]["attempt-num"] += 1

                logger.info(
                    "Starting iteration %d sample %d (test %d of %d) attempt number %d of %d",
                    iter_id, samp_id, tid + 1, len(self.tests),
                    sample_data[idx]["attempt-num"], int(self.run["max-sample-failures"])
                )

                test_id = f"{sample_data[idx]['iteration-id']}-{sample_data[idx]['sample-id']}-{sample_data[idx]['attempt-num']}"
                rb_prefix = f"{test_id}:"

                for phase in ("infra-start", "server-start", "endpoint-start"):
                    for suffix in ("begin", "end"):
                        rb_name = f"{rb_prefix}{phase}-{suffix}"
                        rc, dropped = self.do_roadblock(rb_name, timeout, self.active_followers)
                        abort, quit_flag = self.evaluate_test_roadblock(
                            rb_name, rc, sample_data[idx], dropped, abort, quit_flag
                        )
                        self.remove_dropped_followers(dropped)

                rb_name = f"{rb_prefix}client-start-begin"
                rc, dropped = self.do_roadblock(rb_name, timeout, self.active_followers)
                abort, quit_flag = self.evaluate_test_roadblock(
                    rb_name, rc, sample_data[idx], dropped, abort, quit_flag
                )
                self.remove_dropped_followers(dropped)

                if self.messages_ref and "received" in self.messages_ref:
                    for message in self.messages_ref["received"]:
                        user_obj = message.get("payload", {}).get("message", {}).get("user-object", {})
                        if "timeout" in user_obj:
                            value = user_obj["timeout"]
                            if value == "unbounded":
                                logger.info("A client engine has indicated it will be running an unbounded workload")
                            else:
                                timeout = int(value)
                                logger.info("Found new client-start-end timeout value: %s", timeout)

                rb_name = f"{rb_prefix}client-start-end"
                rc, dropped = self.do_roadblock(rb_name, timeout, self.active_followers)
                abort, quit_flag = self.evaluate_test_roadblock(
                    rb_name, rc, sample_data[idx], dropped, abort, quit_flag
                )
                self.remove_dropped_followers(dropped)

                if timeout != self.default_rb_timeout:
                    timeout = self.default_rb_timeout
                    logger.info("Resetting timeout value: %s", timeout)

                for phase in ("client-stop", "endpoint-stop", "server-stop", "infra-stop"):
                    for suffix in ("begin", "end"):
                        rb_name = f"{rb_prefix}{phase}-{suffix}"
                        rc, dropped = self.do_roadblock(rb_name, timeout, self.active_followers)
                        abort, quit_flag = self.evaluate_test_roadblock(
                            rb_name, rc, sample_data[idx], dropped, abort, quit_flag
                        )
                        self.remove_dropped_followers(dropped)

                if sample_data[idx]["attempt-fail"] == 0 and abort == 0 and quit_flag == 0:
                    sample_data[idx]["complete"] = 1
                    sample_result = "successfully"
                else:
                    sample_result = "unsuccessfully"
                    if abort:
                        logger.warning("[WARNING] An abort signal has been encountered!")
                    if quit_flag:
                        logger.error("[ERROR] A quit signal has been encountered!")

                logger.info(
                    "Completed iteration %d sample %d (test %d of %d) attempt number %d of %d %s",
                    iter_id, samp_id, tid + 1, len(self.tests),
                    sample_data[idx]["attempt-num"], int(self.run["max-sample-failures"]),
                    sample_result
                )

    def wait_for_endpoints(self):
        logger.info("Waiting for endpoints to exit")
        for proc, log_fh in self.endpoint_processes:
            proc.wait()
            log_fh.close()
        logger.info("All endpoints have exited")

        logger.info("Compressing endpoint logs:")
        for endpoint in self.endpoints:
            label = endpoint["label"]
            endpoint_log = os.path.join(self.base_endpoint_run_dir, label, "endpoint-stderrout.txt")
            logger.info("\t%s", label)
            xz_cmd = f"xz --verbose --best --threads=0 {endpoint_log}"
            _, xz_output, xz_rc = run_cmd(xz_cmd)
            if xz_rc == 0:
                list_cmd = f"xz --verbose --list {endpoint_log}.xz"
                _, list_output, list_rc = run_cmd(list_cmd)
                if list_rc == 0:
                    for line in list_output.split("\n"):
                        if "Ratio:" in line:
                            fields = line.split()
                            try:
                                ratio = float(fields[2])
                                savings = (1.0 - ratio) * 100.0
                                logger.info("\t\t%.2f%% reduction", savings)
                            except (ValueError, IndexError):
                                pass
                            break
                else:
                    logger.info("\t\tfailed to query")
            else:
                logger.info("\t\tfailed to compress")

    def organize_run_data(self):
        logger.info("Moving per-client/server/tool data into common iterations and tool-data directories")
        tmp_data_dir = os.path.join(self.run_dir, "tmp-data-dir")
        os.makedirs(tmp_data_dir, exist_ok=True)

        try:
            archives = [a for a in os.listdir(self.engine_archives_dir)
                        if re.match(r'^(\w+)-(.+)-data\.tgz$', a)]
        except FileNotFoundError:
            archives = []

        for archive in sorted(archives):
            m = re.match(r'^(\w+)-(.+)-data\.tgz$', archive)
            if not m:
                continue
            cs_type = m.group(1)
            cs_id = m.group(2)
            logger.info("cs_type: %s, cs_id: %s", cs_type, cs_id)

            archive_full_path = os.path.join(self.engine_archives_dir, archive)
            tar_cmd = f"cd {tmp_data_dir} && tar zmxf {archive_full_path}"
            _, _, _ = run_cmd(tar_cmd)

            if cs_type in ("client", "server"):
                for i in range(1, len(self.run.get("iterations", [])) + 1):
                    iter_dir = os.path.join(tmp_data_dir, f"iteration-{i}")
                    if os.path.isdir(iter_dir):
                        for samp_dir in sorted(os.listdir(iter_dir)):
                            if not samp_dir.startswith("sample"):
                                continue
                            iter_samp_path = os.path.join(iter_dir, samp_dir)
                            dest = os.path.join(self.run_dir, "iterations", f"iteration-{i}", samp_dir, cs_type, cs_id)
                            os.makedirs(dest, exist_ok=True)
                            if os.path.isdir(iter_samp_path):
                                for item in os.listdir(iter_samp_path):
                                    shutil.move(os.path.join(iter_samp_path, item), dest)

            tool_data_dir = os.path.join(tmp_data_dir, "tool-data")
            if os.path.isdir(tool_data_dir) and os.listdir(tool_data_dir):
                dest = os.path.join(self.run_dir, "tool-data", cs_type, cs_id)
                os.makedirs(dest, exist_ok=True)
                for item in os.listdir(tool_data_dir):
                    shutil.move(os.path.join(tool_data_dir, item), dest)
            elif os.path.isdir(tool_data_dir) and cs_type != "profiler":
                logger.warning("WARNING: did not find expected sub-directories in tool-data for %s-%s", cs_type, cs_id)

            sysinfo_dir = os.path.join(tmp_data_dir, "sysinfo")
            if os.path.isdir(sysinfo_dir) and os.listdir(sysinfo_dir):
                dest = os.path.join(self.run_dir, "sysinfo", cs_type, cs_id)
                os.makedirs(dest, exist_ok=True)
                for item in os.listdir(sysinfo_dir):
                    shutil.move(os.path.join(sysinfo_dir, item), dest)

            subprocess.run(f"/bin/rm -rf {tmp_data_dir}/*", shell=True)
            subprocess.run(f"/bin/rm -rf {archive_full_path}", shell=True)

        shutil.rmtree(tmp_data_dir, ignore_errors=True)


def main():
    global logger
    logger = setup_logging("rickshaw-run", "normal")
    logger.info("rickshaw-run.py starting")

    state = RunState()
    logger.info("Found %d available cpus, arch=%s", state.available_cpus, state.arch)

    for e in ("RS_NAME", "RS_EMAIL", "RS_TAGS", "RS_DESC"):
        if e in os.environ:
            var = e.replace("RS_", "").lower().replace("_", "-")
            logger.debug("Found environment variable: %s, assigning '%s' to %s", e, os.environ[e], var)
            state.run[var] = os.environ[e]

    state.process_cmdline()
    state.load_settings_info()
    state.load_bench_params()
    state.validate_controller_env()
    state.make_run_dirs()
    state.save_config_info()
    state.validate_endpoints()
    state.load_tool_params()
    state.load_utility_params()
    state.build_test_order()
    state.prepare_bench_tool_engines()

    state.cs_conf_file = os.path.join(state.config_dir, "cs-conf.json")
    state.source_images()

    state.deploy_endpoints()
    state.process_roadblocks()
    state.wait_for_endpoints()
    state.organize_run_data()

    run_file = os.path.join(state.run_dir, "rickshaw-run.json")
    state.run["rickshaw-run"] = {"schema": {"version": "2020.03.18"}}
    state.run["max-sample-failures"] = int(state.run.get("max-sample-failures", 1))
    state.run["num-samples"] = int(state.run.get("num-samples", 1))
    save_json_file(run_file, state.run)

    if state.abort_test_id is not None:
        logger.warning(
            "WARNING: test %s was aborted. and all subsequent tests were not attempted. Run is incomplete",
            state.abort_test_id
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
