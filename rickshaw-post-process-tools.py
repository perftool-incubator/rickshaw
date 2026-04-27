#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import argparse
import os
import re
import sys
from pathlib import Path

TOOLBOX_HOME = os.environ.get("TOOLBOX_HOME")
if TOOLBOX_HOME:
    sys.path.append(str(Path(TOOLBOX_HOME) / "python"))

RICKSHAW_HOME = os.environ.get("RICKSHAW_HOME")
if RICKSHAW_HOME:
    sys.path.append(RICKSHAW_HOME)

from toolbox.json import load_json_file, validate_schema
from toolbox.logging import setup_logging
from toolbox.run import run_cmd
from toolbox.parallel import run_parallel_jobs, get_max_workers
from rickshaw_lib.schema_fixup import rickshaw_run_schema_fixup


def main():
    parser = argparse.ArgumentParser(
        description="Run tool-specific post-processing scripts for a completed rickshaw run"
    )
    parser.add_argument("--base-run-dir", required=True,
                        help="Directory where result data is located")
    parser.add_argument("--log-level", default="normal",
                        choices=["normal", "debug"],
                        help="Logging verbosity")
    args = parser.parse_args()

    logger = setup_logging("rickshaw-post-process-tools", args.log_level)

    base_run_dir = str(Path(args.base_run_dir).resolve())
    config_dir = os.path.join(base_run_dir, "config")
    run_dir = os.path.join(base_run_dir, "run")

    rickshaw_project_dir = str(Path(__file__).resolve().parent)
    tool_schema_file = os.path.join(rickshaw_project_dir, "schema", "tool.json")
    run_schema_file = os.path.join(rickshaw_project_dir, "schema", "rickshaw-run.json")

    # Load the existing rickshaw-run.json
    run_file = os.path.join(run_dir, "rickshaw-run.json")

    fixup_status = rickshaw_run_schema_fixup(run_file, run_schema_file)
    if fixup_status != 0:
        sys.exit(fixup_status)

    logger.debug("Opening %s for normal processing", run_file)
    run_data, err = load_json_file(run_file)
    if run_data is None:
        run_data, err = load_json_file(run_file + ".xz", uselzma=True)
    if run_data is None:
        logger.error("Could not open the run file: %s", err)
        sys.exit(1)

    if "tools-dir" not in run_data:
        tools_dir = str(Path(rickshaw_project_dir, "..", "..", "..", "subprojects", "tools").resolve())
        logger.info("Assuming tools-dir is %s", tools_dir)
        run_data["tools-dir"] = tools_dir

    # Load tool-id-map.json for multi-instance tool support
    tool_id_map = {}
    tool_id_map_file = os.path.join(config_dir, "tool-id-map.json")
    for candidate in [tool_id_map_file, tool_id_map_file + ".xz"]:
        if os.path.exists(candidate):
            use_xz = candidate.endswith(".xz")
            map_data, map_err = load_json_file(candidate, uselzma=use_xz)
            if map_data is not None:
                tool_id_map = map_data
            break

    # Collect post-processing jobs
    logger.info("Launching a post-process job for each tool * each collector")
    tools_config = {}
    tool_data_dir = os.path.join(run_dir, "tool-data")
    jobs = []

    tool_schema, schema_err = load_json_file(tool_schema_file)
    if tool_schema is None:
        logger.error("Could not load tool schema: %s", schema_err)
        sys.exit(1)

    if not os.path.isdir(tool_data_dir):
        logger.info("No tool-data directory found, nothing to post-process")
        sys.exit(0)

    engine_pattern = re.compile(r'^[a-zA-Z0-9]+-\d+-[a-zA-Z0-9-]+-\d+$')

    for collector in sorted(os.listdir(tool_data_dir)):
        collector_dir = os.path.join(tool_data_dir, collector)
        if not os.path.isdir(collector_dir):
            continue

        for engine in sorted(os.listdir(collector_dir)):
            if not engine_pattern.match(engine):
                continue
            engine_dir = os.path.join(collector_dir, engine)
            if not os.path.isdir(engine_dir):
                continue

            logger.info("Working on tool dir %s/%s", collector, engine)

            for tool in sorted(os.listdir(engine_dir)):
                tool_dir = os.path.join(engine_dir, tool)
                if not os.path.isdir(tool_dir):
                    continue

                # Resolve tool ID to tool name
                tool_name = tool_id_map.get(tool, tool)

                if tool_name not in tools_config:
                    tool_config_file = os.path.join(run_data["tools-dir"], tool_name, "rickshaw.json")
                    tool_cfg, cfg_err = load_json_file(tool_config_file)
                    if tool_cfg is None:
                        logger.error("Could not open the tool config file: %s", tool_config_file)
                        sys.exit(1)
                    if tool_cfg.get("tool") != tool_name:
                        logger.error(
                            "In tool config %s, the value for 'tool' does not match '%s'",
                            tool_config_file, tool_name
                        )
                        sys.exit(1)
                    tools_config[tool_name] = tool_cfg

                pp_cmd = tools_config[tool_name].get("controller", {}).get("post-script", "")
                pp_cmd = pp_cmd.replace("%tool-dir%", os.path.join(run_data["tools-dir"], tool_name) + "/")
                pp_cmd = pp_cmd.replace("%run-dir%", run_dir + "/")
                pp_cmd = pp_cmd.replace("%config-dir%", config_dir + "/")

                if pp_cmd and os.path.exists(pp_cmd):
                    jobs.append({
                        "tool_dir": tool_dir,
                        "pp_cmd": pp_cmd,
                        "tool": tool,
                        "tool_name": tool_name,
                    })

    if not jobs:
        logger.info("No post-processing jobs to run")
        sys.exit(0)

    max_workers = get_max_workers()
    logger.info("Will run a maximum of %d post-processing jobs at a time", max_workers)

    def run_post_process(job):
        cmd = f"cd {job['tool_dir']} && {job['pp_cmd']} >post-process-output.txt 2>&1"
        _, output, rc = run_cmd(cmd)
        return rc

    results = run_parallel_jobs(jobs, run_post_process, max_workers=max_workers)

    failures = 0
    for job, result in results:
        if isinstance(result, Exception):
            logger.error("Post-processing failed for %s: %s", job["tool"], result)
            failures += 1
        elif result != 0:
            logger.warning("Post-processing for %s returned rc=%d", job["tool"], result)

    logger.info("Post-processing complete")


if __name__ == "__main__":
    main()
