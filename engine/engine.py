#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import logging
import os
import sys
from pathlib import Path

TOOLBOX_HOME = os.environ.get("TOOLBOX_HOME", "/opt/toolbox")
sys.path.append(str(Path(TOOLBOX_HOME) / "python"))

from engine_lib import Engine, EngineError
from toolbox.logging import setup_logging

logger = logging.getLogger("engine")


def main():
    engine = Engine()
    engine.load_settings()
    engine.load_roadblock_settings()

    log_level = engine.roadblock_log_level or "normal"
    setup_logging("engine", level=log_level)
    logger.info("Engine starting (log-level: %s)", log_level)
    logger.info("Python %s", sys.version)
    logger.info("os-release:")
    if os.path.exists("/etc/os-release"):
        with open("/etc/os-release") as fp:
            logger.info(fp.read())
    logger.info("uname: %s", os.uname())

    try:
        engine.parse_args()
        engine.validate_env()
        engine.setup_env()
    except EngineError as exc:
        logger.error("Initialization failed: %s", exc)
        sys.exit(1)

    os.chdir(engine.cs_dir)

    cpu_partitioning = os.environ.get("cpu_partitioning", "0")
    if cpu_partitioning == "1":
        hk_cpus = os.environ.get("HK_CPUS", "")
        workload_cpus = os.environ.get("WORKLOAD_CPUS", "")
        if not hk_cpus:
            engine.abort_error(
                "cpu-partitioning enabled but HK_CPUS is empty",
                "engine-init-begin",
            )
            sys.exit(1)
        if not workload_cpus:
            engine.abort_error(
                "cpu-partitioning enabled but WORKLOAD_CPUS is empty",
                "engine-init-begin",
            )
            sys.exit(1)

    # ---- Engine init roadblocks ----
    rc = engine.do_roadblock(
        "engine-init-begin", engine.engine_script_start_timeout
    )
    engine.roadblock_exit_on_error(rc)

    rc = engine.do_roadblock(
        "engine-init-end", engine.engine_script_start_timeout
    )
    engine.roadblock_exit_on_error(rc)
    engine.process_init_messages()
    engine.save_env()

    # ---- Get data ----
    rc = engine.do_roadblock("get-data-begin", engine.default_timeout)
    engine.roadblock_exit_on_error(rc)

    try:
        engine.get_data()
    except EngineError as exc:
        engine.abort_error(str(exc), "get-data-end")

    rc = engine.do_roadblock("get-data-end", engine.default_timeout)
    engine.roadblock_exit_on_error(rc)

    # ---- Collect sysinfo ----
    rc = engine.do_roadblock(
        "collect-sysinfo-begin", engine.collect_sysinfo_timeout
    )
    engine.roadblock_exit_on_error(rc)

    engine.collect_sysinfo()

    rc = engine.do_roadblock(
        "collect-sysinfo-end", engine.collect_sysinfo_timeout
    )
    engine.roadblock_exit_on_error(rc)

    # ---- Tools + benchmarks ----
    start_stop_tools_opt = ""
    if engine.cs_type == "profiler":
        if not engine.tool_name:
            logger.error("tool_name not defined for profiler")
            sys.exit(1)
        start_stop_tools_opt = engine.tool_name

    rc = engine.do_roadblock("start-tools-begin", engine.default_timeout)
    engine.roadblock_exit_on_error(rc)

    engine.start_tools(one_tool=start_stop_tools_opt or None)

    rc = engine.do_roadblock("start-tools-end", engine.default_timeout)
    engine.roadblock_exit_on_error(rc)

    engine.process_bench_roadblocks()

    # ---- Stop tools (via wait-for) ----
    engine.do_roadblock("stop-tools-begin", engine.default_timeout)
    stop_tools_cmd = (
        "python3 /usr/local/bin/engine_lib.py stop_tools"
        " '%s' '%s' '%s' '%s'"
        % (
            os.getcwd(),
            engine.tool_stop_cmds,
            "1" if engine.disable_tools else "0",
            start_stop_tools_opt,
        )
    )
    engine.do_roadblock(
        "stop-tools-end", engine.default_timeout, wait_for=stop_tools_cmd
    )

    # ---- Send data (via wait-for) ----
    engines_shared = os.environ.get("ENGINES_SHARED_DIR", "/shared-engines-dir")
    logger.info("Size of %s:", engines_shared)
    os.system("du -hsc %s" % engines_shared)
    logger.info("Contents of %s:", engines_shared)
    os.system("ls -laR %s" % engines_shared)

    engine.do_roadblock("send-data-begin", engine.default_timeout)
    send_data_cmd = (
        "python3 /usr/local/bin/engine_lib.py send_data"
        " '%s' '%s' '%s' '%s'"
        % (
            engine.ssh_id_file,
            engine.cs_dir,
            engine.rickshaw_host,
            os.path.join(engine.archives_dir, "%s-data.tgz" % engine.cs_label),
        )
    )
    engine.do_roadblock(
        "send-data-end", engine.default_timeout, wait_for=send_data_cmd
    )

    logger.info("All engine scripts finished")
    engine.cleanup()


if __name__ == "__main__":
    main()
