#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import argparse
import json
import os
import re
import shutil
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


def dump_params(params, cs_id, engine, ids_to_benchmark):
    """Build the parameter string for a benchmark post-process invocation.

    Port of the Perl dump_params() function.
    """
    default_role = "client"
    benchmark = ids_to_benchmark.get(str(cs_id))
    params_str = ""
    update_count = 0

    for param in params:
        arg = param.get("arg", "")
        val = param.get("val", "")
        bench = param.get("benchmark", "")
        role = param.get("role", default_role)
        param_id = param.get("id")

        # Fix malformed UUIDs stored as 'id'
        if param_id and re.match(
            r'^[A-Z0-9]{1,8}-[A-Z0-9]{1,4}-[A-Z0-9]{1,4}-[A-Z0-9]{1,4}-[A-Z0-9]{1,12}$',
            str(param_id)
        ):
            param["param-uuid"] = param_id
            del param["id"]
            param_id = None
            update_count += 1

        if param_id is not None and cs_id is not None and str(param_id) != str(cs_id):
            continue

        if bench != benchmark:
            continue

        if role != engine and role != "all":
            continue

        if val:
            val = val.replace("%client-id%", str(cs_id)) if cs_id else val
            params_str += f" --{arg}={val}"
        else:
            params_str += f" --{arg}"

    return params_str.lstrip(), update_count


def main():
    parser = argparse.ArgumentParser(
        description="Run benchmark-specific post-processing scripts for a completed rickshaw run"
    )
    parser.add_argument("--base-run-dir", required=True,
                        help="Directory where result data is located")
    parser.add_argument("--log-level", default="normal",
                        choices=["normal", "debug"],
                        help="Logging verbosity")
    args = parser.parse_args()

    logger = setup_logging("rickshaw-post-process-bench", args.log_level)

    base_run_dir = str(Path(args.base_run_dir).resolve())
    config_dir = os.path.join(base_run_dir, "config")
    run_dir = os.path.join(base_run_dir, "run")

    rickshaw_project_dir = str(Path(__file__).resolve().parent)
    bench_schema_file = os.path.join(rickshaw_project_dir, "schema", "benchmark.json")
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
        logger.error("Could not open the rickshaw-run.json file: %s", err)
        sys.exit(1)

    # Load bench configs
    bench_configs = {}
    bench_dirs = {}
    for bench_dir_path in run_data.get("bench-dir", "").split(","):
        bench_config_file = os.path.join(bench_dir_path, "rickshaw.json")
        if not os.path.exists(bench_config_file):
            logger.error("Benchmark config file %s not found", bench_config_file)
            sys.exit(1)
        bench_cfg, cfg_err = load_json_file(bench_config_file)
        if bench_cfg is None:
            logger.error("Could not open bench config file: %s", cfg_err)
            sys.exit(1)
        benchmark_name = bench_cfg.get("benchmark")
        if not benchmark_name:
            logger.error("'benchmark' not defined in %s", bench_config_file)
            sys.exit(1)
        bench_dirs[benchmark_name] = bench_dir_path
        bench_configs[benchmark_name] = bench_cfg

    # Build ID-to-benchmark and benchmark-to-IDs mappings
    ids_to_benchmark = {}
    benchmark_to_ids = {}
    for bench_and_id in run_data.get("bench-ids", "").split(","):
        parts = bench_and_id.split(":", 1)
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
                    ids_to_benchmark[str(i)] = bench
                    benchmark_to_ids.setdefault(bench, []).append(str(i))
            elif re.match(r'^\d+$', id_range):
                ids_to_benchmark[id_range] = bench
                benchmark_to_ids.setdefault(bench, []).append(id_range)
            else:
                logger.warning("ID range not recognized: %s", id_range)

    max_workers = get_max_workers()
    logger.info("Will run a maximum of %d post-processing jobs at a time", max_workers)

    update_run_json = 0
    iterations = run_data.get("iterations", [])
    num_samples = run_data.get("num-samples", 1)

    logger.info("Launching a post-process job for each iteration x sample x [client|server]")

    for i, iteration in enumerate(iterations, start=1):
        iter_dir = os.path.join(run_dir, "iterations", f"iteration-{i}")
        if not os.path.isdir(iter_dir):
            logger.warning("Directory %s does not exist", iter_dir)
            continue

        samp_dirs = sorted([
            d for d in os.listdir(iter_dir)
            if re.match(r'^sample-\d+$', d) and os.path.isdir(os.path.join(iter_dir, d))
        ])

        for samp_dir in samp_dirs:
            samp_path = os.path.join(iter_dir, samp_dir)
            logger.info("Working on iterations/iteration-%d/%s", i, samp_dir)
            jobs = []

            for cs_name in ("client", "server"):
                cs_name_path = os.path.join(samp_path, cs_name)
                if not os.path.isdir(cs_name_path):
                    continue

                for cs_id_str in sorted(os.listdir(cs_name_path)):
                    if not re.match(r'^\d+$', cs_id_str):
                        continue
                    cs_id_path = os.path.join(cs_name_path, cs_id_str)
                    if not os.path.isdir(cs_id_path):
                        continue

                    benchmark = ids_to_benchmark.get(cs_id_str)
                    if not benchmark:
                        logger.warning("No benchmark found for cs_id %s", cs_id_str)
                        continue

                    bench_dir = bench_dirs.get(benchmark, "")
                    pp_cmd = bench_configs[benchmark].get("controller", {}).get("post-script", "")
                    pp_cmd = pp_cmd.replace("%bench-dir%", bench_dir + "/")
                    pp_cmd = pp_cmd.replace("%run-dir%", run_dir + "/")
                    pp_cmd = pp_cmd.replace("%config-dir%", config_dir + "/")

                    iter_params, param_updates = dump_params(
                        iteration.get("params", []), cs_id_str, cs_name, ids_to_benchmark
                    )
                    update_run_json += param_updates

                    if pp_cmd and os.path.exists(pp_cmd):
                        jobs.append({
                            "cs_id_path": cs_id_path,
                            "pp_cmd": pp_cmd,
                            "iter_params": iter_params,
                            "cs_name": cs_name,
                            "cs_id": cs_id_str,
                        })

            if jobs:
                def run_bench_pp(job):
                    full_cmd = (
                        f"cd {job['cs_id_path']} && "
                        f"RS_CS_LABEL={job['cs_name']}-{job['cs_id']} "
                        f"{job['pp_cmd']} {job['iter_params']} "
                        f">post-process-output.txt 2>&1"
                    )
                    logger.debug("Running: %s", full_cmd)
                    _, output, rc = run_cmd(full_cmd)
                    return rc

                results = run_parallel_jobs(jobs, run_bench_pp, max_workers=max_workers)

                for job, result in results:
                    if isinstance(result, Exception):
                        logger.error("Post-processing failed for %s-%s: %s",
                                     job["cs_name"], job["cs_id"], result)
                    elif result != 0:
                        logger.warning("Post-processing for %s-%s returned rc=%d",
                                       job["cs_name"], job["cs_id"], result)

                logger.info("Post-processing complete for %s", samp_dir)

        if len(samp_dirs) < num_samples:
            logger.warning(
                "Iteration %d is considered failed — not enough passing samples (%d < %d)",
                i, len(samp_dirs), num_samples
            )
            fail_dir = iter_dir + "-fail"
            shutil.move(iter_dir, fail_dir)

    if update_run_json > 0:
        logger.debug("Run data changed, writing %s", run_file)
        try:
            with open(run_file, "w") as f:
                json.dump(run_data, f, indent=2, sort_keys=True)
                f.write("\n")
        except OSError as e:
            logger.error("Could not update rickshaw-run file: %s", e)
            sys.exit(1)


if __name__ == "__main__":
    main()
