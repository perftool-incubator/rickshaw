#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python
#
# rickshaw-gen-docs takes the rickshaw-run.json, plus metric data from benchmarks
# and tools, and generates all OpenSearch documents as NDJSON files in
# <result_dir>/run/opensearch, to be later indexed by CommonDataModel.

import argparse
import json
import os
import re
import shutil
import socket
import sys
import uuid as uuid_module
from pathlib import Path

TOOLBOX_HOME = os.environ.get("TOOLBOX_HOME")
if TOOLBOX_HOME:
    sys.path.append(str(Path(TOOLBOX_HOME) / "python"))

RICKSHAW_HOME = os.environ.get("RICKSHAW_HOME")
if RICKSHAW_HOME:
    sys.path.append(RICKSHAW_HOME)

from toolbox.fileio import open_read_text_file, open_write_text_file
from toolbox.json import load_json_file
from toolbox.logging import setup_logging
from toolbox.parallel import run_parallel_jobs, get_max_workers
from rickshaw_lib.schema_fixup import rickshaw_run_schema_fixup

logger = None

SUPPORTED_CDM_VERS = ["v7dev", "v8dev", "v9dev"]


def generate_uuid():
    return str(uuid_module.uuid1()).upper()


def open_text_file(filepath):
    fh, _ = open_read_text_file(filepath)
    return fh


class CDMState:
    def __init__(self, ver="v9dev"):
        self.ver = ver
        self._set_fields()

    def _set_fields(self):
        if self.ver == "v7dev":
            self.run_id_field = "id"
            self.iter_id_field = "id"
            self.samp_id_field = "id"
            self.period_id_field = "id"
            self.metric_id_field = "id"
        else:
            self.run_id_field = "run-uuid"
            self.iter_id_field = "iteration-uuid"
            self.samp_id_field = "sample-uuid"
            self.period_id_field = "period-uuid"
            self.metric_id_field = "metric_desc-uuid"

    def set_version(self, ver):
        self.ver = ver
        self._set_fields()

    def get_index_base_name(self):
        if self.ver in ("v7dev", "v8dev"):
            return f"cdm{self.ver}-"
        elif self.ver == "v9dev":
            return "cdm-v9dev-"
        return f"cdm{self.ver}-"

    def get_index_name(self, doctype, year, month):
        if self.ver in ("v7dev", "v8dev"):
            return doctype
        elif self.ver == "v9dev":
            return f"{doctype}@{year}.{month}"
        return doctype

    def get_index_full_name(self, doctype, year, month):
        return self.get_index_base_name() + self.get_index_name(doctype, year, month)


def add_persistent_uuid(doc, doc_type, cdm):
    old_field = "id"
    new_field = f"{doc_type}-uuid"

    if cdm.ver == "v7dev":
        if old_field not in doc:
            doc[old_field] = generate_uuid()
            return True
        return False

    if new_field in doc:
        return False

    if old_field not in doc:
        doc[new_field] = generate_uuid()
        return True

    if doc_type == "param" and isinstance(doc.get(old_field), (int, str)) and str(doc[old_field]).isdigit():
        doc[new_field] = generate_uuid()
        return True

    doc[new_field] = doc[old_field]
    del doc[old_field]
    return True


def create_es_doc(result, cdm, doc_type, iter_idx=None, sample_idx=None, period_idx=None):
    es_doc = {"cdm": {"ver": cdm.ver}}

    for field in [cdm.run_id_field, "harness", "host", "email", "name", "source", "begin", "end", "benchmark"]:
        if field in result:
            es_doc.setdefault("run", {})[field] = result[field]

    if doc_type == "tag" and iter_idx is not None:
        tag_idx = iter_idx
        tag = result.get("tags", [])[tag_idx]
        es_doc["tag"] = {f: tag[f] for f in ("name", "val") if f in tag}

    elif doc_type == "param" and iter_idx is not None and sample_idx is not None:
        param_idx = sample_idx
        iteration = result["iterations"][iter_idx]
        for f in [cdm.iter_id_field, "status", "primary-metric", "primary-period", "num", "path"]:
            if f in iteration:
                es_doc.setdefault("iteration", {})[f] = iteration[f]
        param = iteration["params"][param_idx]
        es_doc["param"] = {f: param[f] for f in ("arg", "val") if f in param}

    else:
        if iter_idx is not None:
            iteration = result["iterations"][iter_idx]
            for f in [cdm.iter_id_field, "status", "primary-metric", "primary-period", "num", "path"]:
                if f in iteration:
                    es_doc.setdefault("iteration", {})[f] = iteration[f]
            if sample_idx is not None:
                sample = iteration["samples"][sample_idx]
                for f in [cdm.samp_id_field, "status", "num", "path"]:
                    if f in sample:
                        es_doc.setdefault("sample", {})[f] = sample[f]
                if period_idx is not None:
                    period = sample["periods"][period_idx]
                    for f in [cdm.period_id_field, "name", "begin", "end"]:
                        if f in period:
                            es_doc.setdefault("period", {})[f] = period[f]

    return es_doc


def es_doc_to_ndjson(es_doc, cdm, doc_type, year, month):
    index_name = cdm.get_index_full_name(doc_type, year, month)
    header = json.dumps({"index": {"_index": index_name}})
    body = json.dumps(es_doc, sort_keys=True)
    return f"{header}\n{body}\n"


def generate_metrics(cdm, result, metr_dir, metr_file, cstype, csid, base_doc,
                     year, month, benchmark=None, primary_metric=None):
    num_submitted = 0
    earliest_begin = None
    latest_end = None
    pri_earliest_begin = None
    pri_latest_end = None
    primary_metric_found = False
    ndjson = ""

    eng_env_vars = {
        "engine-type": cstype,
        "engine-id": csid,
    }
    if cstype in ("client", "server"):
        eng_env_vars.update({
            "engine-role": "benchmarker",
            "benchmark-role": cstype,
            "benchmark-name": "unknown",
            "benchmark-group": "unknown",
            "tool-name": "unknown",
        })
    elif cstype in ("worker", "master", "profiler"):
        eng_env_vars.update({
            "engine-role": "profiler",
            "benchmark-role": "none",
            "benchmark-name": "none",
            "tool-name": "unknown",
        })

    eng_env_file = os.path.join(metr_dir, "engine-env.txt")
    if not os.path.exists(eng_env_file):
        eng_env_file += ".xz"
    if os.path.exists(eng_env_file):
        varnames = ["HOSTNAME", "tool_name", "engine_type", "engine_role",
                     "benchmark_group", "benchmark_role", "hosted_by",
                     "hypervisor_host", "osruntime", "endpoint_label", "userenv"]
        try:
            fh = open_text_file(eng_env_file)
            for line in fh:
                line = line.strip()
                for varname in varnames:
                    if line.startswith(f"{varname}="):
                        val = line.split("=", 1)[1]
                        key = varname.replace("_", "-")
                        if key == "HOSTNAME":
                            key = "hostname"
                        eng_env_vars[key] = val
            fh.close()
        except OSError:
            pass

    metr_json_file = os.path.join(metr_dir, metr_file + ".json")
    metr_csv_file = os.path.join(metr_dir, metr_file + ".csv")
    if not os.path.exists(metr_json_file):
        metr_json_file += ".xz"
    if not os.path.exists(metr_csv_file):
        metr_csv_file += ".xz"
    if not os.path.exists(metr_json_file) or not os.path.exists(metr_csv_file):
        logger.error("Could not find metric files for %s in %s", metr_file, metr_dir)
        return "", 0, False, None, None

    use_xz = metr_json_file.endswith(".xz")
    metr_data, err = load_json_file(metr_json_file, uselzma=use_xz)
    if metr_data is None:
        logger.error("Could not open metric data file: %s", err)
        sys.exit(1)

    update_metric_file = 0
    for metr in metr_data:
        if add_persistent_uuid(metr, "metric_desc", cdm):
            update_metric_file += 1
    if update_metric_file > 0:
        out_file = metr_json_file.replace(".xz", "") if use_xz else metr_json_file
        with open(out_file, "w") as f:
            json.dump(metr_data, f, indent=2, sort_keys=True)
            f.write("\n")

    uuid_map = {}
    type_map = {}
    source_map = {}
    for metr in metr_data:
        idx = metr["idx"]
        uuid_map[idx] = metr[cdm.metric_id_field]
        type_map[idx] = metr["desc"]["type"]
        source_map[idx] = metr["desc"]["source"]

        metr_desc_doc = dict(base_doc)
        metr_desc_doc["metric_desc"] = dict(metr["desc"])
        metr_desc_doc["metric_desc"][cdm.metric_id_field] = uuid_map[idx]
        if "names" in metr:
            metr_desc_doc["metric_desc"]["names"] = dict(metr["names"])
        else:
            metr_desc_doc["metric_desc"]["names"] = {}
        if "values" in metr:
            metr_desc_doc["metric_desc"]["values"] = metr["values"]

        metr_desc_doc["metric_desc"]["names"]["cstype"] = cstype
        metr_desc_doc["metric_desc"]["names"]["csid"] = csid
        for k, v in eng_env_vars.items():
            metr_desc_doc["metric_desc"]["names"][k] = v
        metr_desc_doc["metric_desc"]["names-list"] = sorted(metr_desc_doc["metric_desc"]["names"].keys())

        ndjson += es_doc_to_ndjson(metr_desc_doc, cdm, "metric_desc", year, month)
        num_submitted += 1

    csv_fh = open_text_file(metr_csv_file)
    for line in csv_fh:
        m = re.match(r'^(\d+),(\d+),(\d+),(.*)$', line.strip())
        if not m:
            continue
        idx, begin, end, value = int(m.group(1)), int(m.group(2)), int(m.group(3)), m.group(4)
        metr_data_doc = {"cdm": {"ver": cdm.ver}}
        metr_data_doc["metric_desc"] = {cdm.metric_id_field: uuid_map[idx]}
        metr_data_doc["metric_data"] = {
            "begin": begin, "end": end, "value": value,
            "duration": end - begin + 1,
        }
        if cdm.ver != "v7dev":
            metr_data_doc["run"] = {cdm.run_id_field: result[cdm.run_id_field]}

        ndjson += es_doc_to_ndjson(metr_data_doc, cdm, "metric_data", year, month)

        if primary_metric and type_map[idx] == primary_metric and source_map[idx] == benchmark:
            primary_metric_found = True
            if pri_earliest_begin is None or pri_earliest_begin > begin:
                pri_earliest_begin = begin
            if pri_latest_end is None or pri_latest_end < end:
                pri_latest_end = end
        else:
            if earliest_begin is None or earliest_begin > begin:
                earliest_begin = begin
            if latest_end is None or latest_end < end:
                latest_end = end
    csv_fh.close()

    if primary_metric_found:
        return ndjson, num_submitted, True, pri_earliest_begin, pri_latest_end
    return ndjson, num_submitted, False, earliest_begin, latest_end


def main():
    global logger

    parser = argparse.ArgumentParser(
        description="Generate OpenSearch documents from rickshaw run data"
    )
    parser.add_argument("--base-run-dir", required=True)
    parser.add_argument("--max-jobs", type=int, default=32)
    parser.add_argument("--ver", default="v9dev", choices=SUPPORTED_CDM_VERS)
    parser.add_argument("--log-level", default="normal", choices=["normal", "debug"])
    args = parser.parse_args()

    logger = setup_logging("rickshaw-gen-docs", args.log_level)

    base_run_dir = str(Path(args.base_run_dir).resolve())
    config_dir = os.path.join(base_run_dir, "config")
    run_dir = os.path.join(base_run_dir, "run")
    tool_data_dir = os.path.join(run_dir, "tool-data")
    doc_dir = os.path.join(run_dir, "opensearch")

    rickshaw_project_dir = str(Path(__file__).resolve().parent)
    result_schema_file = os.path.join(rickshaw_project_dir, "schema", "rickshaw-run.json")

    # Extract year/month from run dir name for CDMv9 index naming
    year = None
    month = None
    m = re.search(r'--(\d{4})-(\d{2})-\d{2}_', base_run_dir)
    if m:
        year = m.group(1)
        month = m.group(2)

    cdm = CDMState(args.ver)

    # Prepare output directory
    if os.path.exists(doc_dir):
        shutil.rmtree(doc_dir)
    os.makedirs(doc_dir)

    # Schema fixup and load
    run_file = os.path.join(run_dir, "rickshaw-run.json")
    fixup_status = rickshaw_run_schema_fixup(run_file, result_schema_file)
    if fixup_status != 0:
        sys.exit(fixup_status)

    result, err = load_json_file(run_file)
    if result is None:
        result, err = load_json_file(run_file + ".xz", uselzma=True)
    if result is None:
        logger.error("Could not open rickshaw-run.json: %s", err)
        sys.exit(1)

    # Add persistent IDs
    update_run_json = 0
    if "iterations" in result:
        for iteration in result["iterations"]:
            if add_persistent_uuid(iteration, "iteration", cdm):
                update_run_json += 1
            for param in iteration.get("params", []):
                if add_persistent_uuid(param, "param", cdm):
                    update_run_json += 1
        if update_run_json > 0:
            with open(run_file, "w") as f:
                json.dump(result, f, indent=2, sort_keys=True)
                f.write("\n")

    if "run-id" in result:
        result["id"] = result.pop("run-id")
    add_persistent_uuid(result, "run", cdm)

    result["source"] = socket.gethostname() + "/" + base_run_dir
    logger.info("Run ID: %s", result[cdm.run_id_field])
    logger.info("Generating OpenSearch documents in %s", doc_dir)

    # Accumulate main process ndjson
    main_ndjson = ""

    # Tool metrics — parallel
    engine_pattern = re.compile(r'^[a-zA-Z0-9]+-\d+-[a-zA-Z0-9-]+-\d+$')
    if os.path.isdir(tool_data_dir):
        base_metric_doc = create_es_doc(result, cdm, "metric_desc")
        jobs = []
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
                logger.info("Tool document generation for %s/%s queued", collector, engine)
                for tool in sorted(os.listdir(engine_dir)):
                    tool_dir_path = os.path.join(engine_dir, tool)
                    if not os.path.isdir(tool_dir_path):
                        continue
                    processed = set()
                    for tf in sorted(os.listdir(tool_dir_path)):
                        m_file = re.match(r'(metric-data-\S+)\.json', tf)
                        if m_file:
                            tool_file = m_file.group(1)
                            if tool_file in processed:
                                continue
                            processed.add(tool_file)
                            logger.debug("Working on tool_file: %s", tool_file)
                            jobs.append({
                                "tool_dir": tool_dir_path,
                                "tool_file": tool_file,
                                "collector": collector,
                                "engine": engine,
                            })

        logger.info("Launching up to %d jobs", args.max_jobs)

        def gen_tool_metrics(job):
            ndjson, _, _, _, _ = generate_metrics(
                cdm, result, job["tool_dir"], job["tool_file"],
                job["collector"], job["engine"], base_metric_doc,
                year, month
            )
            import threading
            tid = threading.current_thread().ident
            out_file = os.path.join(doc_dir, f"{tid}-{job['tool_file']}-docs.ndjson")
            fh, _ = open_write_text_file(out_file)
            fh.write(ndjson)
            fh.close()
            return 0

        if jobs:
            max_workers = min(args.max_jobs, len(jobs))
            run_parallel_jobs(jobs, gen_tool_metrics, max_workers=max_workers)

    # Benchmark metrics — sequential (modifies shared result state)
    if "iterations" in result:
        logger.info("Benchmark document generation starting")
        iter_num = 0
        for iteration in result["iterations"]:
            iter_num += 1
            iter_idx = iter_num - 1
            logger.info("Working on iteration %d", iter_num)
            iteration["samples"] = []
            iteration["num"] = iter_num

            for param_idx in range(len(iteration.get("params", []))):
                es_doc = create_es_doc(result, cdm, "param", iter_idx, param_idx)
                main_ndjson += es_doc_to_ndjson(es_doc, cdm, "param", year, month)

            this_iter_path = f"iteration-{iter_num}"
            this_iter_dir = os.path.join("iterations", this_iter_path)
            full_iter_dir = os.path.join(run_dir, this_iter_dir)

            if os.path.isdir(full_iter_dir):
                iter_status = "pass"
            elif os.path.isdir(full_iter_dir + "-fail"):
                iter_status = "fail"
                this_iter_dir += "-fail"
                full_iter_dir += "-fail"
                logger.info("Not processing iteration %d (failed)", iter_num)
            else:
                iter_status = "missing"
                logger.info("Not processing iteration %d (directory missing)", iter_num)

            iteration["status"] = iter_status
            if iter_status != "pass":
                continue

            primary_metrics = []
            primary_period = None
            samp_dirs = sorted([
                d for d in os.listdir(full_iter_dir)
                if re.match(r'^sample-\d+$', d) and os.path.isdir(os.path.join(full_iter_dir, d))
            ])

            for samp_dir in samp_dirs:
                samp_persist_file = os.path.join(full_iter_dir, samp_dir, "persistent-ids.json")
                create_persist_file = False
                update_persist_file = 0
                samp_persist = None

                if os.path.exists(samp_persist_file) or os.path.exists(samp_persist_file + ".xz"):
                    use_xz = os.path.exists(samp_persist_file + ".xz")
                    samp_persist, _ = load_json_file(
                        samp_persist_file + (".xz" if use_xz else ""), uselzma=use_xz
                    )
                if samp_persist is None:
                    create_persist_file = True
                    samp_persist = {
                        "sample-persistent-ids": {"schema": {"version": "2024.01.20"}},
                        "samples": {},
                        "periods": [],
                    }

                sample_num = int(re.match(r'^sample-(\d+)$', samp_dir).group(1))
                sample_idx = sample_num - 1
                while len(iteration["samples"]) <= sample_idx:
                    iteration["samples"].append({})
                this_sample = iteration["samples"][sample_idx]
                this_sample["status"] = "pass"
                this_sample["num"] = sample_num

                if "id" in samp_persist.get("samples", {}):
                    this_sample[cdm.samp_id_field] = samp_persist["samples"]["id"]
                else:
                    samp_persist.setdefault("samples", {})["id"] = generate_uuid()
                    this_sample[cdm.samp_id_field] = samp_persist["samples"]["id"]
                    update_persist_file += 1

                this_sample["periods"] = []
                this_samp_dir = os.path.join(this_iter_dir, samp_dir)
                full_samp_dir = os.path.join(run_dir, this_samp_dir)
                logger.debug("Working on %s", this_samp_dir)

                for cs_name in ("client", "server"):
                    cs_name_dir = os.path.join(full_samp_dir, cs_name)
                    if not os.path.isdir(cs_name_dir):
                        continue
                    for cs_id_str in sorted(os.listdir(cs_name_dir)):
                        if not re.match(r'^\d+$', cs_id_str):
                            continue
                        cs_id_dir = os.path.join(cs_name_dir, cs_id_str)
                        data_file = os.path.join(cs_id_dir, "post-process-data.json")
                        data, _ = load_json_file(data_file)
                        if data is None:
                            data, _ = load_json_file(data_file + ".xz", uselzma=True)
                        if data is None:
                            if cs_name == "client":
                                logger.error("Could not open client post-process-data.json: %s", cs_id_dir)
                                sys.exit(1)
                            continue

                        pm_str = f"{data['benchmark']}::{data['primary-metric']}"
                        if pm_str not in primary_metrics:
                            primary_metrics.append(pm_str)

                        if primary_period is None and "primary-period" in data:
                            primary_period = data["primary-period"]
                            iteration["primary-period"] = primary_period

                        if "periods" not in data:
                            continue

                        for period_data in data["periods"]:
                            period_idx = None
                            for idx, existing in enumerate(this_sample["periods"]):
                                if existing.get("name") == period_data["name"]:
                                    period_idx = idx
                                    break

                            if period_idx is None:
                                period = {"name": period_data["name"]}
                                for pid_entry in samp_persist.get("periods", []):
                                    if pid_entry["name"] == period["name"]:
                                        period[cdm.period_id_field] = pid_entry["id"]
                                        break
                                if cdm.period_id_field not in period:
                                    new_pid = {"name": period["name"], "id": generate_uuid()}
                                    samp_persist.setdefault("periods", []).append(new_pid)
                                    period[cdm.period_id_field] = new_pid["id"]
                                    update_persist_file += 1
                                if "begin" in period_data:
                                    period["begin"] = period_data["begin"]
                                if "end" in period_data:
                                    period["end"] = period_data["end"]
                                this_sample["periods"].append(period)
                                period_idx = len(this_sample["periods"]) - 1

                            base_metric_doc = create_es_doc(
                                result, cdm, "metric_desc", iter_idx, sample_idx, period_idx
                            )

                            for metric_file_prefix in period_data.get("metric-files", []):
                                metric_dir = os.path.join(run_dir, this_samp_dir, cs_name, cs_id_str)
                                ndjson_chunk, _, pm_found, this_begin, this_end = generate_metrics(
                                    cdm, result, metric_dir, metric_file_prefix,
                                    cs_name, cs_id_str, base_metric_doc,
                                    year, month, data["benchmark"], data["primary-metric"]
                                )
                                main_ndjson += ndjson_chunk

                                p = this_sample["periods"][period_idx]
                                if this_begin is not None:
                                    if "begin" not in p or p["begin"] < this_begin:
                                        p["begin"] = this_begin
                                    if "end" not in p or p["end"] > this_end:
                                        p["end"] = this_end
                                    if "begin" not in result or result["begin"] > p.get("begin", float("inf")):
                                        result["begin"] = p["begin"]
                                    if "end" not in result or result["end"] < p.get("end", 0):
                                        result["end"] = p["end"]

                # Generate period and sample docs
                for period_idx in range(len(this_sample["periods"])):
                    es_doc = create_es_doc(result, cdm, "period", iter_idx, sample_idx, period_idx)
                    main_ndjson += es_doc_to_ndjson(es_doc, cdm, "period", year, month)
                es_doc = create_es_doc(result, cdm, "sample", iter_idx, sample_idx)
                main_ndjson += es_doc_to_ndjson(es_doc, cdm, "sample", year, month)

                if create_persist_file or update_persist_file > 0:
                    with open(samp_persist_file, "w") as f:
                        json.dump(samp_persist, f, indent=2, sort_keys=True)
                        f.write("\n")

            if not primary_metrics:
                logger.error("No primary-metrics found")
                sys.exit(1)
            iteration["primary-metric"] = ",".join(primary_metrics)

            if primary_period is None:
                logger.error("No primary-period found for %s", this_iter_dir)
                sys.exit(1)

            es_doc = create_es_doc(result, cdm, "iteration", iter_idx)
            main_ndjson += es_doc_to_ndjson(es_doc, cdm, "iteration", year, month)

        logger.info("Benchmark data generation complete")

    # Tags
    if "tags" in result:
        for tag_idx in range(len(result["tags"])):
            es_doc = create_es_doc(result, cdm, "tag", tag_idx)
            main_ndjson += es_doc_to_ndjson(es_doc, cdm, "tag", year, month)

    # Run doc
    es_doc = create_es_doc(result, cdm, "run")
    main_ndjson += es_doc_to_ndjson(es_doc, cdm, "run", year, month)

    main_ndjson_file = os.path.join(doc_dir, f"{os.getpid()}-docs.ndjson")
    fh, _ = open_write_text_file(main_ndjson_file)
    fh.write(main_ndjson)
    fh.close()

    logger.info("Generating OpenSearch documents complete")


if __name__ == "__main__":
    main()
