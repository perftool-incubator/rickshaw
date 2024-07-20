#!/usr/bin/python3

"""
Endpoint to run 1 or more engines on 1 or more remotehost systems
"""

import base64
import copy
from fabric import Connection
import jsonschema
import logging
import os
from paramiko import ssh_exception
from pathlib import Path
import queue
import re
import requests
import sys
import tempfile
import threading
import time
import traceback

script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
endpoints_dir = script_dir + "/../"
sys.path.append(endpoints_dir)
import endpoints

TOOLBOX_HOME = os.environ.get('TOOLBOX_HOME')
if TOOLBOX_HOME is None:
    print("This script requires libraries that are provided by the toolbox project.")
    print("Toolbox can be acquired from https://github.com/perftool-incubator/toolbox and")
    print("then use 'export TOOLBOX_HOME=/path/to/toolbox' so that it can be located.")
    exit(1)
else:
    p = Path(TOOLBOX_HOME) / 'python'
    if not p.exists() or not p.is_dir():
        print("ERROR: <TOOLBOX_HOME>/python ('%s') does not exist!" % (p))
        exit(2)
    sys.path.append(str(p))
from toolbox.json import *

endpoint_defaults = {
    "controller-ip-address": None,
    "cpu-partitioning": False,
    "disable-tools": False,
    "host-mounts": [],
    "hypervisor-host": "none",
    "image-cache-size": 9,
    "numa-node": None,
    "osruntime": "podman",
    "user": "root",
    "podman-settings": {},
    "maximum-worker-threads-count": 250
}

def validate():
    """
    Validate the input and return configuration details to the caller

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters

    Returns:
        int: zero for success / non-zero for failure
    """
    endpoints.validate_comment("environment: %s" % (dict(os.environ)))

    endpoints.validate_comment("params: %s" % (endpoints.cli_stream()))

    endpoints.validate_comment("argparse: %s" % (args))

    endpoints.validate_comment("run-file: %s" % (args.run_file))
    endpoints.validate_comment("endpoint-index: %d" % (args.endpoint_index))

    json, err = load_json_file(args.run_file)
    if json is None:
        endpoints.validate_error(err)
        return 1
    endpoints.validate_comment("run-file: %s" % (json))

    valid, err = validate_schema(json, args.rickshaw_dir + "/util/JSON/schema.json")
    if not valid:
        endpoints.validate_error(err)
        return 1

    if args.endpoint_index >= len(json["endpoints"]):
        endpoints.validate_error("endpoint_index %d does not exist in endpoints array from run-file" % (args.endpoint_index))
        return 1

    endpoint_settings = json["endpoints"][args.endpoint_index]
    endpoints.validate_comment("endpoint-settings: %s" % (endpoint_settings))

    valid, err = validate_schema(endpoint_settings, args.rickshaw_dir + "/schema/remotehosts.json")
    if not valid:
        endpoints.validate_error(err)
        return 1

    for endpoint_idx,endpoint in enumerate(json["endpoints"]):
        if endpoint_idx == args.endpoint_index:
            continue

        if json["endpoints"][endpoint_idx]["type"] == "remotehosts":
            endpoints.validate_error("You can only specify one instance of the remotehosts endpoint.  If there is a reason you need multiple remotehosts endpoints then it should be seen as a bug in the remotehosts endpoint.")

    rickshaw_settings, err = load_json_file(args.base_run_dir + "/config/rickshaw-settings.json.xz", uselzma = True)
    if rickshaw_settings is None:
        endpoints.validate_error(err)
        return 1
    endpoints.validate_comment("rickshaw-settings: %s" % rickshaw_settings)

    endpoint_settings = normalize_endpoint_settings(endpoint_settings, rickshaw_settings)
    if endpoint_settings is None:
        return 1
    endpoints.validate_comment("normalized endpoint-settings: %s" % (endpoint_settings))

    benchmark_engine_mapping = endpoints.build_benchmark_engine_mapping(json["benchmarks"])
    endpoints.validate_comment("benchmark-engine-mapping: %s" % (benchmark_engine_mapping))

    engines = dict()
    userenvs = []
    for remote in endpoint_settings["remotes"]:
        for engine in remote["engines"]:
            if engine["role"] == "profiler":
                continue
            if not engine["role"] in engines:
                engines[engine["role"]] = []
            engines[engine["role"]].extend(engine["ids"])

        if not remote["config"]["settings"]["userenv"] in userenvs:
            userenvs.append(remote["config"]["settings"]["userenv"])

    endpoints.validate_comment("engines: %s" % (engines))
    for role in engines.keys():
        endpoints.validate_log("%s %s" % (role, " ".join(map(str, engines[role]))))
        if len(engines[role]) != len(set(engines[role])):
            endpoints.validate_error("There are duplicate IDs present for %s" % (role))
        for engine_id in engines[role]:
            found_engine = False
            for benchmark in benchmark_engine_mapping.keys():
                if engine_id in benchmark_engine_mapping[benchmark]["ids"]:
                    found_engine = True
                    break
            if not found_engine:
                endpoints.validate_error("Could not find a benchmark mapping for engine ID %d" % (engine_id))

    endpoints.validate_comment("userenvs: %s" % (userenvs))
    for userenv in userenvs:
        endpoints.validate_log("userenv %s" % (userenv))

    remotes = dict()
    for remote in endpoint_settings["remotes"]:
        if not remote["config"]["host"] in remotes:
            remotes[remote["config"]["host"]] = dict()
        if not remote["config"]["settings"]["remote-user"] in remotes[remote["config"]["host"]]:
            remotes[remote["config"]["host"]][remote["config"]["settings"]["remote-user"]] = True
    endpoints.validate_comment("remotes: %s" % (remotes))

    debug_output = False
    if args.log_level == "debug":
        debug_output = True
    for remote in remotes.keys():
        for remote_user in remotes[remote].keys():
            try:
                with endpoints.remote_connection(remote, remote_user, validate = True) as c:
                    result = endpoints.run_remote(c, "uptime", validate = True, debug = debug_output)
                    endpoints.validate_comment("remote login verification for %s with user %s: rc=%d and stdout=[%s] annd stderr=[%s]" % (remote, remote_user, result.exited, result.stdout.rstrip('\n'), result.stderr.rstrip('\n')))

                    result = endpoints.run_remote(c, "podman --version", validate = True, debug = debug_output)
                    endpoints.validate_comment("remote podman presence check for %s: rc=%d and stdout=[%s] and stderr=[%s]" % (remote, result.exited, result.stdout.rstrip('\n'), result.stderr.rstrip('\n')))
                    if result.exited != 0:
                        result = endpoints.run_remote(c, "yum install -y podman", validate = True, debug = debug_output)
                        endpoints.validate_comment("remote podman installation for %s: rc=%d" % (remote, result.exited))
                        if result.exited != 0:
                            endpoints.validate_error("Could not install podman to remote %s" % (remote))
                            endpoints.validate_error("stdout:\n%s" % (result.stdout))
                            endpoints.validate_error("stderr:\n%s" % (result.stderr))
            except ssh_exception.AuthenticationException as e:
                endpoints.validate_error("remote login verification for %s with user %s resulted in an authentication exception '%s'" % (remote, remote_user, str(e)))
            except ssh_exception.NoValidConnectionsError as e:
                endpoints.validate_error("remote login verification for %s with user %s resulted in an connection exception '%s'" % (remote, remote_user, str(e)))

    return 0

def init_settings():
    """
    Initialize the basic settings that are used throughout the script

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Initializing settings based on CLI parameters")

    settings["dirs"] = dict()

    settings["dirs"]["local"] = {
        "base": args.base_run_dir,
        "conf": args.base_run_dir + "/config",
        "run": args.base_run_dir + "/run"
    }
    settings["dirs"]["local"]["engine"] = settings["dirs"]["local"]["run"] + "/engine"
    settings["dirs"]["local"]["endpoint"] = settings["dirs"]["local"]["run"] + "/endpoint/" + args.endpoint_label
    settings["dirs"]["local"]["sysinfo"] = settings["dirs"]["local"]["run"] + "/sysinfo/endpoint/" + args.endpoint_label
    settings["dirs"]["local"]["tool-cmds"] = settings["dirs"]["local"]["conf"] + "/tool-cmds"
    settings["dirs"]["local"]["engine-conf"] = settings["dirs"]["local"]["conf"] + "/engine"
    settings["dirs"]["local"]["engine-cmds"] = settings["dirs"]["local"]["engine-conf"] + "/bench-cmds"
    settings["dirs"]["local"]["engine-logs"] = settings["dirs"]["local"]["engine"] + "/logs"
    settings["dirs"]["local"]["roadblock-msgs"] = settings["dirs"]["local"]["endpoint"] + "/roadblock-msgs"

    remote_base = "/var/lib/crucible"
    settings["dirs"]["remote"] = {
        "base": remote_base,
        "run": remote_base + "/" + args.endpoint_label + "_" + args.run_id
    }
    settings["dirs"]["remote"]["cfg"] = settings["dirs"]["remote"]["run"] + "/cfg"
    settings["dirs"]["remote"]["logs"] = settings["dirs"]["remote"]["run"] + "/logs"
    settings["dirs"]["remote"]["data"] = settings["dirs"]["remote"]["run"] + "/data"
    settings["dirs"]["remote"]["sysinfo"] = settings["dirs"]["remote"]["run"] + "/sysinfo"
    settings["dirs"]["remote"]["tmp"] = settings["dirs"]["remote"]["data"] + "/tmp"

    log_settings(mode = "dirs")

    log.info("Initializing misc settings")

    settings["misc"] = dict()

    log.info("Creating image map")
    settings["misc"]["image-map"] = dict()
    images = args.images.split(",")
    for image in images:
        image_split = image.split("::")
        settings["misc"]["image-map"][image_split[0]] = image_split[1]

    log_settings(mode = "misc")

    return 0

def log_settings(mode = "all"):
    """
    Log the current requested contents of the settings data structure

    Args:
        mode (str): which piece of the settings dict to log

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        None
    """
    match mode:
        case "benchmark-mapping":
            return log.info("settings[benchmark-mapping]:\n%s" % (endpoints.dump_json(settings["engines"]["benchmark-mapping"])), stacklevel = 2)
        case "engines":
            return log.info("settings[engines]:\n%s" % (endpoints.dump_json(settings["engines"])), stacklevel = 2)
        case "misc":
            return log.info("settings[misc]:\n%s" % (endpoints.dump_json(settings["misc"])), stacklevel = 2)
        case "dirs":
            return log.info("settings[dirs]:\n%s" % (endpoints.dump_json(settings["dirs"])), stacklevel = 2)
        case "endpoint":
            return log.info("settings[endpoint]:\n%s" % (endpoints.dump_json(settings["run-file"]["endpoints"][args.endpoint_index])), stacklevel = 2)
        case "rickshaw":
            return log.info("settings[rickshaw]:\n%s" % (endpoints.dump_json(settings["rickshaw"])), stacklevel = 2)
        case "run-file":
            return log.info("settings[run-file]:\n%s" % (endpoints.dump_json(settings["run-file"])), stacklevel = 2)
        case "all" | _:
            return log.info("settings:\n%s" % (endpoints.dump_json(settings)), stacklevel = 2)

def create_local_dirs():
    """
    Create the basic local directories

    Args:
        None

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0
    """
    log.info("Creating local directories")
    endpoints.my_make_dirs(settings["dirs"]["local"]["run"])
    endpoints.my_make_dirs(settings["dirs"]["local"]["engine-logs"])
    endpoints.my_make_dirs(settings["dirs"]["local"]["roadblock-msgs"])
    endpoints.my_make_dirs(settings["dirs"]["local"]["sysinfo"])
    return 0

def load_settings():
    """
    Load settings from config multiple config files

    Args:
        None

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0
    """
    log.info("Loading settings from config files")

    rickshaw_settings_file = settings["dirs"]["local"]["conf"] + "/rickshaw-settings.json.xz"
    settings["rickshaw"],err = load_json_file(rickshaw_settings_file, uselzma = True)
    if settings["rickshaw"] is None:
        log.error("Failed to load rickshaw-settings from %s with error '%s'" % (rickshaw_settings_file, err))
        return 1
    else:
        log.info("Loaded rickshaw-settings from %s" % (rickshaw_settings_file))

    log_settings(mode = "rickshaw")

    settings["run-file"],err = load_json_file(args.run_file)
    if settings["run-file"] is None:
        log.error("Failed to load run-file from %s with error '%s'" % (args.run_file, err))
        return 1
    else:
        log.info("Loaded run-file from %s" % (args.run_file))

    valid, err = validate_schema(settings["run-file"], args.rickshaw_dir + "/util/JSON/schema.json")
    if not valid:
        log.error("JSON validation failed for run-file")
        return 1
    else:
        log.info("First level JSON validation for run-file passed")

    valid, err = validate_schema(settings["run-file"]["endpoints"][args.endpoint_index], args.rickshaw_dir + "/schema/remotehosts.json")
    if not valid:
        log.error("JSON validation failed for remotehosts endpoint at index %d in run-file" % (args.endpoint_index))
        return 1
    else:
        log.info("Endpoint specific JSON validation for remotehosts endpoint at index %d in run-file passed" % (args.endpoint_index))

    log_settings(mode = "run-file")

    log.info("Normalizing endpoint settings")
    settings["run-file"]["endpoints"][args.endpoint_index] = normalize_endpoint_settings(endpoint = settings["run-file"]["endpoints"][args.endpoint_index], rickshaw = settings["rickshaw"])
    log_settings(mode = "endpoint")

    log.info("Building benchmark engine mapping")
    if not "engines" in settings:
        settings["engines"] = dict()
    settings["engines"]["benchmark-mapping"] = endpoints.build_benchmark_engine_mapping(settings["run-file"]["benchmarks"])
    log_settings(mode = "benchmark-mapping")

    log.info("Loading SSH private key into misc settings")
    settings["misc"]["ssh-private-key"] = ""
    try:
        with open(settings["dirs"]["local"]["conf"] + "/rickshaw_id.rsa", "r", encoding = "ascii") as ssh_private_key:
            for line in ssh_private_key:
                line = re.sub(r"\n", r"\\n", line)
                settings["misc"]["ssh-private-key"] += line
    except IOError as e:
        log.error("Failed to load the SSH private key [%s]" % (e))
        return 1

    log_settings(mode = "misc")

    return 0

def normalize_endpoint_settings(endpoint, rickshaw):
    """
    Normalize the endpoint settings by determining where default settings need to be applied and expanding ID ranges

    Args:
        endpoint (dict): The specific endpoint dictionary from the run-file that this endpoint instance is handling
        rickshaw (dict): The rickshaw settings dictionary

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance
        endpoint_defaults (dict): the endpoint defaults

    Returns:
        endpoint (dict): The normalized endpoint dictionary
    """
    defaults = {
        "controller-ip-address": endpoint_defaults["controller-ip-address"],
        "cpu-partitioning": endpoint_defaults["cpu-partitioning"],
        "disable-tools": endpoint_defaults["disable-tools"],
        "host-mounts": endpoint_defaults["host-mounts"],
        "hypervisor-host": endpoint_defaults["hypervisor-host"],
        "image-cache-size": endpoint_defaults["image-cache-size"],
        "numa-node": endpoint_defaults["numa-node"],
        "osruntime": endpoint_defaults["osruntime"],
        "remote-user": endpoint_defaults["user"],
        "podman-settings": endpoint_defaults["podman-settings"],
        "userenv": rickshaw["userenvs"]["default"]["benchmarks"]
    }

    if "settings" in endpoint:
        for key in defaults.keys():
            if key in endpoint["settings"]:
                defaults[key] = endpoint["settings"][key]

    cached_controller_ips = dict()
    for remote in endpoint["remotes"]:
        if not "settings" in remote["config"]:
            remote["config"]["settings"] = dict()

        for key in defaults.keys():
            if not key in remote["config"]["settings"]:
                remote["config"]["settings"][key] = defaults[key]

        if remote["config"]["settings"]["controller-ip-address"] is None:
            if remote["config"]["host"] in cached_controller_ips:
                remote["config"]["settings"]["controller-ip-address"] = cached_controller_ips[remote["config"]["host"]]
            else:
                try:
                    remote["config"]["settings"]["controller-ip-address"] = endpoints.get_controller_ip(remote["config"]["host"])
                    cached_controller_ips[remote["config"]["host"]] = remote["config"]["settings"]["controller-ip-address"]
                except ValueError as e:
                    msg = "While determining controller IP address for remote '%s' encountered exception '%s'" % (remote["config"]["host"], str(e))
                    if args.validate:
                        endpoints.validate_error(msg)
                    else:
                        log.error(msg)
                    return None

        for engine in remote["engines"]:
            if engine["role"] == "profiler":
                continue
            try:
                engine["ids"] = endpoints.expand_ids(engine["ids"])
            except ValueError as e:
                msg = "While expanding '%s' encountered exception '%s'" % (engine["ids"], str(e))
                if args.validate:
                    endpoints.validate_error(msg)
                else:
                    log.error(msg)
                return None

    return endpoint

def check_base_requirements():
    """
    Check if the base requirements to perform a non-validation run were provided

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Checking base requirements")

    if args.run_id == "":
        log.error("The run ID was not provided")
        return 1
    else:
        log.info("run-id: %s" % (args.run_id))

    path = Path(settings["dirs"]["local"]["engine-cmds"] + "/client/1")
    if not path.is_dir():
        log.error("client-1 bench command directory not found [%s]" % (path))
        return 1
    else:
        log.info("client-1 bench command directory found [%s]" % (path))

    return 0

def build_unique_remote_configs():
    """
    Process endpoint settings and determine the unique remotes and populate them with the appropriate tools engines

    Args:
        None

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Building unique remote configs")

    if not "engines" in settings:
        settings["engines"] = dict()
    settings["engines"]["remotes"] = dict()
    settings["engines"]["profiler-mapping"] = dict()
    settings["engines"]["new-followers"] = []

    for remote_idx,remote in enumerate(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"]):
        if not remote["config"]["host"] in settings["engines"]["remotes"]:
            settings["engines"]["remotes"][remote["config"]["host"]] = {
                "first-engine": None,
                "roles": dict(),
                "run-file-idx": [],
                "disable-tools": None,
                "engines": []
            }

        if settings["engines"]["remotes"][remote["config"]["host"]]["disable-tools"] is None:
            settings["engines"]["remotes"][remote["config"]["host"]]["disable-tools"] = remote["config"]["settings"]["disable-tools"]
        elif settings["engines"]["remotes"][remote["config"]["host"]]["disable-tools"] != remote["config"]["settings"]["disable-tools"]:
            raise ValueError("Conflicting values for disable-tools for remote %s" % (remote["config"]["host"]))

        settings["engines"]["remotes"][remote["config"]["host"]]["run-file-idx"].append(remote_idx)

        for engine in remote["engines"]:
            if not engine["role"] in settings["engines"]["remotes"][remote["config"]["host"]]["roles"]:
                settings["engines"]["remotes"][remote["config"]["host"]]["roles"][engine["role"]] = {
                    "ids": []
                }
            if engine["role"] != "profiler":
                settings["engines"]["remotes"][remote["config"]["host"]]["roles"][engine["role"]]["ids"].extend(engine["ids"])

    for remote in settings["engines"]["remotes"].keys():
        for role in settings["engines"]["remotes"][remote]["roles"].keys():
            if "ids" in settings["engines"]["remotes"][remote]["roles"][role]:
                settings["engines"]["remotes"][remote]["roles"][role]["ids"].sort()

    for remote in settings["engines"]["remotes"].keys():
        for role in [ "client", "server" ]:
            if settings["engines"]["remotes"][remote]["first-engine"] is None and role in settings["engines"]["remotes"][remote]["roles"] and len(settings["engines"]["remotes"][remote]["roles"][role]["ids"]) > 0:
                settings["engines"]["remotes"][remote]["first-engine"] = {
                    "role": role,
                    "id": settings["engines"]["remotes"][remote]["roles"][role]["ids"][0]
                }
                break

        if settings["engines"]["remotes"][remote]["first-engine"] is None:
            settings["engines"]["remotes"][remote]["first-engine"] = {
                "role": "profiler",
                "id": None
            }

    profiler_count = 0
    for remote in settings["engines"]["remotes"].keys():
        if settings["engines"]["remotes"][remote]["disable-tools"]:
            continue

        tools = []
        try:
            tool_cmd_dir = settings["engines"]["remotes"][remote]["first-engine"]["role"]
            if tool_cmd_dir != "profiler":
                tool_cmd_dir += "/" + str(settings["engines"]["remotes"][remote]["first-engine"]["id"])
            with open(settings["dirs"]["local"]["tool-cmds"] + "/" + tool_cmd_dir + "/start") as tool_cmd_file:
                for line in tool_cmd_file:
                    split_line = line.split(":")
                    tools.append(split_line[0])
        except IOError as e:
            log.error("Failed to load the start tools command file from %s" % (tool_cmd_dir))
            return 1

        profiler_count += 1
        for tool in tools:
            profiler_id = args.endpoint_label + "-" + tool + "-" + str(profiler_count)

            if not "profiler" in settings["engines"]["remotes"][remote]["roles"]:
                settings["engines"]["remotes"][remote]["roles"]["profiler"] = {
                    "ids": []
                }

            settings["engines"]["remotes"][remote]["roles"]["profiler"]["ids"].append(profiler_id)

            settings["engines"]["new-followers"].append("profiler-" + profiler_id)

            if not tool in settings["engines"]["profiler-mapping"]:
                settings["engines"]["profiler-mapping"][tool] = {
                    "name": tool,
                    "ids": []
                }
            settings["engines"]["profiler-mapping"][tool]["ids"].append(profiler_id)

    for remote in settings["engines"]["remotes"].keys():
        for role in settings["engines"]["remotes"][remote]["roles"].keys():
            for id in settings["engines"]["remotes"][remote]["roles"][role]["ids"]:
                engine_name = "%s-%s" % (role, id)
                settings["engines"]["remotes"][remote]["engines"].append(engine_name)

    log_settings(mode = "engines")

    log.info("Adding new profiler engines to endpoint settings")
    for remote in settings["engines"]["remotes"].keys():
        if not "profiler" in settings["engines"]["remotes"][remote]["roles"]:
            continue

        settings["engines"]["remotes"][remote]["run-file-idx"].sort()
        run_file_idx = settings["engines"]["remotes"][remote]["run-file-idx"][0]

        profiler_role_idx = None
        for engine_idx,engine in enumerate(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][run_file_idx]["engines"]):
            if engine["role"] == "profiler":
                profiler_role_idx = engine_idx
                engine["ids"] = []
        if profiler_role_idx is None:
            profiler_role = {
                "role": "profiler",
                "ids": []
            }
            settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][run_file_idx]["engines"].append(profiler_role)
            profiler_role_idx = len(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][run_file_idx]["engines"]) - 1
        settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][run_file_idx]["engines"][profiler_role_idx]["ids"].extend(settings["engines"]["remotes"][remote]["roles"]["profiler"]["ids"])

    log_settings(mode = "endpoint")

    return 0

def get_profiler(profiler_id):
    """
    Get the profiler that a specific profiler engine should be running from the profiler mapping

    Args:
        profiler_id (str): A tool/profiler engine's ID

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        str: The name of the tool to run if the profiler_id is found in the mapping
        or
        None: If the profiler_id is not found in the mapping
    """
    for profiler_key in settings["engines"]["profiler-mapping"].keys():
        if profiler_id in settings["engines"]["profiler-mapping"][profiler_key]["ids"]:
            return settings["engines"]["profiler-mapping"][profiler_key]["name"]

    return None

def get_benchmark(benchmark_id):
    """
    Get the benchmark that a specific benchmark engine should be running from the benchmark mapping

    Args:
        benchmark_id (str): A benchmark engine's ID

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        str: The name of the benchmark to run if the benchmark_id is found in the mapping
        or
        None: If the benchmark)id is not found in the mapping
    """
    for benchmark_key in settings["engines"]["benchmark-mapping"].keys():
        if benchmark_id in settings["engines"]["benchmark-mapping"][benchmark_key]["ids"]:
            return settings["engines"]["benchmark-mapping"][benchmark_key]["name"]

    return None

def get_image(image_id):
    """
    Get the image that is used to run a specific benchmark/tool

    Args:
        image_id (str): The tool or benchmark whose container image is being asked for

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        str: The container image that is used to run the specified tool or benchmark
        or
        None: If no matching container image can be located
    """
    for image_key in settings["misc"]["image-map"].keys():
        if image_id == image_key:
            return settings["misc"]["image-map"][image_key]

    return None

def image_pull_worker_thread(thread_id, work_queue, threads_rcs):
    """
    Worker thread to consume and perform image pull jobs for a unique remote

    Args:
        thread_id (int): The specifc worker thread that this is
        work_queue (Queue): The work queue to pull jobs to process from
        threads_rcs (list): The list to record the threads return code in

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread = threading.current_thread()
    thread_name = thread.name
    thread_logger(thread_name, "Starting image pull thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote = None
        try:
            remote = work_queue.get(block = False)
        except queue.Empty:
            thread_logger(thread_name, "Received a work queue empty exception")
            break

        if remote is None:
            thread_logger(thread_name, "Received a null job", log_level = "warning")
            continue

        job_count += 1
        thread_logger(thread_name, "Retrieved remote %s" % (remote))

        my_unique_remote = settings["engines"]["remotes"][remote]
        my_run_file_remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][my_unique_remote["run-file-idx"][0]]

        thread_logger(thread_name, "Remote user is %s" % (my_run_file_remote["config"]["settings"]["remote-user"]), remote_name = remote)

        with endpoints.remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as c:
            for image in my_unique_remote["images"]:
                result = endpoints.run_remote(c, "podman pull " + image)
                loglevel = "info"
                if result.exited != 0:
                    loglevel = "error"
                thread_logger(thread_name, "Attempted to pull %s with return code %d:\nstdout:\n%sstderr:\n%s" % (image, result.exited, result.stdout, result.stderr), log_level = loglevel, remote_name = remote)
                rc += result.exited

                result = endpoints.run_remote(c, "echo '" + image + " " + str(int(time.time())) + " " + args.run_id + "' >> " + settings["dirs"]["remote"]["base"] + "/remotehosts-container-image-census")
                loglevel = "info"
                if result.exited != 0:
                    loglevel = "error"
                thread_logger(thread_name, "Recorded usage for %s in the census with return code %d:\nstdout:\n%sstderr:\n%s" % (image, result.exited, result.stdout, result.stderr), log_level = loglevel, remote_name = remote)
                rc += result.exited

        thread_logger(thread_name, "Notifying work queue that job processing is complete", remote_name = remote)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger(thread_name, "Stopping image pull thread after processing %d job(s)" % (job_count))
    return

def thread_logger(thread_id, msg, log_level = "info", remote_name = None, engine_name = None, log_prefix = None):
    """
    Logging function with specific metadata the identifies the thread/remote/engine the message should be associated with

    Args:
        thread_id (str): An identifier of which thread is logging the message
        msg (str): The message to log
        log_level (str): Which logging level to use for the message
        remote_name (str): An optional remote identifier to label the message with
        engine_name (str): An optional engine identifier to label the message with
        log_prefix (str): An optional prefix to apply to the message

    Globals:
        log: a logger instance

    Returns:
        None
    """
    thread_label = "[Thread %s]" % (thread_id)
    remote_label = ""
    if not remote_name is None:
        remote_label = "[Remote %s]" % (remote_name)
    engine_label = ""
    if not engine_name is None:
        engine_label = "[Engine %s]" % (engine_name)
    prefix_label =  ""
    if not log_prefix is None:
        prefix_label = "[%s]" % (log_prefix)
    msg = "%s%s%s%s %s" % (thread_label, remote_label, engine_label, prefix_label, msg)

    match log_level:
        case "debug":
            return log.debug(msg, stacklevel = 2)
        case "error":
            return log.error(msg, stacklevel = 2)
        case "info":
            return log.info(msg, stacklevel = 2)
        case "warning":
            return log.warning(msg, stacklevel = 2)
        case _:
            raise ValueError("Uknown log_level '%s' in thread_logger" % (log_level))

def get_engine_id_image(role, id):
    """
    Get the image associated with a specific engine role and ID

    Args:
        role (str): The engine's role
        id (str, int): The engine's ID

    Globals:
        None

    Returns:
        image (str): The container image to use for the specified engine
        or
        None: If no container image was located for the specified engine
    """

    image = None
    match role:
        case "profiler":
            image_role = get_profiler(id)
        case _:
            image_role = get_benchmark(id)
    if not image_role is None:
        image = get_image(image_role)
    return image

def create_thread_pool(description, acronym, work, worker_threads_count, worker_thread_function):
    """
    A generic framework for creating thread pools to process queued jobs using a user provided job consuming function

    Args:
        description (str): What the thread will be doing in detail
        acronym (str): A short identifier for the type of work the threads will be doing
        work (Queue): The work queue to pull jobs from that will be passed to the worker threads
        worker_threads_count (int): The number of worker threads to create
        worker_thread_function (func): The function to use as the worker thread

    Globals:
        endpoint_defaults (dict): the endpoint defaults

    Returns:
        None
    """
    thread_logger("MAIN", "Creating thread pool")
    thread_logger("MAIN", "Thread pool description: %s" % (description))
    thread_logger("MAIN", "Thread pool acronym: %s" % (acronym))
    thread_logger("MAIN", "Thread pool size: %d" % (worker_threads_count))

    if not endpoint_defaults["maximum-worker-threads-count"] is None and worker_threads_count > endpoint_defaults["maximum-worker-threads-count"]:
        thread_logger("MAIN", "Reducing thread pool size to %d in accordance with maximum thread pool size definition" % (endpoint_defaults["maximum-worker-threads-count"]))
        worker_threads_count = endpoint_defaults["maximum-worker-threads-count"]

    worker_threads = [None] * worker_threads_count
    worker_threads_rcs = [None] * worker_threads_count

    thread_logger("MAIN", "Launching %d %s (%s)" % (worker_threads_count, description, acronym))
    for thread_id in range(0, worker_threads_count):
        if work.empty():
            thread_logger("MAIN", "Aborting %s launch because no more work to do" % (acronym))
            break
        thread_name = "%s-%d" % (acronym, thread_id)
        thread_logger("MAIN", "Creating and starting thread %s" % (thread_name))
        try:
            worker_threads[thread_id] = threading.Thread(target = worker_thread_function, args = (thread_id, work, worker_threads_rcs), name = thread_name)
            worker_threads[thread_id].start()
        except RuntimeError as e:
            thread_logger("MAIN", "Failed to create and start thread %s due to exception '%s'" % (thread_name, str(e)), log_level = "error")

    thread_logger("MAIN", "Waiting for all %s work jobs to be consumed" % (acronym))
    work.join()
    thread_logger("MAIN", "All %s work jobs have been consumed" % (acronym))

    thread_logger("MAIN", "Joining %s" % (acronym))
    for thread_id in range(0, worker_threads_count):
        thread_name = "%s-%d" % (acronym, thread_id)
        if not worker_threads[thread_id] is None:
            if not worker_threads[thread_id].native_id is None:
                worker_threads[thread_id].join()
                thread_logger("MAIN", "Joined thread %s" % (thread_name))
            else:
                thread_logger("MAIN", "Skipping join of thread %s because it was not started" % (thread_name), log_level = "warning")
        else:
            thread_logger("MAIN", "Skipping join of thread %s because it does not exist" % (thread_name), log_level = "warning")

    thread_logger("MAIN", "Return codes for each %s:\n%s" % (acronym, endpoints.dump_json(worker_threads_rcs)))

    return

def remotes_pull_images():
    """
    Handle the pulling of images necessary to run the test to the remotes

    Args:
        None

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Determining which images to pull to which remotes")
    for remote in settings["engines"]["remotes"].keys():
        if not "images" in settings["engines"]["remotes"][remote]:
            settings["engines"]["remotes"][remote]["images"] = []

        for role in settings["engines"]["remotes"][remote]["roles"].keys():
            for id in settings["engines"]["remotes"][remote]["roles"][role]["ids"]:
                image = get_engine_id_image(role, id)
                if image is None:
                    log.error("Could not find image for remote %s with role %s and id %d" % (remote, role, str(id)))
                else:
                    settings["engines"]["remotes"][remote]["images"].append(image)

        settings["engines"]["remotes"][remote]["images"] = list(set(settings["engines"]["remotes"][remote]["images"]))

    log_settings(mode = "engines")

    image_pull_work = queue.Queue()
    for remote in settings["engines"]["remotes"].keys():
        image_pull_work.put(remote)
    worker_threads_count = len(settings["engines"]["remotes"])

    create_thread_pool("Image Pull Worker Threads", "IPWT", image_pull_work, worker_threads_count, image_pull_worker_thread)

    return 0

def remote_mkdirs_worker_thread(thread_id, work_queue, threads_rcs):
    """
    Worker thread to consume and perform directory creation jobs for a unique remote

    Args:
        thread_id (int): The specifc worker thread that this is
        work_queue (Queue): The work queue to pull jobs to process from
        threads_rcs (list): The list to record the threads return code in

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread = threading.current_thread()
    thread_name = thread.name
    thread_logger(thread_name, "Starting remote mkdirs thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote = None
        try:
            remote = work_queue.get(block = False)
        except queue.Empty:
            thread_logger(thread_name, "Received a work queue empty exception")
            break

        if remote is None:
            thread_logger(thread_name, "Received a null job", log_level = "warning")
            continue

        job_count += 1
        thread_logger(thread_name, "Retrieved remote %s" % (remote))

        my_unique_remote = settings["engines"]["remotes"][remote]
        my_run_file_remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][my_unique_remote["run-file-idx"][0]]

        with endpoints.remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as con:
            for remote_dir in settings["dirs"]["remote"].keys():
                result = endpoints.run_remote(con, "mkdir --parents --verbose " + settings["dirs"]["remote"][remote_dir])
                thread_logger(thread_name, "Remote attempted to mkdir %s with return code %d:\nstdout:\n%sstderr:\n%s" % (settings["dirs"]["remote"][remote_dir], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)
                rc += result.exited

        thread_logger(thread_name, "Notifying work queue that job processing is complete", remote_name = remote)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger(thread_name, "Stopping remote mkdirs thread after processing %d job(s)" % (job_count))
    return

def create_remote_dirs():
    """
    Handle the creation of directories on the remotes

    Args:
        None

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    remote_mkdirs_work = queue.Queue()
    for remote in settings["engines"]["remotes"].keys():
        remote_mkdirs_work.put(remote)
    worker_threads_count = len(settings["engines"]["remotes"])

    create_thread_pool("Remote Mkdir Worker Threads", "RMWT", remote_mkdirs_work, worker_threads_count, remote_mkdirs_worker_thread)

    return 0

def set_total_cpu_partitions():
    """
    Determine the total cpu partitions that each unique remote should be hosting

    Args:
        None

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Setting the total cpu-partitions per unique remote")
    for remote in settings["engines"]["remotes"].keys():
        settings["engines"]["remotes"][remote]["total-cpu-partitions"] = 0
        settings["engines"]["remotes"][remote]["cpu-partitions-idx"] = 0
        settings["engines"]["remotes"][remote]["cpu-partitions-idx-lock"] = threading.Lock()

        for run_file_idx in settings["engines"]["remotes"][remote]["run-file-idx"]:
            if settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][run_file_idx]["config"]["settings"]["cpu-partitioning"]:
                for engine in settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][run_file_idx]["engines"]:
                    if engine["role"] == "profiler":
                        continue
                    settings["engines"]["remotes"][remote]["total-cpu-partitions"] += len(engine["ids"])

    log_settings(mode = "engines")

    return 0

def create_podman(thread_name, remote_name, engine_name, container_name, connection, remote, controller_ip, role, image, cpu_partitioning, numa_node, host_mounts, podman_settings):
    """
    Using an existing connection create a podman pod for use as a podman pod at runtime

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created
        container_name (str): The name to use for the pod on the remote
        connection (Fabric): The Fabric connection to use to run commands on the remote
        remote (str): The remote that is being used for this action
        controller_ip (str): The controller IP address to tell the engine so it can talk to the controller
        role (str): What is the specific role of this engine
        image (str): The container image to use for the engine
        cpu_partitioning (int): Whether to use cpu-partitioning for thie engine or not
        numa_node (int): The NUMA node to bind this engine to
        host_mounts (list): The user requested host directories to bind mount into the engine
        podman-settings (dict): settings specific to only podman

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread_logger(thread_name, "Running create podman", remote_name = remote_name, engine_name = engine_name)

    local_env_file_name = settings["dirs"]["local"]["endpoint"] + "/" + engine_name + "_env.txt"
    thread_logger(thread_name, "Creating env file %s" % (local_env_file_name), remote_name = remote_name, engine_name = engine_name)
    with open(local_env_file_name, "w", encoding = "ascii") as env_file:
        env_file.write("base_run_dir=" + settings["dirs"]["local"]["base"] + "\n")
        env_file.write("cs_label=" + engine_name + "\n")
        env_file.write("cpu_partitioning=" + str(cpu_partitioning) + "\n")
        if cpu_partitioning == 1:
            settings["engines"]["remotes"][remote]["cpu-partitions-idx-lock"].acquire()
            cpu_partition_idx = settings["engines"]["remotes"][remote]["cpu-partitions-idx"]
            settings["engines"]["remotes"][remote]["cpu-partitions-idx"] += 1
            settings["engines"]["remotes"][remote]["cpu-partitions-idx-lock"].release()

            thread_logger(thread_name, "Allocated cpu-partition index %d" % (cpu_partition_idx), remote_name = remote_name, engine_name = engine_name)
            env_file.write("cpu_partition_index=" + str(cpu_partition_idx) + "\n")
            env_file.write("cpu_partitions=" + str(settings["engines"]["remotes"][remote]["total-cpu-partitions"]) + "\n")
        if role != "profiler":
            env_file.write("disable_tools=1" + "\n")
        env_file.write("endpoint=remotehosts" + "\n")
        env_file.write("endpoint_run_dir=" + settings["dirs"]["local"]["endpoint"] + "\n")
        env_file.write("engine_script_start_timeout=" + str(args.engine_script_start_timeout) + "\n")
        env_file.write("max_sample_failures=" + str(args.max_sample_failures) + "\n")
        env_file.write("max_rb_attempts=" + str(args.max_rb_attempts) + "\n")
        env_file.write("rickshaw_host=" + controller_ip + "\n")
        env_file.write("roadblock_id=" + args.roadblock_id + "\n")
        env_file.write("roadblock_passwd=" + args.roadblock_passwd + "\n")
        env_file.write("ssh_id=" + settings["misc"]["ssh-private-key"] + "\n")

    remote_env_file_name = settings["dirs"]["remote"]["cfg"] + "/" + engine_name + "_env.txt"
    result = connection.put(local_env_file_name, remote_env_file_name)
    thread_logger(thread_name, "Copied %s to %s:%s" % (local_env_file_name, remote, remote_env_file_name), remote_name = remote_name, engine_name = engine_name)

    mandatory_mounts = [
        {
            "src": settings["dirs"]["remote"]["data"],
            "dest": "/tmp"
        },
        {
            "src": "/lib/firmware"
        },
        {
            "src": "/lib/modules"
        },
        {
            "src": "/usr/src"
        }
    ]

    create_cmd = [ "podman",
                   "create",
                   "--name=" + container_name,
                   "--env-file=" + remote_env_file_name,
                   "--privileged",
                   "--pid=host",
                   "--net=host",
                   "--security-opt=label=disable" ]

    if "device" in podman_settings:
        create_cmd.append("--device " + podman_settings["device"])

    if "shm-size" in podman_settings:
        create_cmd.append("--shm-size " + podman_settings["shm-size"])
    else:
        create_cmd.append("--ipc=host") # only works when not using shm

    for mount in mandatory_mounts + host_mounts:
        if not "dest" in mount:
            mount["dest"] = mount["src"]

        create_cmd.append("--mount=type=bind,source=" + mount["src"] + ",destination=" + mount["dest"])

    create_cmd.append(image)

    thread_logger(thread_name, "Podman create command is:\n%s" % (endpoints.dump_json(create_cmd)), remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, " ".join(create_cmd))
    thread_logger(thread_name, "Creating container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    return

def create_chroot(thread_name, remote_name, engine_name, container_name, connection, image, host_mounts):
    """
    Using an existing connection create a podman pod for use as a chroot environment at runtime

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created
        container_name (str): The name to use for the pod on the remote
        connection (Fabric): The Fabric connection to use to run commands on the remote
        image (str): The container image to use for the engine
        host_mounts (list): The user requested host directories to bind mount into the engine

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        create_info (dict): A collection of information about the chroot that is needed to properly destroy it later
    """
    thread_logger(thread_name, "Running create chroot", remote_name = remote_name, engine_name = engine_name)

    create_info = {
        "id": None,
        "mount": None,
        "name": container_name,
        "mounts": {
            "regular": [],
            "rbind": []
        }
    }

    create_cmd = [ "podman",
                   "create",
                   "--name=" + container_name,
                   image ]

    thread_logger(thread_name, "Podman create command is:\n%s" % (endpoints.dump_json(create_cmd)), remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, " ".join(create_cmd))
    thread_logger(thread_name, "Creating container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)
    if result.exited == 0:
        create_info["id"] = result.stdout.rstrip('\n')
    else:
        return create_info

    mount_cmd = [ "podman",
                  "mount",
                  container_name ]

    result = endpoints.run_remote(connection, " ".join(mount_cmd))
    thread_logger(thread_name, "Mounting container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)
    if result.exited == 0:
        create_info["mount"] = result.stdout.rstrip('\n')
    else:
        return create_info

    thread_logger(thread_name, "Container mount is '%s'" % (create_info["mount"]), remote_name = remote_name, engine_name = engine_name)

    mandatory_mounts = [
        {
            "src": settings["dirs"]["remote"]["data"],
            "dest": "/tmp",
            "rbind": False
        },
        {
            "src": "/proc",
            "rbind": True
        },
        {
            "src": "/dev",
            "rbind": True
        },
        {
            "src": "/sys",
            "rbind": True
        },
        {
            "src": "/lib/firmware",
            "rbind": True
        },
        {
            "src": "/lib/modules",
            "rbind": True
        },
        {
            "src": "/usr/src",
            "rbind": True
        },
        {
            "src": "/boot",
            "rbind": True
        },
        {
            "src": "/var/run",
            "rbind": True
        }
    ]

    for mount in mandatory_mounts + host_mounts:
        if not "dest" in mount:
            mount["dest"] = mount["src"]

        if not "rbind" in mount:
            mount["rbind"] = False

        mount["dest"] = create_info["mount"] + mount["dest"]

        thread_logger(thread_name, "Procesing mount:\n%s" % (endpoints.dump_json(mount)), remote_name = remote_name, engine_name = engine_name)

        result = endpoints.run_remote(connection, "mkdir --parents --verbose " + mount["dest"])
        thread_logger(thread_name, "Creating '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["dest"], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

        if mount["rbind"]:
            result = endpoints.run_remote(connection, "mount --verbose --options rbind " + mount["src"] + " " + mount["dest"])
            thread_logger(thread_name, "rbind mounting '%s' to '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["src"], mount["dest"], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

            result = endpoints.run_remote(connection, "mount --verbose --make-rslave " + mount["dest"])
            thread_logger(thread_name, "making rslave '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["dest"], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

            create_info["mounts"]["rbind"].append(mount["dest"])
        else:
            result = endpoints.run_remote(connection, "mount --verbose --options bind " + mount["src"] + " " + mount["dest"])
            thread_logger(thread_name, "bind mounting '%s' to '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["src"], mount["dest"], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

            create_info["mounts"]["regular"].append(mount["dest"])

    local_ssh_private_key_file = settings["dirs"]["local"]["conf"] + "/rickshaw_id.rsa"
    remote_ssh_private_key_file = create_info["mount"] + "/tmp/" + "rickshaw_id.rsa"
    result = connection.put(local_ssh_private_key_file, remote_ssh_private_key_file)
    thread_logger(thread_name, "Copied %s to %s:%s" % (local_ssh_private_key_file, connection.host, remote_ssh_private_key_file), remote_name = remote_name, engine_name = engine_name)

    for etc_file in [ "hosts", "resolv.conf" ]:
        src_etc_file = "/etc/" + etc_file
        dst_etc_dir = create_info["mount"] + "/etc"
        result = endpoints.run_remote(connection, "cp --verbose " + src_etc_file + " " + dst_etc_dir)
        thread_logger(thread_name, "Remotely copied %s to %s" % (src_etc_file, dst_etc_dir), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    thread_logger(thread_name, "chroot create info:\n%s" % (endpoints.dump_json(create_info)), remote_name = remote_name, engine_name = engine_name)

    return create_info

def start_podman(thread_name, remote_name, engine_name, container_name, connection):
    """
    Start a podman pod

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created
        container_name (str): The name to use for the pod on the remote
        connection (Fabric): The Fabric connection to use to run commands on the remote

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread_logger(thread_name, "Running start podman", remote_name = remote_name, engine_name = engine_name)

    start_cmd = [
        "podman",
        "start",
        container_name
    ]

    result = endpoints.run_remote(connection, " ".join(start_cmd))
    thread_logger(thread_name, "Starting container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    return

def start_chroot(thread_name, remote_name, engine_name, container_name, connection, remote, controller_ip, cpu_partitioning, numa_node, chroot_dir):
    """
    Start a chroot

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created
        container_name (str): The name to use for the pod on the remote
        connection (Fabric): The Fabric connection to use to run commands on the remote
        remote (str): The remote that is being used for this action
        controller_ip (str): The controller IP address to tell the engine so it can talk to the controller
        cpu_partitioning (int): Whether to use cpu-partitioning for thie engine or not
        numa_node (int): The NUMA node to bind this engine to
        chroot_dir (str): The directory where the pod is mounted that will be chrooted into

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread_logger(thread_name, "Running start chroot", remote_name = remote_name, engine_name = engine_name)

    start_cmd = [
        "nohup",
        "chroot",
        chroot_dir
    ]

    if not numa_node is None:
        start_cmd.extend([
            "numactl",
            "--cpunodebind=" + str(numa_node),
            "--membind=" + str(numa_node)
        ])

    start_cmd.extend([
        "/usr/local/bin/bootstrap",
        "--base-run-dir=" + settings["dirs"]["local"]["base"],
        "--cpu-partitioning=" + str(cpu_partitioning),
        "--cs-label=" + engine_name,
        "--disable-tools=1",
        "--endpoint=remotehosts",
        "--endpoint-run-dir=" + settings["dirs"]["local"]["endpoint"],
        "--engine-script-start-timeout=" + str(args.engine_script_start_timeout),
        "--max-rb-attempts=" + str(args.max_rb_attempts),
        "--max-sample-failures=" + str(args.max_sample_failures),
        "--rickshaw-host=" + controller_ip,
        "--roadblock-id=" + args.roadblock_id,
        "--roadblock-passwd=" + args.roadblock_passwd,
    ])

    if cpu_partitioning == 1:
        settings["engines"]["remotes"][remote]["cpu-partitions-idx-lock"].acquire()
        cpu_partition_idx = settings["engines"]["remotes"][remote]["cpu-partitions-idx"]
        settings["engines"]["remotes"][remote]["cpu-partitions-idx"] += 1
        settings["engines"]["remotes"][remote]["cpu-partitions-idx-lock"].release()

        thread_logger(thread_name, "Allocated cpu-partition index %d" % (cpu_partition_idx), remote_name = remote_name, engine_name = engine_name)
        start_cmd.extend([
            "--cpu-partition-index=" + str(cpu_partition_idx),
            "--cpu-partitions=" + str(settings["engines"]["remotes"][remote]["total-cpu-partitions"])
        ])

    start_cmd.extend([
        ">" + settings["dirs"]["remote"]["logs"] + "/" + engine_name + ".txt",
        "&"
    ])

    thread_logger(thread_name, "chroot start command is:\n%s" % (endpoints.dump_json(start_cmd)), remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, " ".join(start_cmd))
    thread_logger(thread_name, "Starting chroot with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    return

def launch_engines_worker_thread(thread_id, work_queue, threads_rcs):
    """
    Worker thread to consume and perform engine launches on a remote

    Args:
        thread_id (int): The specifc worker thread that this is
        work_queue (Queue): The work queue to pull jobs to process from
        threads_rcs (list): The list to record the threads return code in

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread = threading.current_thread()
    thread_name = thread.name
    thread_logger(thread_name, "Starting launch engines thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote_idx = None
        try:
            remote_idx = work_queue.get(block = False)
        except queue.Empty:
            thread_logger(thread_name, "Received a work queue empty exception")
            break

        if remote_idx is None:
            thread_logger(thread_name, "Received a null job", log_level = "warning")
            continue

        job_count += 1

        remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][remote_idx]
        remote_name = "%s-%s" % (remote["config"]["host"], remote_idx)

        thread_logger(thread_name, "Processing remote '%s' at index %d" % (remote["config"]["host"], remote_idx), remote_name = remote_name)
        thread_logger(thread_name, "Remote user is %s" % (remote["config"]["settings"]["remote-user"]), remote_name = remote_name)

        with endpoints.remote_connection(remote["config"]["host"], remote["config"]["settings"]["remote-user"]) as con:
            for engine in remote["engines"]:
                for engine_id in engine["ids"]:
                    engine_name = "%s-%s" % (engine["role"], str(engine_id))
                    container_name = "%s_%s" % (args.run_id, engine_name)
                    thread_logger(thread_name, "Creating engine '%s'" % (engine_name), remote_name = remote_name, engine_name = engine_name)
                    thread_logger(thread_name, "Container name will be '%s'" % (container_name), remote_name = remote_name, engine_name = engine_name)

                    image = get_engine_id_image(engine["role"], engine_id)
                    if image is None:
                        thread_logger(thread_name, "Could not determine image", log_level = "error", remote_name = remote_name, engine_name = engine_name)
                        continue
                    else:
                        thread_logger(thread_name, "Image is '%s'" % (image), remote_name = remote_name, engine_name = engine_name)

                    osruntime = None
                    if engine["role"] == "profiler":
                        osruntime = "podman"
                    else:
                        osruntime = remote["config"]["settings"]["osruntime"]
                    thread_logger(thread_name, "osruntime is '%s'" % (osruntime), remote_name = remote_name, engine_name = engine_name)

                    thread_logger(thread_name, "host-mounts is %s" % (remote["config"]["settings"]["host-mounts"]), remote_name = remote_name, engine_name = engine_name)

                    result = endpoints.run_remote(con, "podman ps --all --filter 'name=" + container_name + "' --format '{{.Names}}'")
                    thread_logger(thread_name, "Check for existing container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), remote_name = remote_name, engine_name = engine_name)
                    if result.exited != 0:
                        thread_logger(thread_name, "Check for existing container exited with non-zero return code %d" % (result.exited), log_level = "error", remote_name = remote_name, engine_name = engine_name)
                    if result.stdout.rstrip('\n') == container_name:
                        thread_logger(thread_name, "Found existing container '%s'" % (container_name), log_level = "warning", remote_name = remote_name, engine_name = engine_name)

                        result = endpoints.run_remote(con, "podman rm --force " + container_name)
                        thread_logger(thread_name, "Forced removal of existing container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), remote_name = remote_name, engine_name = engine_name)
                        if result.exited != 0:
                            thread_logger(thread_name, "Forced removal of existing container exited with non-zero return code %d" % (result.exited), log_level = 'error', remote_name = remote_name, engine_name = engine_name)

                    match osruntime:
                        case "podman":
                            cpu_partitioning = None
                            if engine["role"] == "profiler":
                                cpu_partitioning = 0
                            else:
                                if remote["config"]["settings"]["cpu-partitioning"]:
                                    cpu_partitioning = 1
                                else:
                                    cpu_partitioning = 0
                            thread_logger(thread_name, "cpu-partitioning is '%s'" % (str(cpu_partitioning)), remote_name = remote_name, engine_name = engine_name)

                            numa_node = None
                            if not remote["config"]["settings"]["numa-node"] is None:
                                numa_node = remote["config"]["settings"]["numa-node"]
                            thread_logger(thread_name, "numa-node is '%s'" % (str(numa_node)), remote_name = remote_name, engine_name = engine_name)

                            create_podman(thread_name, remote_name, engine_name, container_name, con, remote["config"]["host"], remote["config"]["settings"]["controller-ip-address"], engine["role"], image, cpu_partitioning, numa_node, remote["config"]["settings"]["host-mounts"], remote["config"]["settings"]["podman-settings"])
                        case "chroot":
                            if not "chroots" in remote:
                                remote["chroots"] = dict()
                            create_info = create_chroot(thread_name, remote_name, engine_name, container_name, con, image, remote["config"]["settings"]["host-mounts"])
                            remote["chroots"][engine_name] = create_info

            for engine in remote["engines"]:
                for engine_id in engine["ids"]:
                    engine_name = "%s-%s" % (engine["role"], str(engine_id))
                    container_name = "%s_%s" % (args.run_id, engine_name)
                    thread_logger(thread_name, "Starting engine '%s'" % (engine_name), remote_name = remote_name, engine_name = engine_name)

                    osruntime = None
                    if engine["role"] == "profiler":
                        osruntime = "podman"
                    else:
                        osruntime = remote["config"]["settings"]["osruntime"]
                    thread_logger(thread_name, "osruntime is '%s'" % (osruntime), remote_name = remote_name, engine_name = engine_name)

                    match osruntime:
                        case "podman":
                            start_podman(thread_name, remote_name, engine_name, container_name, con)
                        case "chroot":
                            cpu_partitioning = None
                            if engine["role"] == "profiler":
                                cpu_partitioning = 0
                            else:
                                if remote["config"]["settings"]["cpu-partitioning"]:
                                    cpu_partitioning = 1
                                else:
                                    cpu_partitioning = 0
                            thread_logger(thread_name, "cpu-partitioning is '%s'" % (str(cpu_partitioning)), remote_name = remote_name, engine_name = engine_name)

                            numa_node = None
                            if not remote["config"]["settings"]["numa-node"] is None:
                                numa_node = remote["config"]["settings"]["numa-node"]
                            thread_logger(thread_name, "numa-node is '%s'" % (str(numa_node)), remote_name = remote_name, engine_name = engine_name)

                            start_chroot(thread_name, remote_name, engine_name, container_name, con, remote["config"]["host"], remote["config"]["settings"]["controller-ip-address"], cpu_partitioning, numa_node, remote["chroots"][engine_name]["mount"])

        thread_logger(thread_name, "Notifying work queue that job processing is complete", remote_name = remote_name)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger(thread_name, "Stopping launch engines thread after processing %d job(s)" % (job_count))
    return

def launch_engines():
    """
    Handle the launching of engines to run the test on the remotes

    Args:
        None

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Creating threadpool to handle engine launching")

    launch_engines_work = queue.Queue()
    for remote_idx,remote in enumerate(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"]):
        launch_engines_work.put(remote_idx)
    worker_threads_count = len(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"])

    create_thread_pool("Launch Engines Worker Threads", "LEWT", launch_engines_work, worker_threads_count, launch_engines_worker_thread)

    return 0

def engine_init():
    """
    Construct messages to initialize the engines with metadata specific to them'

    Args:
        None

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        env_vars_msg_file (str): A file containing all the messages to send to the engines
    """
    env_vars_msgs = []
    for remote in settings["run-file"]["endpoints"][args.endpoint_index]["remotes"]:
        for engine in remote["engines"]:
            for id in engine["ids"]:
                engine_name = "%s-%s" % (engine["role"], str(id))
                env_vars_payload = {
                    "env-vars": {
                        "endpoint_label": args.endpoint_label,
                        "hosted_by": remote["config"]["host"],
                        "hypervisor_host": remote["config"]["settings"]["hypervisor-host"],
                        "userenv": remote["config"]["settings"]["userenv"],
                        "osruntime": remote["config"]["settings"]["osruntime"]
                    }
                }

                env_vars_msgs.extend(endpoints.create_roadblock_msg("follower", engine_name, "user-object", env_vars_payload))

    env_vars_msg_file = settings["dirs"]["local"]["roadblock-msgs"] + "/env-vars.json"
    log.info("Writing follower env-vars messages to %s" % (env_vars_msg_file))
    env_vars_msgs_json = endpoints.dump_json(env_vars_msgs)
    with open(env_vars_msg_file, "w", encoding = "ascii") as env_vars_msg_file_fp:
        env_vars_msg_file_fp.write(env_vars_msgs_json)
    log.info("Contents of %s:\n%s" % (env_vars_msg_file, env_vars_msgs_json))

    return env_vars_msg_file

def test_start(msgs_dir, test_id, tx_msgs_dir):
    """
    Perform endpoint responsibilities that must be completed prior to running an iteration test sample

    Args:
        msgs_dir (str): The directory look for received messages in
        test_id (str): A string of the for "<iteration>:<sample>:<attempt>" used to identify the current test
        tx_msgs_dir (str): The directory where to write queued messages for transmit

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    log.info("Running test_start()")

    this_msg_file = msgs_dir + "/" + test_id + ":server-start-end.json"
    path = Path(this_msg_file)

    if path.exists() and path.is_file():
        log.info("Found '%s'" % (this_msg_file))

        msgs_json,err = load_json_file(this_msg_file)
        if not msgs_json is None:
            if "received" in msgs_json:
                for msg in msgs_json["received"]:
                    if msg["payload"]["message"]["command"] == "user-object":
                        if "svc" in msg["payload"]["message"]["user-object"] and "ports" in msg["payload"]["message"]["user-object"]["svc"]:
                            server_engine = msg["payload"]["sender"]["id"]
                            client_engine = re.sub(r"server", r"client", server_engine)

                            log.info("Found a service message from server engine %s to client engine %s" % (server_engine, client_engine))

                            server_remote = None
                            client_remote = None
                            server_mine = False
                            client_mine = False

                            process_msg = False
                            for remote in settings["engines"]["remotes"].keys():
                                if server_remote is None and server_engine in settings["engines"]["remotes"][remote]["engines"]:
                                    server_remote = remote
                                    server_mine = True
                                    log.info("The server engine '%s' is running from this endpoint on remote '%s'" % (server_engine, server_remote))

                                if client_remote is None and client_engine in settings["engines"]["remotes"][remote]["engines"]:
                                    client_remote = remote
                                    client_mine = True
                                    log.info("The client engine '%s' is running from this endpoint on remote '%s'" % (client_engine, client_remote))

                                if not server_remote is None and not client_remote is None:
                                    break

                            if not server_mine and not client_mine:
                                log.info("Neither the server engine '%s' or the client engine '%s' is running from this endpoint, ignoring" % (server_engine, client_engine))
                            elif not server_mine and client_mine:
                                log.info("Only the client engine '%s' is mine, ignoring" % (client_engine))
                            elif server_mine and not client_mine:
                                log.info("Only the server engine '%s' is mine, processing" % (server_engine))
                                process_msg = True
                            elif server_mine and client_mine:
                                log.info("Both the server engine '%s' and the client engine '%s' are mine" % (server_engine, client_engine))

                                if server_remote == client_remote:
                                    log.info("My server engine '%s' and my client engine '%s' are on the same remote '%s', nothing to do" % (server_engine, client_engine, server_remote))
                                else:
                                    log.info("My server engine '%s' is on remote '%s' and my client engine '%s' is on remote '%s', processing" % (server_engine, server_remote, client_engine, client_remote))
                                    process_msg = True

                            if process_msg:
                                log.info("Processing received message:\n%s" % (endpoints.dump_json(msg["payload"])))

                                # punching a hole through the fireall would go here

                                log.info("Creating a message to send to the client engine '%s' with IP and port info" % (client_engine))
                                msg = endpoints.create_roadblock_msg("follower", client_engine, "user-object", msg["payload"]["message"]["user-object"])

                                msg_file = tx_msgs_dir + "/server-ip-" + server_engine + ".json"
                                log.info("Writing follower service-ip message to '%s'" % (msg_file))
                                with open(msg_file, "w", encoding = "ascii") as msg_file_fp:
                                    msg_file_fp.write(endpoints.dump_json(msg))
        else:
            log.error("Failed to load '%s' due to error '%s'" % (this_msg_file, str(err)))
    else:
        log.info("Could not find '%s'" % (this_msg_file))

    log.info("Returning from test_start()")
    return

def test_stop():
    """
    Perform endpoint responsibilties that must be completed after an iteration test sample

    Args:
        None

    Globals:
        log: a logger instance

    Returns:
        None
    """
    log.info("Running test_stop()")
    log.info("...nothing to do here, run along...")
    log.info("Returning from test_stop()")
    return

def collect_podman_log(thread_name, remote_name, engine_name, container_name, connection):
    """
    Use an existing connection to collect the podman logs for a specific engine

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created
        container_name (str): The name to use for the pod on the remote
        connection (Fabric): The Fabric connection to use to run commands on the remote

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        True: On success
        or
        False: On failure
    """
    thread_logger(thread_name, "Collecting podman log", remote_name = remote_name, engine_name = engine_name)

    cmd_retries = 5
    cmd_attempt = 1
    cmd_rc = 1

    while cmd_rc != 0 and cmd_attempt <= cmd_retries:
        result = endpoints.run_remote(connection, "podman logs --timestamps " + container_name + " | xz -c | base64")
        if result.exited != 0:
            thread_logger(thread_name, "Collecting podman log for %s failed with return code %d on attempt %d of %d:\nstdout:\n%sstderr:\n%s" %
                        (
                            engine_name,
                            result.exited,
                            cmd_attempt,
                            cmd_retries,
                            result.stdout,
                            result.stderr
                        ), log_level = "error", remote_name = remote_name, engine_name = engine_name)
        else:
            log_file = settings["dirs"]["local"]["engine-logs"] + "/" + engine_name + ".txt.xz"
            with open(log_file, "wb") as log_file_fp:
                log_file_fp.write(base64.b64decode(result.stdout))
            thread_logger(thread_name, "Wrote podman log to %s" % (log_file), remote_name = remote_name, engine_name = engine_name)
        cmd_rc = result.exited
        cmd_attempt += 1

    if cmd_rc != 0:
        thread_loggeer(thread_name, "Failed to collect podman log for %s" % (engine_name), log_level = "error", remote_name = remote_name, engine_name = engine_name)
        return False
    else:
        return True

def collect_chroot_log(thread_name, remote_name, engine_name, container_name, connection):
    """
    Use an existing connection to collect the chroot logs for a specific engine

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created
        container_name (str): The name to use for the pod on the remote
        connection (Fabric): The Fabric connection to use to run commands on the remote

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        True: On success
        or
        False: On failure
    """
    thread_logger(thread_name, "Collecting chroot log", remote_name = remote_name, engine_name = engine_name)

    remote_log_file = settings["dirs"]["remote"]["logs"] + "/" + engine_name + ".txt"
    result = endpoints.run_remote(connection, "cat " + remote_log_file + " | xz -c | base64")
    if result.exited != 0:
        thread_logger(thread_name, "Collecting chroot log for %s failed with return code %d:\nstdout:\n%sstderr:\n%s" %
                      (
                          engine_name,
                          result.exited,
                          result.stdout,
                          result.stderr
                      ), log_level = "error", remote_name = remote_name, engine_name = engine_name)
        return False
    else:
        log_file = settings["dirs"]["local"]["engine-logs"] + "/" + engine_name + ".txt.xz"
        with open(log_file, "wb") as log_file_fp:
            log_file_fp.write(base64.b64decode(result.stdout))
        thread_logger(thread_name, "Wrote chroot log to %s" % (log_file), remote_name = remote_name, engine_name = engine_name)

        result = endpoints.run_remote(connection, "rm --verbose " + remote_log_file)
        thread_logger(thread_name, "Removal of engine log for '%s' gave return code %d:\nstdout:\n%stderr:\n%s" %
                      (
                          engine_name,
                          result.exited,
                          result.stdout,
                          result.stderr
                      ), log_level =  endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)
        return True

def remove_rickshaw_settings(connection, thread_name, remote_name, engine_name):
    """
    Use an existing connection to remove the rickshaw settings file from a remote as part of cleanup

    Args:
        connection (Fabric): The Fabric connection to use to run commands on the remote
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    remote_rickshaw_settings = settings["dirs"]["remote"]["data"] + "/rickshaw-settings.json.xz"
    result = endpoints.run_remote(connection, "if [ -e \"" + remote_rickshaw_settings + "\" ]; then rm --verbose \"" + remote_rickshaw_settings + "\"; else echo \"rickshaw settings already removed\"; fi")
    thread_logger(thread_name, "Removal of rickshaw settings '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (remote_rickshaw_settings, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    return

def remove_ssh_private_key(connection, thread_name, remote_name, engine_name):
    """
    Use an existing connection to remove the SSH private key from a remote as part of cleanup

    Args:
        connection (Fabric): The Fabric connection to use to run commands on the remote
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    remote_ssh_private_key = settings["dirs"]["remote"]["data"] + "/rickshaw_id.rsa"
    result = endpoints.run_remote(connection, "if [ -e \"" + remote_ssh_private_key + "\" ]; then rm --verbose \"" + remote_ssh_private_key + "\"; else echo \"ssh private key already removed\"; fi")
    thread_logger(thread_name, "Removal of ssh private key '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (remote_ssh_private_key, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    return

def destroy_podman(thread_name, remote_name, engine_name, container_name, connection):
    """
    Use an existing connection to destroy a podman pod used as a podman pod

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created
        container_name (str): The name to use for the pod on the remote
        connection (Fabric): The Fabric connection to use to run commands on the remote

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread_logger(thread_name, "Destroying podman", remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, "podman rm --force " + container_name)
    thread_logger(thread_name, "Removal of pod '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (engine_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    remote_env_file = settings["dirs"]["remote"]["cfg"] + "/" + engine_name + "_env.txt"
    result = endpoints.run_remote(connection, "rm --verbose " + remote_env_file)
    thread_logger(thread_name, "Removal of env file  '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (remote_env_file, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    remove_ssh_private_key(connection, thread_name, remote_name, engine_name)

    remove_rickshaw_settings(connection, thread_name, remote_name, engine_name)

    return

def destroy_chroot(thread_name, remote_name, engine_name, container_name, connection, chroot_info):
    """
    Use an existing connection to destroy a podman pod used as a chroot

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        engine_name (str): The specific engine that is being created
        container_name (str): The name to use for the pod on the remote
        connection (Fabric): The Fabric connection to use to run commands on the remote
        chroot_info (dict): A dictionary containing information about the chroot that can be used to clean it up

    Globals:
        None

    Returns:
        None
    """
    thread_logger(thread_name, "Destroying chroot", remote_name = remote_name, engine_name = engine_name)

    for mount in chroot_info["mounts"]["regular"]:
        result = endpoints.run_remote(connection, "umount --verbose " + mount)
        thread_logger(thread_name, "regular unmounting of '%s' resulted in record code %d:\nstdout:\n%sstderr:\n%s" % (mount, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    for mount in chroot_info["mounts"]["rbind"]:
        result = endpoints.run_remote(connection, "umount --verbose --recursive " + mount)
        thread_logger(thread_name, "recursive unmounting of '%s' resulted in record code %d:\nstdout:\n%sstderr:\n%s" % (mount, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, "podman rm --force " + container_name)
    thread_logger(thread_name, "Removal of pod '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (engine_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    remove_ssh_private_key(connection, thread_name, remote_name, engine_name)

    remove_rickshaw_settings(connection, thread_name, remote_name, engine_name)

    return

def remove_image(thread_name, remote_name, log_prefix, connection, image):
    """
    Use an existing connection to remove a podman image from a remote

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        log_prefix (str): A label to prefix the log messages with
        image (str): The container image to be removed

    Globals:
        None

    Returns:
        None
    """
    thread_logger(thread_name, "Removing image '%s'" % (image), remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman rmi " + image)
    thread_logger(thread_name, "Removing podman image '%s' gave return code %d:\nstdout:\n%sstderr:\n%s" % (image, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, log_prefix = log_prefix)

    return

def remote_image_manager(thread_name, remote_name, connection, image_max_cache_size):
    """
    Using an existing connection implement the image management algorithm that determines which images to
    keep (ie. cache) and which to remove on a remote

    Args:
        thread_name (str): The thread identifier for the context this is being run under
        remote_name (str): The remote that is being used for this action
        connection (Fabric): The Fabric connection to use to run commands on the remote
        image_max_cache_size (int): The maximum idealnumber of images to store the in the cache (can be exceeded
                                    in order to cache a complete set of images for a run)

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    log_prefix = "RIM"
    thread_logger(thread_name, "Performing container image management", remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman images --all")
    thread_logger(thread_name, "All podman images on this remote host before running image manager:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

    images = dict()
    images["rickshaw"] = dict()
    images["podman"] = dict()
    images["quay"] = dict()

    result = endpoints.run_remote(connection, "cat /var/lib/crucible/remotehosts-container-image-census")
    if result.exited != 0:
        thread_logger(thread_name, "Reading image census returned %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = "error", remote_name = remote_name, log_prefix = log_prefix)
        return
    for line in result.stdout.splitlines():
        fields = line.split(" ")
        image_name = fields[0]
        image_timestamp = int(fields[1])
        image_run_id = fields[2]

        if not image_name in images["rickshaw"]:
            images["rickshaw"][image_name] = {
                "cached": False,
                "latest-usage": None,
                "run-ids": dict(),
                "timestamps": [],
                "uses": None
            }

        images["rickshaw"][image_name]["timestamps"].append(image_timestamp)
        images["rickshaw"][image_name]["run-ids"][image_timestamp] = image_run_id
    thread_logger(thread_name, "images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman images --format='{{.Repository}}:{{.Tag}}|{{.CreatedAt}}'")
    if result.exited != 0:
        thread_logger(thread_name, "Getting podman images returned %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = "error", remote_name = remote_name, log_prefix = log_prefix)
        return
    for line in result.stdout.splitlines():
        fields = line.split("|")
        image_name = fields[0]
        image_timestamp = fields[1]

        if not image_name in images["podman"]:
            images["podman"][image_name] = {
                "created-input": image_timestamp,
                "created": endpoints.gmtimestamp_to_gmepoch(image_timestamp)
            }
    thread_logger(thread_name, "images[podman]:\n%s" % (endpoints.dump_json(images["podman"])), remote_name = remote_name, log_prefix = log_prefix)

    if settings["rickshaw"]["quay"]["refresh-expiration"]["api-url"] is not None:
        thread_logger(thread_name, "Found configuration information necessary to utilize the quay API to obtain image expiration", remote_name = remote_name, log_prefix = log_prefix)
        thread_logger(thread_name, "Quay API URL: %s" % (settings["rickshaw"]["quay"]["refresh-expiration"]["api-url"]), remote_name = remote_name, log_prefix = log_prefix)

        for image in images["podman"].keys():
            image_parts = image.split(":")

            get_request = requests.get(settings["rickshaw"]["quay"]["refresh-expiration"]["api-url"] + "/tag", params = { "onlyActiveTags": True, "specificTag": image_parts[1] })

            query_log_level = "info"
            if get_request.status_code != requests.codes.ok:
                query_log_level = "warning"

            thread_logger(thread_name, "Quay API query for %s returned %d" % (image, get_request.status_code), log_level = query_log_level, remote_name = remote_name, log_prefix = log_prefix)

            if get_request.status_code == requests.codes.ok:
                image_json = get_request.json()
                if len(image_json["tags"]) == 1:
                    images["quay"][image] = image_json["tags"][0]
                else:
                    thread_logger(thread_name, "Quay API query for %s found %d tags" % (image, len(image_json["tags"])), log_level = "warning", remote_name = remote_name, log_prefix = log_prefix)

        thread_logger(thread_name, "images[quay]:\n%s" % (endpoints.dump_json(images["quay"])), remote_name = remote_name, log_prefix = log_prefix)
    else:
        thread_logger(thread_name, "Configuration information necessary to utilize the quay API to obtain image expiration timestamps is not available", remote_name = remote_name, log_prefix = log_prefix)

    image_expiration = endpoints.image_expiration_gmepoch()
    thread_logger(thread_name, "Images evaludated by their expiration data will be considered expired if it is before %d" % (image_expiration), remote_name = remote_name, log_prefix = log_prefix)

    expiration_weeks = int(settings["rickshaw"]["quay"]["image-expiration"].rstrip("w"))
    image_created_expiration = endpoints.image_created_expiration_gmepoch(expiration_weeks)
    thread_logger(thread_name, "Images evaludated by their creation data will be considered expired if it is before %d (%d weeks ago)" % (image_created_expiration, expiration_weeks), remote_name = remote_name, log_prefix = log_prefix)

    deletes = []
    for image in images["rickshaw"].keys():
        if not image in images["podman"]:
            thread_logger(thread_name, "Rickshaw image '%s' is no longer present in podman images, removing from consideration" % (image), remote_name = remote_name, log_prefix = log_prefix)
            deletes.append(image)
            continue

        images["rickshaw"][image]["timestamps"].sort(reverse = True)

        images["rickshaw"][image]["uses"] = len(images["rickshaw"][image]["timestamps"])

        images["rickshaw"][image]["latest-usage"] = images["rickshaw"][image]["timestamps"][0]

        del images["rickshaw"][image]["timestamps"]
    for image in deletes:
        del images["rickshaw"][image]
    thread_logger(thread_name, "images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)

    deletes = dict()
    deletes["rickshaw"] = []
    deletes["podman"] = []
    for image in images["podman"].keys():
        image_repo = os.environ.get("RS_REG_REPO")
        if image_repo is None:
            image_prefix = r"client-server"
            thread_logger(thread_name, "Using default podman image prefix '%s'" % (image_prefix), log_level = "warning", remote_name = remote_name, log_prefix = log_prefix)
        else:
            image_prefix = image_repo.split("/")
            image_prefix = image_prefix[len(image_prefix) - 1]
            thread_logger(thread_name, "Using podman image prefix '%s'" % (image_prefix), remote_name = remote_name, log_prefix = log_prefix)

        m = re.search(image_prefix, image)
        if m is None:
            thread_logger(thread_name, "Podman image '%s' is not a '%s' image, ignoring" % (image, image_prefix), remote_name = remote_name, log_prefix = log_prefix)
            deletes["podman"].append(image)
            continue
        else:
            thread_logger(thread_name, "Podman image '%s' is a '%s' image, processing" % (image, image_prefix), remote_name = remote_name, log_prefix = log_prefix)

        if not image in images["rickshaw"]:
            thread_logger(thread_name, "Podman image '%s' is not present in rickshaw container image census, removing it from the image cache" % (image), remote_name = remote_name, log_prefix = log_prefix)
            deletes["podman"].append(image)
            remove_image(thread_name, remote_name, log_prefix, connection, image)

        if image in images["quay"]:
            if images["quay"][image]["end_ts"] < image_expiration:
                thread_logger(thread_name, "Podman image '%s' has been evaluated based on it's expiration data and has expired, removing it from the image cache" % (image), remote_name = remote_name, log_prefix = log_prefix)
                deletes["podman"].append(image)
                deletes["rickshaw"].append(image)
                remove_image(thread_name, remote_name, log_prefix, connection, image)
            else:
                thread_logger(thread_name, "Podman image '%s' has been evaluated based on it's expiration data and has not expired" % (image), remote_name = remote_name, log_prefix = log_prefix)
        elif images["podman"][image]["created"] < image_created_expiration:
            thread_logger(thread_name, "Podman image '%s' has been evaluated based on it's creation data and has expired, removing it from the image cache" % (image), remote_name = remote_name, log_prefix = log_prefix)
            deletes["podman"].append(image)
            deletes["rickshaw"].append(image)
            remove_image(thread_name, remote_name, log_prefix, connection, image)
        else:
            thread_logger(thread_name, "Podman image '%s' has been evaluated based on it's creation data and has not expired" % (image), remote_name = remote_name, log_prefix = log_prefix)

        if not image in deletes["podman"]:
            thread_logger(thread_name, "Podman image '%s' is valid and remains under consideration" % (image), remote_name = remote_name, log_prefix = log_prefix)
    for kind in deletes.keys():
        for image in deletes[kind]:
            del images[kind][image]
    thread_logger(thread_name, "images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)
    thread_logger(thread_name, "images[podman]:\n%s" % (endpoints.dump_json(images["podman"])), remote_name = remote_name, log_prefix = log_prefix)

    cache_size = 0

    if cache_size < image_max_cache_size:
        thread_logger(thread_name, "Attemping to cache the most recently used image(s)", remote_name = remote_name, log_prefix = log_prefix)
        sorted_images = sorted(images["rickshaw"].items(), key = lambda x: (x[1]["latest-usage"], x[0]), reverse = True)
        thread_logger(thread_name, "latest sorted images[rickshaw]:\n%s" % (endpoints.dump_json(sorted_images)), remote_name = remote_name, log_prefix = log_prefix)

        image = sorted_images[0][0]
        image_last_used = sorted_images[0][1]["latest-usage"]
        image_run_id = sorted_images[0][1]["run-ids"][image_last_used]
        images["rickshaw"][image]["cached"] = True
        cache_size += 1
        thread_logger(thread_name, "Rickshaw image '%s' is being preserved in the image cache at slot %d of %d due to last used" % (image, cache_size, image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

        if cache_size < image_max_cache_size:
            for image in images["rickshaw"].keys():
                if cache_size >= image_max_cache_size:
                    thread_logger(thread_name, "Image cache is full", remote_name = remote_name, log_prefix = log_prefix)
                    break

                if images["rickshaw"][image]["cached"]:
                    continue

                for timestamp in images["rickshaw"][image]["run-ids"].keys():
                    if images["rickshaw"][image]["run-ids"][timestamp] == image_run_id:
                        images["rickshaw"][image]["cached"] = True
                        cache_size += 1
                        thread_logger(thread_name, "Rickshaw image '%s' is being preserved in the image cache at slot %d of %d due to association with last used" % (image, cache_size, image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

                if cache_size > image_max_cache_size:
                    thread_logger(thread_name, "Image cache maximum size '%d' exceeded in order to cache complete run image sets" % (image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)
        else:
            thread_logger(thread_name, "Image cache is full", remote_name = remote_name, log_prefix = log_prefix)

        thread_logger(thread_name, "images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)
    else:
        thread_logger(thread_name, "No images can be cached due to maximum image cache size of %d" % (image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

    if cache_size < image_max_cache_size:
        thread_logger(thread_name, "Attempting to cache the most used image(s)", remote_name = remote_name, log_prefix = log_prefix)
        sorted_images = sorted(images["rickshaw"].items(), key = lambda x: (x[1]["uses"], x[0]), reverse = True)
        thread_logger(thread_name, "usage sorted images[rickshaw]:\n%s" % (endpoints.dump_json(sorted_images)), remote_name = remote_name, log_prefix = log_prefix)

        loop = True
        while loop and cache_size < image_max_cache_size:
            all_processed = True

            for image in sorted_images:
                if not image[1]["cached"]:
                    all_processed = False
                    image_last_used = image[1]["latest-usage"]
                    image_run_id = image[1]["run-ids"][image_last_used]
                    image[1]["cached"] = True
                    images["rickshaw"][image[0]]["cached"] = True
                    cache_size += 1
                    thread_logger(thread_name, "Rickshaw image '%s' is being preserved in the image cache at slot %d of %d due to usage" % (image[0], cache_size, image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

                    if cache_size < image_max_cache_size:
                        for image2 in images["rickshaw"].keys():
                            if cache_size >= image_max_cache_size:
                                thread_logger(thread_name, "Image cache is full", remote_name = remote_name, log_prefix = log_prefix)
                                loop = False
                                break

                            if images["rickshaw"][image2]["cached"]:
                                continue

                            for timestamp in images["rickshaw"][image2]["run-ids"].keys():
                                if images["rickshaw"][image2]["run-ids"][timestamp] == image_run_id:
                                    images["rickshaw"][image2]["cached"] = True
                                    cache_size += 1
                                    thread_logger(thread_name, "Rickshaw image '%s' is being preserved in the image cache at slot %d of %d due to association with usage" % (image2, cache_size, image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

                            if cache_size > image_max_cache_size:
                                thread_logger(thread_name, "Image cache maximum size '%d' exceeded in order to cache complete run image sets" % (image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)
                                loop = False
                                break
                    else:
                        thread_logger(thread_name, "Image cache is full", remote_name = remote_name, log_prefix = log_prefix)
                        loop = False

            if all_processed:
                thread_logger(thread_name, "All rickshaw images are cached", remote_name = remote_name, log_prefix = log_prefix)
                break

        thread_logger(thread_name, "images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)
    else:
        if cache_size == 0:
            thread_logger(thread_name, "No images can be cached due to maximum image cache size of %d" % (image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)
        else:
            thread_logger(thread_name, "Image cache is full", remote_name = remote_name, log_prefix = log_prefix)

    thread_logger(thread_name, "Removing all rickshaw images not marked to preserve in the image cache", remote_name = remote_name, log_prefix = log_prefix)
    all_cached = True
    for image in images["rickshaw"].keys():
        if not images["rickshaw"][image]["cached"]:
            all_cached = False
            remove_image(thread_name, remote_name, log_prefix, connection, image)
    if all_cached:
        thread_logger(thread_name, "All rickshaw images are cached", remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman image prune -f")
    thread_logger(thread_name, "Pruning dangling images resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman images --all")
    thread_logger(thread_name, "All podman images on this remote host after running image manager:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

    return

def shutdown_engines_worker_thread(thread_id, work_queue, threads_rcs):
    """
    Worker thread to consume and perform engine shutdown jobs for a remote

    Args:
        thread_id (int): The specifc worker thread that this is
        work_queue (Queue): The work queue to pull jobs to process from
        threads_rcs (list): The list to record the threads return code in

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread = threading.current_thread()
    thread_name = thread.name
    thread_logger(thread_name, "Starting shutdown engines thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote_idx = None
        try:
            remote_idx = work_queue.get(block = False)
        except queue.Empty:
            thread_logger(thread_name, "Received a work queue empty exception")
            break

        if remote_idx is None:
            thread_logger(thread_name, "Received a null job", log_level = "warning")
            continue

        job_count += 1

        remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][remote_idx]
        remote_name = "%s-%s" % (remote["config"]["host"], remote_idx)

        thread_logger(thread_name, "Processing remote '%s' at index %d" % (remote["config"]["host"], remote_idx), remote_name = remote_name)
        thread_logger(thread_name, "Remote user is %s" % (remote["config"]["settings"]["remote-user"]), remote_name = remote_name)

        with endpoints.remote_connection(remote["config"]["host"], remote["config"]["settings"]["remote-user"]) as con:
            result = endpoints.run_remote(con, "mount")
            thread_logger(thread_name, "All mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = endpoints.run_remote(con, "podman ps --all")
            thread_logger(thread_name, "All podman pods on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = endpoints.run_remote(con, "podman mount")
            thread_logger(thread_name, "All podman container mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            for engine in remote["engines"]:
                for engine_id in engine["ids"]:
                    engine_name = "%s-%s" % (engine["role"], str(engine_id))
                    container_name = "%s_%s" % (args.run_id, engine_name)
                    thread_logger(thread_name, "Processing engine '%s'" % (engine_name), remote_name = remote_name, engine_name = engine_name)
                    thread_logger(thread_name, "Container name is '%s'" % (container_name), remote_name = remote_name, engine_name = engine_name)

                    osruntime = None
                    if engine["role"] == "profiler":
                        osruntime = "podman"
                    else:
                        osruntime = remote["config"]["settings"]["osruntime"]
                    thread_logger(thread_name, "osruntime is '%s'" % (osruntime), remote_name = remote_name, engine_name = engine_name)

                    match osruntime:
                        case "podman":
                            success = collect_podman_log(thread_name, remote_name, engine_name, container_name, con)
                            if success:
                                destroy_podman(thread_name, remote_name, engine_name, container_name, con)
                        case "chroot":
                            success = collect_chroot_log(thread_name, remote_name, engine_name, container_name, con)
                            if success:
                                destroy_chroot(thread_name, remote_name, engine_name, container_name, con, remote["chroots"][engine_name])

            result = endpoints.run_remote(con, "mount")
            thread_logger(thread_name, "All mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = endpoints.run_remote(con, "podman ps --all")
            thread_logger(thread_name, "All podman pods on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = endpoints.run_remote(con, "podman mount")
            thread_logger(thread_name, "All podman container mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

        thread_logger(thread_name, "Notifying work queue that job processing is complete", remote_name = remote_name)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger(thread_name, "Stopping shutdown engines thread after processing %d job(s)" % (job_count))
    return

def shutdown_engines():
    """
    Handle the shutdown of engines after the test is complete

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Creating threadpool to handle engine shutdown")

    shutdown_engines_work = queue.Queue()
    for remote_idx,remote in enumerate(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"]):
        shutdown_engines_work.put(remote_idx)
    worker_threads_count = len(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"])

    create_thread_pool("Shutdown Engines Worker Threads", "SEWT", shutdown_engines_work, worker_threads_count, shutdown_engines_worker_thread)

    return 0

def image_mgmt_worker_thread(thread_id, work_queue, threads_rcs):
    """
    Worker thread to consume and perform image management jobs for a unique remote

    Args:
        thread_id (int): The specifc worker thread that this is
        work_queue (Queue): The work queue to pull jobs to process from
        threads_rcs (list): The list to record the threads return code in

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread = threading.current_thread()
    thread_name = thread.name
    thread_logger(thread_name, "Starting image management thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote = None
        try:
            remote = work_queue.get(block = False)
        except queue.Empty:
            thread_logger(thread_name, "Received a work queue empty exception")
            break

        if remote is None:
            thread_logger(thread_name, "Received a null job", log_level = "warning")
            continue

        job_count += 1

        my_unique_remote = settings["engines"]["remotes"][remote]
        my_run_file_remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][my_unique_remote["run-file-idx"][0]]

        thread_logger(thread_name, "Remote user is %s" % (my_run_file_remote["config"]["settings"]["remote-user"]), remote_name = remote)

        with endpoints.remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as con:
            remote_image_manager(thread_name, remote, con, my_run_file_remote["config"]["settings"]["image-cache-size"])

        thread_logger(thread_name, "Notifying work queue that job processing is complete", remote_name = remote)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger(thread_name, "Stopping image management thread after processing %d job(s)" % (job_count))
    return

def image_mgmt():
    """
    Handle the image management on the remotes

    Args:
        None

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Creating threadpool to handle image management")

    image_mgmt_work = queue.Queue()
    for remote in settings["engines"]["remotes"].keys():
        image_mgmt_work.put(remote)
    worker_threads_count = len(settings["engines"]["remotes"])

    create_thread_pool("Image Management Worker Threads", "IMWT", image_mgmt_work, worker_threads_count, image_mgmt_worker_thread)

    return 0

def remote_cleanup():
    """
    Cleanup the remotes after a test

    Args:
        None

    Globals:
        None

    Returns:
        0
    """
    shutdown_engines()

    image_mgmt()

    return 0

def collect_sysinfo_worker_thread(thread_id, work_queue, threads_rcs):
    """
    Worker thread to consume and perform sysinfo jobs for a unique remote

    Args:
        thread_id (int): The specifc worker thread that this is
        work_queue (Queue): The work queue to pull jobs to process from
        threads_rcs (list): The list to record the threads return code in

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    thread = threading.current_thread()
    thread_name = thread.name
    thread_logger(thread_name, "Starting collect sysinfo thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote = None
        try:
            remote = work_queue.get(block = False)
        except queue.Empty:
            thread_logger(thread_name, "Received a work queue empty exception")
            break

        if remote is None:
            thread_logger(thread_name, "Received a null job", log_level = "warning")
            continue

        job_count += 1

        my_unique_remote = settings["engines"]["remotes"][remote]
        my_run_file_remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][my_unique_remote["run-file-idx"][0]]

        thread_logger(thread_name, "Remote user is %s" % (my_run_file_remote["config"]["settings"]["remote-user"]), remote_name = remote)

        with endpoints.remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as con:
            local_dir = settings["dirs"]["local"]["sysinfo"] + "/" + remote
            endpoints.my_make_dirs(local_dir)

            local_packrat_file = args.packrat_dir + "/packrat"
            remote_packrat_file = settings["dirs"]["remote"]["run"] + "/packrat"
            result = con.put(local_packrat_file, remote_packrat_file)
            thread_logger(thread_name, "Copied %s to %s:%s" % (local_packrat_file, remote, remote_packrat_file), remote_name = remote)

            result = endpoints.run_remote(con, remote_packrat_file + " " + settings["dirs"]["remote"]["sysinfo"])
            thread_logger(thread_name, "Running packrat resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)

            result = endpoints.run_remote(con, "tar --create --directory " + settings["dirs"]["remote"]["sysinfo"]  + " packrat-archive | xz --stdout | base64")
            thread_logger(thread_name, "Transferring packrat files resulted in return code %d:\nstderr:\n%s" % (result.exited, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)

            archive_file = local_dir + "/packrat.tar.xz"
            with open(archive_file, "wb") as archive_file_fp:
                archive_file_fp.write(base64.b64decode(result.stdout))
            thread_logger(thread_name, "Wrote packrat archive to '%s'" % (archive_file), remote_name = remote)

            result = endpoints.run_local("xz --decompress --stdout " + archive_file + " | tar --extract --verbose --directory " + local_dir)
            thread_logger(thread_name, "Unpacking packrat archive resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)

            path = Path(archive_file)
            path.unlink()
            thread_logger(thread_name, "Removed packrat archive '%s'" % (archive_file), remote_name = remote)

            result = endpoints.run_remote(con, "rm --recursive --force " + remote_packrat_file + " " + settings["dirs"]["remote"]["sysinfo"] + "/packrat-archive")
            thread_logger(thread_name, "Removing remote packrat files resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)

        thread_logger(thread_name, "Notifying work queue that job processing is complete", remote_name = remote)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger(thread_name, "Stopping collect sysinfo thread after processing %d job(s)" % (job_count))
    return

def collect_sysinfo():
    """
    Handle the collection of sysinfo data on the unique remotes

    Args:
        None

    Globals:
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Creating threadpool to collect sysinfo")

    sysinfo_work = queue.Queue()
    for remote in settings["engines"]["remotes"].keys():
        sysinfo_work.put(remote)
    worker_threads_count = len(settings["engines"]["remotes"])

    create_thread_pool("Collect Sysinfo Worker Threads", "CSWT", sysinfo_work, worker_threads_count, collect_sysinfo_worker_thread)

    return 0

def thread_exception_hook(args):
    """
    Generic thread exception handler

    Args:
        args (namespace): information about the exception being handled

    Globals:
        log: a logger instance

    Returns:
        None
    """
    thread_name = "UNKNOWN"
    if not args.thread is None:
        thread_name = args.thread.name

    msg = "[Thread %s] Thread failed with exception:\ntype: %s\nvalue: %s\ntraceback:\n%s" % (thread_name, args.exc_type, args.exc_value, "".join(traceback.format_list(traceback.extract_tb(args.exc_traceback))))
    log.error(msg, stacklevel = 3)

    return

def main():
    """
    Main control block

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters

    Returns:
        rc (int): The return code for the script
    """
    global args
    global log
    global settings

    threading.excepthook = thread_exception_hook

    if args.validate:
        return(validate())

    log = endpoints.setup_logger(args.log_level)

    endpoints.log_env()
    endpoints.log_cli(args)
    init_settings()
    if load_settings() != 0:
        return 1
    if check_base_requirements() != 0:
        return 1
    if build_unique_remote_configs() != 0:
        return 1
    create_local_dirs()
    create_remote_dirs()
    remotes_pull_images()
    set_total_cpu_partitions()
    launch_engines()

    remotehosts_callbacks = {
        "engine-init": engine_init,
        "collect-sysinfo": collect_sysinfo,
        "test-start": test_start,
        "test-stop": test_stop,
        "remote-cleanup": remote_cleanup
    }
    rc = endpoints.process_roadblocks(callbacks = remotehosts_callbacks,
                                      roadblock_id = args.roadblock_id,
                                      endpoint_label = args.endpoint_label,
                                      endpoint_deploy_timeout = args.endpoint_deploy_timeout,
                                      max_roadblock_attempts = args.max_rb_attempts,
                                      roadblock_password = args.roadblock_passwd,
                                      new_followers = settings["engines"]["new-followers"],
                                      roadblock_messages_dir = settings["dirs"]["local"]["roadblock-msgs"],
                                      roadblock_timeouts = settings["rickshaw"]["roadblock"]["timeouts"],
                                      max_sample_failures = args.max_sample_failures,
                                      engine_commands_dir = settings["dirs"]["local"]["engine-cmds"],
                                      endpoint_dir = settings["dirs"]["local"]["endpoint"])

    log.info("Logging 'final' settings data structure")
    log_settings(mode = "settings")
    log.info("remotehosts endpoint exiting")
    return rc

if __name__ == "__main__":
    args = endpoints.process_options()
    log = None
    settings = dict()
    exit(main())
