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

    engine_types = set()
    engine_types.add("profiler")
    for remote in endpoint_settings["remotes"]:
        for engine in remote["engines"]:
            engine_types.add(engine["role"])
    
    endpoints.validate_comment("engine types that this endpoint is using")
    endpoints.validate_log("engine-types %s" % (" ".join(list(engine_types))))

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

def normalize_endpoint_settings(endpoint, rickshaw):
    """
    Normalize the endpoint settings by determining where default settings need to be applied and expanding ID ranges

    Args:
        endpoint (dict): The specific endpoint dictionary from the run-file that this endpoint instance is handling
        rickshaw (dict): The rickshaw settings dictionary

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
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
                        logger.error(msg)
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
                    logger.error(msg)
                return None

    return endpoint

def check_base_requirements():
    """
    Check if the base requirements to perform a non-validation run were provided

    Args:
        None

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    logger.info("Checking base requirements")

    if settings["misc"]["run-id"] == "":
        logger.error("The run ID was not provided")
        return 1
    else:
        logger.info("run-id: %s" % (settings["misc"]["run-id"]))

    path = Path(settings["dirs"]["local"]["engine-cmds"] + "/client/1")
    if not path.is_dir():
        logger.error("client-1 bench command directory not found [%s]" % (path))
        return 1
    else:
        logger.info("client-1 bench command directory found [%s]" % (path))

    return 0

def build_unique_remote_configs():
    """
    Process endpoint settings and determine the unique remotes and populate them with the appropriate tools engines

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    logger.info("Building unique remote configs")

    if not "engines" in settings:
        settings["engines"] = dict()
    settings["engines"]["remotes"] = dict()
    settings["engines"]["profiler-mapping"] = dict()
    settings["engines"]["new-followers"] = []

    for remote_idx,remote in enumerate(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"]):
        if not remote["config"]["host"] in settings["engines"]["remotes"]:
            settings["engines"]["remotes"][remote["config"]["host"]] = {
                "roles": dict(),
                "run-file-idx": [],
                "disable-tools": None,
                "engines": [],
                "tool-opt-in-tags": [],
                "tool-opt-out-tags": []
            }

        if settings["engines"]["remotes"][remote["config"]["host"]]["disable-tools"] is None:
            settings["engines"]["remotes"][remote["config"]["host"]]["disable-tools"] = remote["config"]["settings"]["disable-tools"]
        elif settings["engines"]["remotes"][remote["config"]["host"]]["disable-tools"] != remote["config"]["settings"]["disable-tools"]:
            raise ValueError("Conflicting values for disable-tools for remote %s" % (remote["config"]["host"]))

        for opt_tag_type in [ "tool-opt-in-tags", "tool-opt-out-tags" ]:
            if opt_tag_type in remote["config"]["settings"]:
                settings["engines"]["remotes"][remote["config"]["host"]][opt_tag_type].extend(remote["config"]["settings"][opt_tag_type])

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

    profiler_count = 0
    for remote in settings["engines"]["remotes"].keys():
        logger.info("Configuring tools for remote %s" % (remote))

        if settings["engines"]["remotes"][remote]["disable-tools"]:
            logger.info("Remote %s has tools disabled" % (remote))
            continue

        start_tools,err = load_json_file(settings["dirs"]["local"]["tool-cmds"] + "/profiler/start.json.xz", uselzma = True)
        if start_tools is None:
            logger.error("Failed to load the start tools command file from %s" % (tool_cmd_dir))
            return 1

        profiler_count += 1
        for tool in start_tools["tools"]:
            logger.info("Processing tool %s with deployment mode %s" % (tool["name"], tool["deployment"]))

            if tool["deployment"] != "auto":
                if tool["deployment"] == "opt-in":
                    if not tool["opt-tag"] in settings["engines"]["remotes"][remote]["tool-opt-in-tags"]:
                        logger.info("Not enabling tool %s on remote %s due to missing opt-in tag" % (tool["name"], remote))
                        continue
                    else:
                        logger.info("Tool %s opt-in tag found for remote %s" % (tool["name"], remote))
                elif tool["deployment"] == "opt-out":
                    if tool["opt-tag"] in settings["engines"]["remotes"][remote]["tool-opt-out-tags"]:
                        logger.info("Not enabling tool %s on remote %s due to present opt-out tag" % (tool["name"], remote))
                        continue
                    else:
                        logger.info("Tool %s opt-out tag not found for remote %s" % (tool["name"], remote))

            logger.info("Enabling tool %s on remote %s" % (tool["name"], remote))

            profiler_id = args.endpoint_label + "-" + tool["name"] + "-" + str(profiler_count)

            if not "profiler" in settings["engines"]["remotes"][remote]["roles"]:
                settings["engines"]["remotes"][remote]["roles"]["profiler"] = {
                    "ids": []
                }

            settings["engines"]["remotes"][remote]["roles"]["profiler"]["ids"].append(profiler_id)

            profiler_engine_name = "profiler-" + profiler_id
            logger.info("Tool %s will be engine %s on remote %s" % (tool["name"], profiler_engine_name, remote))
            
            settings["engines"]["new-followers"].append(profiler_engine_name)

            if not tool["name"] in settings["engines"]["profiler-mapping"]:
                settings["engines"]["profiler-mapping"][tool["name"]] = {
                    "name": tool["name"],
                    "ids": []
                }
            settings["engines"]["profiler-mapping"][tool["name"]]["ids"].append(profiler_id)

    for remote in settings["engines"]["remotes"].keys():
        for role in settings["engines"]["remotes"][remote]["roles"].keys():
            for id in settings["engines"]["remotes"][remote]["roles"][role]["ids"]:
                engine_name = "%s-%s" % (role, id)
                settings["engines"]["remotes"][remote]["engines"].append(engine_name)

    endpoints.log_settings(settings, mode = "engines")

    logger.info("Adding new profiler engines to endpoint settings")
    for remote in settings["engines"]["remotes"].keys():
        if not "profiler" in settings["engines"]["remotes"][remote]["roles"]:
            continue

        settings["engines"]["remotes"][remote]["run-file-idx"].sort()

        remote_profilers = 0
        for tmp_run_file_idx in settings["engines"]["remotes"][remote]["run-file-idx"]:
            for engine in settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][tmp_run_file_idx]["engines"]:
                if engine["role"] == "profiler":
                    run_file_idx = tmp_run_file_idx
                    remote_profilers += 1

        if remote_profilers == 0:
            run_file_idx = settings["engines"]["remotes"][remote]["run-file-idx"][0]
        elif remote_profilers > 1:
            raise ValueError("You cannot have more than one profiler role for remote %s" % (remote))

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

    endpoints.log_settings(settings, mode = "endpoint", endpoint_index = args.endpoint_index)

    return 0

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
    thread_logger("Starting image pull thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote = None
        try:
            remote = work_queue.get(block = False)
        except queue.Empty:
            thread_logger("Received a work queue empty exception")
            break

        if remote is None:
            thread_logger("Received a null job", log_level = "warning")
            continue

        job_count += 1
        thread_logger("Retrieved remote %s" % (remote))

        my_unique_remote = settings["engines"]["remotes"][remote]
        my_run_file_remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][my_unique_remote["run-file-idx"][0]]

        thread_logger("Remote user is %s" % (my_run_file_remote["config"]["settings"]["remote-user"]), remote_name = remote)

        with endpoints.remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as c:
            for image_info in my_unique_remote["images"]:
                auth_arg = ""
                remote_auth_file = settings["dirs"]["remote"]["run"] + "/pull-token.json"
                if "pull-token" in image_info:
                    thread_logger("Image %s requires pull token %s" % (image_info["image"], image_info["pull-token"]), remote_name = remote)
                    result = c.put(image_info["pull-token"], remote_auth_file)
                    thread_logger("Copied %s to %s:%s" % (image_info["pull-token"], remote, remote_auth_file), remote_name = remote)
                    auth_arg = "--authfile=" + remote_auth_file

                result = endpoints.run_remote(c, "podman pull " + auth_arg + " " + image_info["image"])
                loglevel = "info"
                if result.exited != 0:
                    loglevel = "error"
                thread_logger("Attempted to pull %s with return code %d:\nstdout:\n%sstderr:\n%s" % (image_info["image"], result.exited, result.stdout, result.stderr), log_level = loglevel, remote_name = remote)
                rc += result.exited

                if "pull-token" in image_info:
                    result = endpoints.run_remote(c, "rm -v " + remote_auth_file)
                    log_level = "info"
                    if result.exited != 0:
                        loglevel = "error"
                    thread_logger("Attempted to remove %s with return code %d:\nstdout:\n%sstderr:\n%s" % (remote_auth_file, result.exited, result.stdout, result.stderr), log_level = loglevel, remote_name = remote)

                result = endpoints.run_remote(c, "echo '" + image_info["image"] + " " + str(int(time.time())) + " " + settings["misc"]["run-id"] + "' >> " + settings["dirs"]["remote"]["base"] + "/remotehosts-container-image-census")
                loglevel = "info"
                if result.exited != 0:
                    loglevel = "error"
                thread_logger("Recorded usage for %s in the census with return code %d:\nstdout:\n%sstderr:\n%s" % (image_info["image"], result.exited, result.stdout, result.stderr), log_level = loglevel, remote_name = remote)
                rc += result.exited

        thread_logger("Notifying work queue that job processing is complete", remote_name = remote)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger("Stopping image pull thread after processing %d job(s)" % (job_count))
    return

def thread_logger(msg, log_level = "info", remote_name = None, engine_name = None, log_prefix = None):
    """
    Logging function with specific metadata the identifies the thread/remote/engine the message should be associated with

    Args:
        msg (str): The message to log
        log_level (str): Which logging level to use for the message
        remote_name (str): An optional remote identifier to label the message with
        engine_name (str): An optional engine identifier to label the message with
        log_prefix (str): An optional prefix to apply to the message

    Globals:
        logger: a logger instance

    Returns:
        None
    """
    remote_label = ""
    if not remote_name is None:
        remote_label = "[Remote %s]" % (remote_name)
    engine_label = ""
    if not engine_name is None:
        engine_label = "[Engine %s]" % (engine_name)
    prefix_label =  ""
    if not log_prefix is None:
        prefix_label = "[%s]" % (log_prefix)
    msg = "%s%s%s %s" % (remote_label, engine_label, prefix_label, msg)

    match log_level:
        case "debug":
            return logger.debug(msg, stacklevel = 2)
        case "error":
            return logger.error(msg, stacklevel = 2)
        case "info":
            return logger.info(msg, stacklevel = 2)
        case "warning":
            return logger.warning(msg, stacklevel = 2)
        case _:
            raise ValueError("Uknown log_level '%s' in thread_logger" % (log_level))

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
        rc (int): an aggregated return code value from all the launched worker threads
    """
    thread_logger("Creating thread pool")
    thread_logger("Thread pool description: %s" % (description))
    thread_logger("Thread pool acronym: %s" % (acronym))
    thread_logger("Thread pool size: %d" % (worker_threads_count))

    if not endpoint_defaults["maximum-worker-threads-count"] is None and worker_threads_count > endpoint_defaults["maximum-worker-threads-count"]:
        thread_logger("Reducing thread pool size to %d in accordance with maximum thread pool size definition" % (endpoint_defaults["maximum-worker-threads-count"]))
        worker_threads_count = endpoint_defaults["maximum-worker-threads-count"]

    worker_threads = [None] * worker_threads_count
    worker_threads_rcs = [None] * worker_threads_count

    thread_logger("Launching %d %s (%s)" % (worker_threads_count, description, acronym))
    for thread_id in range(0, worker_threads_count):
        if work.empty():
            thread_logger("Aborting %s launch because no more work to do" % (acronym))
            break
        thread_name = "%s-%d" % (acronym, thread_id)
        thread_logger("Creating and starting thread %s" % (thread_name))
        try:
            worker_threads[thread_id] = threading.Thread(target = worker_thread_function, args = (thread_id, work, worker_threads_rcs), name = thread_name)
            worker_threads[thread_id].start()
        except RuntimeError as e:
            thread_logger("Failed to create and start thread %s due to exception '%s'" % (thread_name, str(e)), log_level = "error")

    thread_logger("Waiting for all %s work jobs to be consumed" % (acronym))
    work.join()
    thread_logger("All %s work jobs have been consumed" % (acronym))

    thread_logger("Joining %s" % (acronym))
    for thread_id in range(0, worker_threads_count):
        thread_name = "%s-%d" % (acronym, thread_id)
        if not worker_threads[thread_id] is None:
            if not worker_threads[thread_id].native_id is None:
                worker_threads[thread_id].join()
                thread_logger("Joined thread %s" % (thread_name))
            else:
                thread_logger("Skipping join of thread %s because it was not started" % (thread_name), log_level = "warning")
        else:
            thread_logger("Skipping join of thread %s because it does not exist" % (thread_name), log_level = "warning")

    thread_logger("Return codes for each %s:\n%s" % (acronym, endpoints.dump_json(worker_threads_rcs)))

    rc = 0
    for thread_rc in worker_threads_rcs:
        rc += thread_rc
    thread_logger("Aggregate return code for %s: %d" % (acronym, rc))

    return rc

def remotes_pull_images():
    """
    Handle the pulling of images necessary to run the test to the remotes

    Args:
        None

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        rc (int): defines if the image pull was successfull (0) or not (!=0)
    """
    logger.info("Determining which images to pull to which remotes")
    for remote in settings["engines"]["remotes"].keys():
        if not "raw-images" in settings["engines"]["remotes"][remote]:
            settings["engines"]["remotes"][remote]["raw-images"] = []

        for role in settings["engines"]["remotes"][remote]["roles"].keys():
            for id in settings["engines"]["remotes"][remote]["roles"][role]["ids"]:
                userenv = None
                image = None

                if role == "profiler":
                    userenv = endpoints.get_profiler_userenv(settings, id)
                else:
                    for remote_idx in settings["engines"]["remotes"][remote]["run-file-idx"]:
                        for tmp_role in settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][remote_idx]["engines"]:
                            if tmp_role["role"] == role and id in tmp_role["ids"]:
                                userenv = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][remote_idx]["config"]["settings"]["userenv"]
                                break
                        if userenv is not None:
                            break

                if userenv is None:
                    logger.error("Cound not find userenv for remote %s with role %s and id %s" % (remote, role, str(id)))
                    print("could not find userenv for " + endpoint)
                else:
                    image = endpoints.get_engine_id_image(settings, role, id, userenv)

                if image is None:
                    logger.error("Could not find image for remote %s with role %s and id %s" % (remote, role, str(id)))
                else:
                    settings["engines"]["remotes"][remote]["raw-images"].append(image)

        settings["engines"]["remotes"][remote]["raw-images"] = list(set(settings["engines"]["remotes"][remote]["raw-images"]))
        settings["engines"]["remotes"][remote]["images"] = []
        for image in settings["engines"]["remotes"][remote]["raw-images"]:
            image_info = dict()
            image_split = image.split("::")
            image_info["image"] = image_split[0]
            if len(image_split) > 1:
                logger.info("Found image %s with pull token %s" % (image_split[0], image_split[1]))
                image_info["pull-token"] = image_split[1]
            settings["engines"]["remotes"][remote]["images"].append(image_info)

    endpoints.log_settings(settings, mode = "engines")

    image_pull_work = queue.Queue()
    for remote in settings["engines"]["remotes"].keys():
        image_pull_work.put(remote)
    worker_threads_count = len(settings["engines"]["remotes"])

    rc = create_thread_pool("Image Pull Worker Threads", "IPWT", image_pull_work, worker_threads_count, image_pull_worker_thread)

    return rc

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
    thread_logger("Starting remote mkdirs thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote = None
        try:
            remote = work_queue.get(block = False)
        except queue.Empty:
            thread_logger("Received a work queue empty exception")
            break

        if remote is None:
            thread_logger("Received a null job", log_level = "warning")
            continue

        job_count += 1
        thread_logger("Retrieved remote %s" % (remote))

        my_unique_remote = settings["engines"]["remotes"][remote]
        my_run_file_remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][my_unique_remote["run-file-idx"][0]]

        with endpoints.remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as con:
            for remote_dir in settings["dirs"]["remote"].keys():
                result = endpoints.run_remote(con, "mkdir --parents --verbose " + settings["dirs"]["remote"][remote_dir])
                thread_logger("Remote attempted to mkdir %s with return code %d:\nstdout:\n%sstderr:\n%s" % (settings["dirs"]["remote"][remote_dir], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)
                rc += result.exited

        thread_logger("Notifying work queue that job processing is complete", remote_name = remote)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger("Stopping remote mkdirs thread after processing %d job(s)" % (job_count))
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
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    logger.info("Setting the total cpu-partitions per unique remote")
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

    endpoints.log_settings(settings, mode = "engines")

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
    thread_logger("Running create podman", remote_name = remote_name, engine_name = engine_name)

    local_env_file_name = settings["dirs"]["local"]["endpoint"] + "/" + engine_name + "_env.txt"
    thread_logger("Creating env file %s" % (local_env_file_name), remote_name = remote_name, engine_name = engine_name)
    with open(local_env_file_name, "w", encoding = "ascii") as env_file:
        env_file.write("base_run_dir=" + settings["dirs"]["local"]["base"] + "\n")
        env_file.write("cs_label=" + engine_name + "\n")
        env_file.write("cpu_partitioning=" + str(cpu_partitioning) + "\n")
        if cpu_partitioning == 1:
            with settings["engines"]["remotes"][remote]["cpu-partitions-idx-lock"]:
                cpu_partition_idx = settings["engines"]["remotes"][remote]["cpu-partitions-idx"]
                settings["engines"]["remotes"][remote]["cpu-partitions-idx"] += 1

            thread_logger("Allocated cpu-partition index %d" % (cpu_partition_idx), remote_name = remote_name, engine_name = engine_name)
            env_file.write("cpu_partition_index=" + str(cpu_partition_idx) + "\n")
            env_file.write("cpu_partitions=" + str(settings["engines"]["remotes"][remote]["total-cpu-partitions"]) + "\n")
        if role != "profiler":
            env_file.write("disable_tools=1" + "\n")
        env_file.write("endpoint=remotehosts" + "\n")
        env_file.write("endpoint_run_dir=" + settings["dirs"]["local"]["endpoint"] + "\n")
        env_file.write("engine_script_start_timeout=" + str(args.engine_script_start_timeout) + "\n")
        env_file.write("max_sample_failures=" + str(args.max_sample_failures) + "\n")
        env_file.write("rickshaw_host=" + controller_ip + "\n")
        env_file.write("roadblock_id=" + args.roadblock_id + "\n")
        env_file.write("roadblock_passwd=" + args.roadblock_passwd + "\n")
        env_file.write("ssh_id=" + settings["misc"]["ssh-private-key"] + "\n")
        if "env-vars" in podman_settings:
            for env_var in podman_settings["env-vars"]:
                env_file.write(env_var["var"] + "=" + env_var["value"])

    remote_env_file_name = settings["dirs"]["remote"]["cfg"] + "/" + engine_name + "_env.txt"
    result = connection.put(local_env_file_name, remote_env_file_name)
    thread_logger("Copied %s to %s:%s" % (local_env_file_name, remote, remote_env_file_name), remote_name = remote_name, engine_name = engine_name)

    mandatory_mounts = [
        {
            "src": settings["dirs"]["remote"]["data"],
            "dest": "/shared-engines-dir"
        },
        {
            "src": "/lib/firmware"
        },
        {
            "src": "/lib/modules"
        },
        {
            "src": "/usr/src"
        },
        {
            "src": "/var/run"
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
        if isinstance(podman_settings["device"], list):
            for podman_device in podman_settings["device"]:
                create_cmd.append("--device " + podman_device)
        else:
            create_cmd.append("--device " + podman_settings["device"])

    if "shm-size" in podman_settings:
        create_cmd.append("--shm-size " + podman_settings["shm-size"])
    else:
        create_cmd.append("--ipc=host") # only works when not using shm

    if "pids-limit" in podman_settings:
        create_cmd.append("--pids-limit " + str(podman_settings["pids-limit"]))

    for mount in mandatory_mounts + host_mounts:
        if not "dest" in mount:
            mount["dest"] = mount["src"]

        create_cmd.append("--mount=type=bind,source=" + mount["src"] + ",destination=" + mount["dest"])

    create_cmd.append(image)

    thread_logger("Podman create command is:\n%s" % (endpoints.dump_json(create_cmd)), remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, " ".join(create_cmd))
    thread_logger("Creating container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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
    thread_logger("Running create chroot", remote_name = remote_name, engine_name = engine_name)

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

    thread_logger("Podman create command is:\n%s" % (endpoints.dump_json(create_cmd)), remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, " ".join(create_cmd))
    thread_logger("Creating container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)
    if result.exited == 0:
        create_info["id"] = result.stdout.rstrip('\n')
    else:
        return create_info

    mount_cmd = [ "podman",
                  "mount",
                  container_name ]

    result = endpoints.run_remote(connection, " ".join(mount_cmd))
    thread_logger("Mounting container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)
    if result.exited == 0:
        create_info["mount"] = result.stdout.rstrip('\n')
    else:
        return create_info

    thread_logger("Container mount is '%s'" % (create_info["mount"]), remote_name = remote_name, engine_name = engine_name)

    mandatory_mounts = [
        {
            "src": settings["dirs"]["remote"]["data"],
            "dest": "/shared-engines-dir",
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

        thread_logger("Procesing mount:\n%s" % (endpoints.dump_json(mount)), remote_name = remote_name, engine_name = engine_name)

        result = endpoints.run_remote(connection, "mkdir --parents --verbose " + mount["dest"])
        thread_logger("Creating '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["dest"], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

        if mount["rbind"]:
            result = endpoints.run_remote(connection, "mount --verbose --options rbind " + mount["src"] + " " + mount["dest"])
            thread_logger("rbind mounting '%s' to '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["src"], mount["dest"], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

            result = endpoints.run_remote(connection, "mount --verbose --make-rslave " + mount["dest"])
            thread_logger("making rslave '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["dest"], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

            create_info["mounts"]["rbind"].append(mount["dest"])
        else:
            result = endpoints.run_remote(connection, "mount --verbose --options bind " + mount["src"] + " " + mount["dest"])
            thread_logger("bind mounting '%s' to '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["src"], mount["dest"], result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

            create_info["mounts"]["regular"].append(mount["dest"])

    local_ssh_private_key_file = settings["dirs"]["local"]["conf"] + "/ssh/" + settings["misc"]["run-id"]
    remote_ssh_private_key_file = create_info["mount"] + "/shared-engines-dir/" + "rickshaw_ssh_id"
    result = connection.put(local_ssh_private_key_file, remote_ssh_private_key_file)
    thread_logger("Copied %s to %s:%s" % (local_ssh_private_key_file, connection.host, remote_ssh_private_key_file), remote_name = remote_name, engine_name = engine_name)

    for etc_file in [ "hosts", "resolv.conf" ]:
        src_etc_file = "/etc/" + etc_file
        dst_etc_dir = create_info["mount"] + "/etc"
        result = endpoints.run_remote(connection, "cp --verbose " + src_etc_file + " " + dst_etc_dir)
        thread_logger("Remotely copied %s to %s" % (src_etc_file, dst_etc_dir), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    thread_logger("chroot create info:\n%s" % (endpoints.dump_json(create_info)), remote_name = remote_name, engine_name = engine_name)

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
    thread_logger("Running start podman", remote_name = remote_name, engine_name = engine_name)

    start_cmd = [
        "podman",
        "start",
        container_name
    ]

    result = endpoints.run_remote(connection, " ".join(start_cmd))
    thread_logger("Starting container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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
    thread_logger("Running start chroot", remote_name = remote_name, engine_name = engine_name)

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
        "--max-sample-failures=" + str(args.max_sample_failures),
        "--rickshaw-host=" + controller_ip,
        "--roadblock-id=" + args.roadblock_id,
        "--roadblock-passwd=" + args.roadblock_passwd,
    ])

    if cpu_partitioning == 1:
        with settings["engines"]["remotes"][remote]["cpu-partitions-idx-lock"]:
            cpu_partition_idx = settings["engines"]["remotes"][remote]["cpu-partitions-idx"]
            settings["engines"]["remotes"][remote]["cpu-partitions-idx"] += 1

        thread_logger("Allocated cpu-partition index %d" % (cpu_partition_idx), remote_name = remote_name, engine_name = engine_name)
        start_cmd.extend([
            "--cpu-partition-index=" + str(cpu_partition_idx),
            "--cpu-partitions=" + str(settings["engines"]["remotes"][remote]["total-cpu-partitions"])
        ])

    start_cmd.extend([
        ">" + settings["dirs"]["remote"]["logs"] + "/" + engine_name + ".txt",
        "&"
    ])

    thread_logger("chroot start command is:\n%s" % (endpoints.dump_json(start_cmd)), remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, " ".join(start_cmd))
    thread_logger("Starting chroot with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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
    thread_logger("Starting launch engines thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote_idx = None
        try:
            remote_idx = work_queue.get(block = False)
        except queue.Empty:
            thread_logger("Received a work queue empty exception")
            break

        if remote_idx is None:
            thread_logger("Received a null job", log_level = "warning")
            continue

        job_count += 1

        remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][remote_idx]
        remote_name = "%s-%s" % (remote["config"]["host"], remote_idx)

        thread_logger("Processing remote '%s' at index %d" % (remote["config"]["host"], remote_idx), remote_name = remote_name)
        thread_logger("Remote user is %s" % (remote["config"]["settings"]["remote-user"]), remote_name = remote_name)

        with endpoints.remote_connection(remote["config"]["host"], remote["config"]["settings"]["remote-user"]) as con:
            for engine in remote["engines"]:
                for engine_id in engine["ids"]:
                    engine_name = "%s-%s" % (engine["role"], str(engine_id))
                    container_name = "%s_%s" % (settings["misc"]["run-id"], engine_name)
                    thread_logger("Creating engine '%s'" % (engine_name), remote_name = remote_name, engine_name = engine_name)
                    thread_logger("Container name will be '%s'" % (container_name), remote_name = remote_name, engine_name = engine_name)

                    if engine["role"] == "profiler":
                        userenv = endpoints.get_profiler_userenv(settings, engine_id)
                    else:
                        userenv = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][remote_idx]["config"]["settings"]["userenv"]

                    image = endpoints.get_engine_id_image(settings, engine["role"], engine_id, userenv)
                    if image is None:
                        thread_logger("Could not determine image", log_level = "error", remote_name = remote_name, engine_name = engine_name)
                        continue
                    else:
                        image_split = image.split("::")
                        image = image_split[0]
                        thread_logger("Image is '%s'" % (image), remote_name = remote_name, engine_name = engine_name)

                    osruntime = None
                    if engine["role"] == "profiler":
                        osruntime = "podman"
                    else:
                        osruntime = remote["config"]["settings"]["osruntime"]
                    thread_logger("osruntime is '%s'" % (osruntime), remote_name = remote_name, engine_name = engine_name)

                    thread_logger("host-mounts is %s" % (remote["config"]["settings"]["host-mounts"]), remote_name = remote_name, engine_name = engine_name)

                    result = endpoints.run_remote(con, "podman ps --all --filter 'name=" + container_name + "' --format '{{.Names}}'")
                    thread_logger("Check for existing container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), remote_name = remote_name, engine_name = engine_name)
                    if result.exited != 0:
                        thread_logger("Check for existing container exited with non-zero return code %d" % (result.exited), log_level = "error", remote_name = remote_name, engine_name = engine_name)
                    if result.stdout.rstrip('\n') == container_name:
                        thread_logger("Found existing container '%s'" % (container_name), log_level = "warning", remote_name = remote_name, engine_name = engine_name)

                        result = endpoints.run_remote(con, "podman rm --force " + container_name)
                        thread_logger("Forced removal of existing container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), remote_name = remote_name, engine_name = engine_name)
                        if result.exited != 0:
                            thread_logger("Forced removal of existing container exited with non-zero return code %d" % (result.exited), log_level = 'error', remote_name = remote_name, engine_name = engine_name)

                    match osruntime:
                        case "podman":
                            podman_settings = {}
                            cpu_partitioning = None
                            if engine["role"] == "profiler":
                                cpu_partitioning = 0
                            else:
                                podman_settings = remote["config"]["settings"]["podman-settings"]
                                if remote["config"]["settings"]["cpu-partitioning"]:
                                    cpu_partitioning = 1
                                else:
                                    cpu_partitioning = 0
                            thread_logger("cpu-partitioning is '%s'" % (str(cpu_partitioning)), remote_name = remote_name, engine_name = engine_name)

                            numa_node = None
                            if not remote["config"]["settings"]["numa-node"] is None:
                                numa_node = remote["config"]["settings"]["numa-node"]
                            thread_logger("numa-node is '%s'" % (str(numa_node)), remote_name = remote_name, engine_name = engine_name)

                            create_podman(thread_name, remote_name, engine_name, container_name, con, remote["config"]["host"], remote["config"]["settings"]["controller-ip-address"], engine["role"], image, cpu_partitioning, numa_node, remote["config"]["settings"]["host-mounts"], podman_settings)
                        case "chroot":
                            if not "chroots" in remote:
                                remote["chroots"] = dict()
                            create_info = create_chroot(thread_name, remote_name, engine_name, container_name, con, image, remote["config"]["settings"]["host-mounts"])
                            remote["chroots"][engine_name] = create_info

            for engine in remote["engines"]:
                for engine_id in engine["ids"]:
                    engine_name = "%s-%s" % (engine["role"], str(engine_id))
                    container_name = "%s_%s" % (settings["misc"]["run-id"], engine_name)
                    thread_logger("Starting engine '%s'" % (engine_name), remote_name = remote_name, engine_name = engine_name)

                    osruntime = None
                    if engine["role"] == "profiler":
                        osruntime = "podman"
                    else:
                        osruntime = remote["config"]["settings"]["osruntime"]
                    thread_logger("osruntime is '%s'" % (osruntime), remote_name = remote_name, engine_name = engine_name)

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
                            thread_logger("cpu-partitioning is '%s'" % (str(cpu_partitioning)), remote_name = remote_name, engine_name = engine_name)

                            numa_node = None
                            if not remote["config"]["settings"]["numa-node"] is None:
                                numa_node = remote["config"]["settings"]["numa-node"]
                            thread_logger("numa-node is '%s'" % (str(numa_node)), remote_name = remote_name, engine_name = engine_name)

                            start_chroot(thread_name, remote_name, engine_name, container_name, con, remote["config"]["host"], remote["config"]["settings"]["controller-ip-address"], cpu_partitioning, numa_node, remote["chroots"][engine_name]["mount"])

        thread_logger("Notifying work queue that job processing is complete", remote_name = remote_name)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger("Stopping launch engines thread after processing %d job(s)" % (job_count))
    return

def launch_engines():
    """
    Handle the launching of engines to run the test on the remotes

    Args:
        None

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    logger.info("Creating threadpool to handle engine launching")

    launch_engines_work = queue.Queue()
    for remote_idx,remote in enumerate(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"]):
        launch_engines_work.put(remote_idx)
    worker_threads_count = len(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"])

    create_thread_pool("Launch Engines Worker Threads", "LEWT", launch_engines_work, worker_threads_count, launch_engines_worker_thread)

    return 0

def engine_init():
    """
    Construct messages to initialize the engines with metadata specific to them

    Args:
        None

    Globals:
        logger: a logger instance
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
    logger.info("Writing follower env-vars messages to %s" % (env_vars_msg_file))
    env_vars_msgs_json = endpoints.dump_json(env_vars_msgs)
    with open(env_vars_msg_file, "w", encoding = "ascii") as env_vars_msg_file_fp:
        env_vars_msg_file_fp.write(env_vars_msgs_json)
    logger.info("Contents of %s:\n%s" % (env_vars_msg_file, env_vars_msgs_json))

    return env_vars_msg_file

def test_start(msgs_dir, test_id, tx_msgs_dir):
    """
    Perform endpoint responsibilities that must be completed prior to running an iteration test sample

    Args:
        msgs_dir (str): The directory look for received messages in
        test_id (str): A string of the form "<iteration>:<sample>:<attempt>" used to identify the current test
        tx_msgs_dir (str): The directory where to write queued messages for transmit

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    logger.info("Running test_start() for '%s' (<iteration>-<sample>-<attempt>)" % (test_id))

    this_msg_file = msgs_dir + "/" + test_id + ":server-start-end.json"
    path = Path(this_msg_file)

    if path.exists() and path.is_file():
        logger.info("Found '%s'" % (this_msg_file))

        msgs_json,err = load_json_file(this_msg_file)
        if not msgs_json is None:
            if "received" in msgs_json:
                for msg in msgs_json["received"]:
                    if msg["payload"]["message"]["command"] == "user-object":
                        if "svc" in msg["payload"]["message"]["user-object"] and "ports" in msg["payload"]["message"]["user-object"]["svc"]:
                            server_engine = msg["payload"]["sender"]["id"]
                            client_engine = re.sub(r"server", r"client", server_engine)

                            logger.info("Found a service message from server engine %s to client engine %s" % (server_engine, client_engine))

                            server_remote = None
                            client_remote = None
                            server_mine = False
                            client_mine = False

                            process_msg = False
                            for remote in settings["engines"]["remotes"].keys():
                                if server_remote is None and server_engine in settings["engines"]["remotes"][remote]["engines"]:
                                    server_remote = remote
                                    server_mine = True
                                    logger.info("The server engine '%s' is running from this endpoint on remote '%s'" % (server_engine, server_remote))

                                if client_remote is None and client_engine in settings["engines"]["remotes"][remote]["engines"]:
                                    client_remote = remote
                                    client_mine = True
                                    logger.info("The client engine '%s' is running from this endpoint on remote '%s'" % (client_engine, client_remote))

                                if not server_remote is None and not client_remote is None:
                                    break

                            if not server_mine and not client_mine:
                                logger.info("Neither the server engine '%s' or the client engine '%s' is running from this endpoint, ignoring" % (server_engine, client_engine))
                            elif not server_mine and client_mine:
                                logger.info("Only the client engine '%s' is mine, ignoring" % (client_engine))
                            elif server_mine and not client_mine:
                                logger.info("Only the server engine '%s' is mine, processing" % (server_engine))
                                process_msg = True
                            elif server_mine and client_mine:
                                logger.info("Both the server engine '%s' and the client engine '%s' are mine" % (server_engine, client_engine))

                                if server_remote == client_remote:
                                    logger.info("My server engine '%s' and my client engine '%s' are on the same remote '%s', nothing to do" % (server_engine, client_engine, server_remote))
                                else:
                                    logger.info("My server engine '%s' is on remote '%s' and my client engine '%s' is on remote '%s', processing" % (server_engine, server_remote, client_engine, client_remote))
                                    process_msg = True

                            if process_msg:
                                logger.info("Processing received message:\n%s" % (endpoints.dump_json(msg["payload"])))

                                # punching a hole through the fireall would go here

                                logger.info("Creating a message to send to the client engine '%s' with IP and port info" % (client_engine))
                                msg = endpoints.create_roadblock_msg("follower", client_engine, "user-object", msg["payload"]["message"]["user-object"])

                                msg_file = tx_msgs_dir + "/server-ip-" + server_engine + ".json"
                                logger.info("Writing follower service-ip message to '%s'" % (msg_file))
                                with open(msg_file, "w", encoding = "ascii") as msg_file_fp:
                                    msg_file_fp.write(endpoints.dump_json(msg))
        else:
            logger.error("Failed to load '%s' due to error '%s'" % (this_msg_file, str(err)))
    else:
        logger.info("Could not find '%s'" % (this_msg_file))

    logger.info("Returning from test_start() for '%s' (<iteration>-<sample>-<attempt>)" % (test_id))
    return

def test_stop(test_id):
    """
    Perform endpoint responsibilties that must be completed after an iteration test sample

    Args:
        test_id (str): A string of the form "<iteration>:<sample>:<attempt>" used to identify the current test

    Globals:
        logger: a logger instance

    Returns:
        None
    """
    logger.info("Running test_stop() for '%s' (<iteration>-<sample>-<attempt>)" % (test_id))
    logger.info("...nothing to do here, run along...")
    logger.info("Returning from test_stop() for '%s' (<iteration>-<sample>-<attempt>)" % (test_id))
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
    thread_logger("Collecting podman log", remote_name = remote_name, engine_name = engine_name)

    cmd_retries = 5
    cmd_attempt = 1
    cmd_rc = 1

    while cmd_rc != 0 and cmd_attempt <= cmd_retries:
        result = endpoints.run_remote(connection, "podman logs --timestamps " + container_name + " | xz -c | base64")
        if result.exited != 0:
            thread_logger("Collecting podman log for %s failed with return code %d on attempt %d of %d:\nstdout:\n%sstderr:\n%s" %
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
            thread_logger("Wrote podman log to %s" % (log_file), remote_name = remote_name, engine_name = engine_name)
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
    thread_logger("Collecting chroot log", remote_name = remote_name, engine_name = engine_name)

    remote_log_file = settings["dirs"]["remote"]["logs"] + "/" + engine_name + ".txt"
    result = endpoints.run_remote(connection, "cat " + remote_log_file + " | xz -c | base64")
    if result.exited != 0:
        thread_logger("Collecting chroot log for %s failed with return code %d:\nstdout:\n%sstderr:\n%s" %
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
        thread_logger("Wrote chroot log to %s" % (log_file), remote_name = remote_name, engine_name = engine_name)

        result = endpoints.run_remote(connection, "rm --verbose " + remote_log_file)
        thread_logger("Removal of engine log for '%s' gave return code %d:\nstdout:\n%stderr:\n%s" %
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
    thread_logger("Removal of rickshaw settings '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (remote_rickshaw_settings, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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
    remote_ssh_private_key = settings["dirs"]["remote"]["data"] + "/rickshaw_ssh_id"
    result = endpoints.run_remote(connection, "if [ -e \"" + remote_ssh_private_key + "\" ]; then rm --verbose \"" + remote_ssh_private_key + "\"; else echo \"ssh private key already removed\"; fi")
    thread_logger("Removal of ssh private key '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (remote_ssh_private_key, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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
    thread_logger("Destroying podman", remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, "podman rm --force " + container_name)
    thread_logger("Removal of pod '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (engine_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    remote_env_file = settings["dirs"]["remote"]["cfg"] + "/" + engine_name + "_env.txt"
    result = endpoints.run_remote(connection, "rm --verbose " + remote_env_file)
    thread_logger("Removal of env file  '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (remote_env_file, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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
    thread_logger("Destroying chroot", remote_name = remote_name, engine_name = engine_name)

    for mount in chroot_info["mounts"]["regular"]:
        result = endpoints.run_remote(connection, "umount --verbose " + mount)
        thread_logger("regular unmounting of '%s' resulted in record code %d:\nstdout:\n%sstderr:\n%s" % (mount, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    for mount in chroot_info["mounts"]["rbind"]:
        result = endpoints.run_remote(connection, "umount --verbose --recursive " + mount)
        thread_logger("recursive unmounting of '%s' resulted in record code %d:\nstdout:\n%sstderr:\n%s" % (mount, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    result = endpoints.run_remote(connection, "podman rm --force " + container_name)
    thread_logger("Removal of pod '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (engine_name, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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
    thread_logger("Removing image '%s'" % (image), remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman rmi " + image)
    thread_logger("Removing podman image '%s' gave return code %d:\nstdout:\n%sstderr:\n%s" % (image, result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, log_prefix = log_prefix)

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
    thread_logger("Performing container image management", remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman images --all")
    thread_logger("All podman images on this remote host before running image manager:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

    images = dict()
    images["rickshaw"] = dict()
    images["podman"] = dict()
    images["quay"] = dict()

    result = endpoints.run_remote(connection, "cat /var/lib/crucible/remotehosts-container-image-census")
    if result.exited != 0:
        thread_logger("Reading image census returned %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = "error", remote_name = remote_name, log_prefix = log_prefix)
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
    thread_logger("images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman images --format='{{.Repository}}:{{.Tag}}|{{.CreatedAt}}'")
    if result.exited != 0:
        thread_logger("Getting podman images returned %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = "error", remote_name = remote_name, log_prefix = log_prefix)
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
    thread_logger("images[podman]:\n%s" % (endpoints.dump_json(images["podman"])), remote_name = remote_name, log_prefix = log_prefix)

    expiration_create_registries = dict()
    expiration_refresh_registries = list()
    for registry in [ "public", "private" ]:
        if registry in settings["crucible"]["registries"]["engines"]:
            if "quay" in settings["crucible"]["registries"]["engines"][registry]:
                if "refresh-expiration" in settings["crucible"]["registries"]["engines"][registry]["quay"]:
                    expiration_refresh_registries.append(registry)

                if "w" in settings["crucible"]["registries"]["engines"][registry]["quay"]["expiration-length"]:
                    expiration_weeks = int(settings["crucible"]["registries"]["engines"][registry]["quay"]["expiration-length"].rstrip("w"))
                    image_created_expiration = endpoints.image_created_expiration_gmepoch(expiration_weeks, "weeks")
                    expiration_create_registries[settings["crucible"]["registries"]["engines"][registry]["url"]] = image_created_expiration
                    thread_logger("Images from the %s registry evaluated by their creation data will be considered expired if it is before %d (%d weeks ago)" % (registry, image_created_expiration, expiration_weeks), remote_name = remote_name, log_prefix = log_prefix)
                elif "d" in settings["crucible"]["registries"]["engines"][registry]["quay"]["expiration-length"]:
                    expiration_days = int(settings["crucible"]["registries"]["engines"][registry]["quay"]["expiration-length"].rstrip("d"))
                    image_created_expiration = endpoints.image_created_expiration_gmepoch(expiration_days, "days")
                    expiration_create_registries[settings["crucible"]["registries"]["engines"][registry]["url"]] = image_created_expiration
                    thread_logger("Images from the %s registry evaluated by their creation data will be considered expired if it is before %d (%d days ago)" % (registry, image_created_expiration, expiration_days), remote_name = remote_name, log_prefix = log_prefix)

    if len(expiration_refresh_registries) > 0:
        thread_logger("Using the quay API to look for image expiration timestamps in these registries: %s" % (expiration_refresh_registries), remote_name = remote_name, log_prefix = log_prefix)

        for image in images["podman"].keys():
            image_parts = image.split(":")

            registry_match = None
            for registry in expiration_refresh_registries:
                if settings["crucible"]["registries"]["engines"][registry]["url"] == image_parts[0]:
                    registry_match = registry
                    break

            if registry_match is None:
                thread_logger("Could not find a quay API URL to check for an image expiration timestamp for image '%s'" % (image), remote_name = remote_name, log_prefix = log_prefix)
            else:
                thread_logger("Using the '%s' registry to run a quay API call to get the image's ('%s') expiration timestamp" % (registry_match, image), remote_name = remote_name, log_prefix = log_prefix)

                get_request = requests.get(settings["crucible"]["registries"]["engines"][registry]["quay"]["refresh-expiration"]["api-url"] + "/tag", params = { "onlyActiveTags": True, "specificTag": image_parts[1] })

                query_log_level = "info"
                if get_request.status_code != requests.codes.ok:
                    query_log_level = "warning"

                thread_logger("Quay API query for %s returned status code %d" % (image, get_request.status_code), log_level = query_log_level, remote_name = remote_name, log_prefix = log_prefix)

                if get_request.status_code == requests.codes.ok:
                    image_json = get_request.json()
                    if len(image_json["tags"]) == 1:
                        images["quay"][image] = image_json["tags"][0]
                        thread_logger("Quay API query for %s generated %s" % (image, images["quay"][image]), remote_name = remote_name, log_prefix = log_prefix)
                    else:
                        thread_logger("Quay API query for %s found %d tags" % (image, len(image_json["tags"])), log_level = "warning", remote_name = remote_name, log_prefix = log_prefix)

        thread_logger("images[quay]:\n%s" % (endpoints.dump_json(images["quay"])), remote_name = remote_name, log_prefix = log_prefix)
    else:
        thread_logger("Configuration information necessary to utilize the quay API to obtain image expiration timestamps is not available", remote_name = remote_name, log_prefix = log_prefix)

    image_expiration = endpoints.image_expiration_gmepoch()
    thread_logger("Images evaludated by their expiration data will be considered expired if it is before %d" % (image_expiration), remote_name = remote_name, log_prefix = log_prefix)

    deletes = []
    for image in images["rickshaw"].keys():
        if not image in images["podman"]:
            thread_logger("Rickshaw image '%s' is no longer present in podman images, removing from consideration" % (image), remote_name = remote_name, log_prefix = log_prefix)
            deletes.append(image)
            continue

        images["rickshaw"][image]["timestamps"].sort(reverse = True)

        images["rickshaw"][image]["uses"] = len(images["rickshaw"][image]["timestamps"])

        images["rickshaw"][image]["latest-usage"] = images["rickshaw"][image]["timestamps"][0]

        del images["rickshaw"][image]["timestamps"]
    for image in deletes:
        del images["rickshaw"][image]
    thread_logger("images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)

    deletes = dict()
    deletes["rickshaw"] = []
    deletes["podman"] = []
    for image in images["podman"].keys():
        image_repo = os.environ.get("RS_REG_REPO")
        if image_repo is None:
            image_prefix = r"client-server"
            thread_logger("Using default podman image prefix '%s'" % (image_prefix), log_level = "warning", remote_name = remote_name, log_prefix = log_prefix)
        else:
            image_prefix = image_repo.split("/")
            image_prefix = image_prefix[len(image_prefix) - 1]
            thread_logger("Using podman image prefix '%s'" % (image_prefix), remote_name = remote_name, log_prefix = log_prefix)

        m = re.search(image_prefix, image)
        if m is None:
            thread_logger("Podman image '%s' is not a '%s' image, ignoring" % (image, image_prefix), remote_name = remote_name, log_prefix = log_prefix)
            deletes["podman"].append(image)
            continue
        else:
            thread_logger("Podman image '%s' is a '%s' image, processing" % (image, image_prefix), remote_name = remote_name, log_prefix = log_prefix)

        if not image in images["rickshaw"]:
            thread_logger("Podman image '%s' is not present in rickshaw container image census, removing it from the image cache" % (image), remote_name = remote_name, log_prefix = log_prefix)
            deletes["podman"].append(image)
            remove_image(thread_name, remote_name, log_prefix, connection, image)

        if image in images["quay"]:
            if images["quay"][image]["end_ts"] < image_expiration:
                thread_logger("Podman image '%s' has been evaluated based on its expiration data and has expired, removing it from the image cache" % (image), remote_name = remote_name, log_prefix = log_prefix)
                deletes["podman"].append(image)
                deletes["rickshaw"].append(image)
                remove_image(thread_name, remote_name, log_prefix, connection, image)
            else:
                thread_logger("Podman image '%s' has been evaluated based on its expiration data and has not expired" % (image), remote_name = remote_name, log_prefix = log_prefix)
        else:
            image_parts = image.split(":")
            if image_parts[0] in expiration_create_registries and images["podman"][image]["created"] < expiration_create_registries[image_parts[0]]:
                thread_logger("Podman image '%s' has been evaluated based on its creation data and has expired, removing it from the image cache" % (image), remote_name = remote_name, log_prefix = log_prefix)
                deletes["podman"].append(image)
                deletes["rickshaw"].append(image)
                remove_image(thread_name, remote_name, log_prefix, connection, image)
            else:
                thread_logger("Podman image '%s' has been evaluated based on its creation data and has not expired" % (image), remote_name = remote_name, log_prefix = log_prefix)

        if not image in deletes["podman"]:
            thread_logger("Podman image '%s' is valid and remains under consideration" % (image), remote_name = remote_name, log_prefix = log_prefix)
    for kind in deletes.keys():
        for image in deletes[kind]:
            del images[kind][image]
    thread_logger("images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)
    thread_logger("images[podman]:\n%s" % (endpoints.dump_json(images["podman"])), remote_name = remote_name, log_prefix = log_prefix)

    if len(images["rickshaw"]) == 0 or len(images["podman"]) == 0:
        thread_logger("Invalid state, exiting image manager", remote_name = remote_name, log_prefix = log_prefix)
        return

    cache_size = 0

    if cache_size < image_max_cache_size:
        thread_logger("Attemping to cache the most recently used image(s)", remote_name = remote_name, log_prefix = log_prefix)
        sorted_images = sorted(images["rickshaw"].items(), key = lambda x: (x[1]["latest-usage"], x[0]), reverse = True)
        thread_logger("latest sorted images[rickshaw]:\n%s" % (endpoints.dump_json(sorted_images)), remote_name = remote_name, log_prefix = log_prefix)

        image = sorted_images[0][0]
        image_last_used = sorted_images[0][1]["latest-usage"]
        image_run_id = sorted_images[0][1]["run-ids"][image_last_used]
        images["rickshaw"][image]["cached"] = True
        cache_size += 1
        thread_logger("Rickshaw image '%s' is being preserved in the image cache at slot %d of %d due to last used" % (image, cache_size, image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

        if cache_size < image_max_cache_size:
            for image in images["rickshaw"].keys():
                if cache_size >= image_max_cache_size:
                    thread_logger("Image cache is full", remote_name = remote_name, log_prefix = log_prefix)
                    break

                if images["rickshaw"][image]["cached"]:
                    continue

                for timestamp in images["rickshaw"][image]["run-ids"].keys():
                    if images["rickshaw"][image]["run-ids"][timestamp] == image_run_id:
                        images["rickshaw"][image]["cached"] = True
                        cache_size += 1
                        thread_logger("Rickshaw image '%s' is being preserved in the image cache at slot %d of %d due to association with last used" % (image, cache_size, image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

                if cache_size > image_max_cache_size:
                    thread_logger("Image cache maximum size '%d' exceeded in order to cache complete run image sets" % (image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)
        else:
            thread_logger("Image cache is full", remote_name = remote_name, log_prefix = log_prefix)

        thread_logger("images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)
    else:
        thread_logger("No images can be cached due to maximum image cache size of %d" % (image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

    if cache_size < image_max_cache_size:
        thread_logger("Attempting to cache the most used image(s)", remote_name = remote_name, log_prefix = log_prefix)
        sorted_images = sorted(images["rickshaw"].items(), key = lambda x: (x[1]["uses"], x[0]), reverse = True)
        thread_logger("usage sorted images[rickshaw]:\n%s" % (endpoints.dump_json(sorted_images)), remote_name = remote_name, log_prefix = log_prefix)

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
                    thread_logger("Rickshaw image '%s' is being preserved in the image cache at slot %d of %d due to usage" % (image[0], cache_size, image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

                    if cache_size < image_max_cache_size:
                        for image2 in images["rickshaw"].keys():
                            if cache_size >= image_max_cache_size:
                                thread_logger("Image cache is full", remote_name = remote_name, log_prefix = log_prefix)
                                loop = False
                                break

                            if images["rickshaw"][image2]["cached"]:
                                continue

                            for timestamp in images["rickshaw"][image2]["run-ids"].keys():
                                if images["rickshaw"][image2]["run-ids"][timestamp] == image_run_id:
                                    images["rickshaw"][image2]["cached"] = True
                                    cache_size += 1
                                    thread_logger("Rickshaw image '%s' is being preserved in the image cache at slot %d of %d due to association with usage" % (image2, cache_size, image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

                            if cache_size > image_max_cache_size:
                                thread_logger("Image cache maximum size '%d' exceeded in order to cache complete run image sets" % (image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)
                                loop = False
                                break
                    else:
                        thread_logger("Image cache is full", remote_name = remote_name, log_prefix = log_prefix)
                        loop = False

            if all_processed:
                thread_logger("All rickshaw images are cached", remote_name = remote_name, log_prefix = log_prefix)
                break

        thread_logger("images[rickshaw]:\n%s" % (endpoints.dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)
    else:
        if cache_size == 0:
            thread_logger("No images can be cached due to maximum image cache size of %d" % (image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)
        else:
            thread_logger("Image cache is full", remote_name = remote_name, log_prefix = log_prefix)

    thread_logger("Removing all rickshaw images not marked to preserve in the image cache", remote_name = remote_name, log_prefix = log_prefix)
    all_cached = True
    for image in images["rickshaw"].keys():
        if not images["rickshaw"][image]["cached"]:
            all_cached = False
            remove_image(thread_name, remote_name, log_prefix, connection, image)
    if all_cached:
        thread_logger("All rickshaw images are cached", remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman image prune -f")
    thread_logger("Pruning dangling images resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote_name, log_prefix = log_prefix)

    result = endpoints.run_remote(connection, "podman images --all")
    thread_logger("All podman images on this remote host after running image manager:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

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
    thread_logger("Starting shutdown engines thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote_idx = None
        try:
            remote_idx = work_queue.get(block = False)
        except queue.Empty:
            thread_logger("Received a work queue empty exception")
            break

        if remote_idx is None:
            thread_logger("Received a null job", log_level = "warning")
            continue

        job_count += 1

        remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][remote_idx]
        remote_name = "%s-%s" % (remote["config"]["host"], remote_idx)

        thread_logger("Processing remote '%s' at index %d" % (remote["config"]["host"], remote_idx), remote_name = remote_name)
        thread_logger("Remote user is %s" % (remote["config"]["settings"]["remote-user"]), remote_name = remote_name)

        with endpoints.remote_connection(remote["config"]["host"], remote["config"]["settings"]["remote-user"]) as con:
            result = endpoints.run_remote(con, "mount")
            thread_logger("All mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = endpoints.run_remote(con, "podman ps --all")
            thread_logger("All podman pods on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = endpoints.run_remote(con, "podman mount")
            thread_logger("All podman container mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            for engine in remote["engines"]:
                for engine_id in engine["ids"]:
                    engine_name = "%s-%s" % (engine["role"], str(engine_id))
                    container_name = "%s_%s" % (settings["misc"]["run-id"], engine_name)
                    thread_logger("Processing engine '%s'" % (engine_name), remote_name = remote_name, engine_name = engine_name)
                    thread_logger("Container name is '%s'" % (container_name), remote_name = remote_name, engine_name = engine_name)

                    osruntime = None
                    if engine["role"] == "profiler":
                        osruntime = "podman"
                    else:
                        osruntime = remote["config"]["settings"]["osruntime"]
                    thread_logger("osruntime is '%s'" % (osruntime), remote_name = remote_name, engine_name = engine_name)

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
            thread_logger("All mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = endpoints.run_remote(con, "podman ps --all")
            thread_logger("All podman pods on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = endpoints.run_remote(con, "podman mount")
            thread_logger("All podman container mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

        thread_logger("Notifying work queue that job processing is complete", remote_name = remote_name)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger("Stopping shutdown engines thread after processing %d job(s)" % (job_count))
    return

def shutdown_engines():
    """
    Handle the shutdown of engines after the test is complete

    Args:
        logger: a logger instance

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    logger.info("Creating threadpool to handle engine shutdown")

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
    thread_logger("Starting image management thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote = None
        try:
            remote = work_queue.get(block = False)
        except queue.Empty:
            thread_logger("Received a work queue empty exception")
            break

        if remote is None:
            thread_logger("Received a null job", log_level = "warning")
            continue

        job_count += 1

        my_unique_remote = settings["engines"]["remotes"][remote]
        my_run_file_remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][my_unique_remote["run-file-idx"][0]]

        thread_logger("Remote user is %s" % (my_run_file_remote["config"]["settings"]["remote-user"]), remote_name = remote)

        with endpoints.remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as con:
            remote_image_manager(thread_name, remote, con, my_run_file_remote["config"]["settings"]["image-cache-size"])

        thread_logger("Notifying work queue that job processing is complete", remote_name = remote)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger("Stopping image management thread after processing %d job(s)" % (job_count))
    return

def image_mgmt():
    """
    Handle the image management on the remotes

    Args:
        None

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    logger.info("Creating threadpool to handle image management")

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
    thread_logger("Starting collect sysinfo thread with thread ID %d and name = '%s'" % (thread_id, thread_name))
    rc = 0
    job_count = 0

    while not work_queue.empty():
        remote = None
        try:
            remote = work_queue.get(block = False)
        except queue.Empty:
            thread_logger("Received a work queue empty exception")
            break

        if remote is None:
            thread_logger("Received a null job", log_level = "warning")
            continue

        job_count += 1

        my_unique_remote = settings["engines"]["remotes"][remote]
        my_run_file_remote = settings["run-file"]["endpoints"][args.endpoint_index]["remotes"][my_unique_remote["run-file-idx"][0]]

        thread_logger("Remote user is %s" % (my_run_file_remote["config"]["settings"]["remote-user"]), remote_name = remote)

        with endpoints.remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as con:
            local_dir = settings["dirs"]["local"]["sysinfo"] + "/" + remote
            endpoints.my_make_dirs(local_dir)

            local_packrat_file = args.packrat_dir + "/packrat"
            remote_packrat_file = settings["dirs"]["remote"]["run"] + "/packrat"
            result = con.put(local_packrat_file, remote_packrat_file)
            thread_logger("Copied %s to %s:%s" % (local_packrat_file, remote, remote_packrat_file), remote_name = remote)

            result = endpoints.run_remote(con, remote_packrat_file + " " + settings["dirs"]["remote"]["sysinfo"])
            thread_logger("Running packrat resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)

            result = endpoints.run_remote(con, "tar --create --directory " + settings["dirs"]["remote"]["sysinfo"]  + " packrat-archive | xz --stdout | base64")
            thread_logger("Transferring packrat files resulted in return code %d:\nstderr:\n%s" % (result.exited, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)

            archive_file = local_dir + "/packrat.tar.xz"
            with open(archive_file, "wb") as archive_file_fp:
                archive_file_fp.write(base64.b64decode(result.stdout))
            thread_logger("Wrote packrat archive to '%s'" % (archive_file), remote_name = remote)

            result = endpoints.run_local("xz --decompress --stdout " + archive_file + " | tar --extract --verbose --directory " + local_dir)
            thread_logger("Unpacking packrat archive resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)

            path = Path(archive_file)
            path.unlink()
            thread_logger("Removed packrat archive '%s'" % (archive_file), remote_name = remote)

            result = endpoints.run_remote(con, "rm --recursive --force " + remote_packrat_file + " " + settings["dirs"]["remote"]["sysinfo"] + "/packrat-archive")
            thread_logger("Removing remote packrat files resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = endpoints.get_result_log_level(result), remote_name = remote)

        thread_logger("Notifying work queue that job processing is complete", remote_name = remote)
        work_queue.task_done()

    threads_rcs[thread_id] = rc
    thread_logger("Stopping collect sysinfo thread after processing %d job(s)" % (job_count))
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
    logger.info("Creating threadpool to collect sysinfo")

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
        logger: a logger instance

    Returns:
        None
    """
    thread_name = "UNKNOWN"
    if not args.thread is None:
        thread_name = args.thread.name

    msg = "[Thread %s] Thread failed with exception:\ntype: %s\nvalue: %s\ntraceback:\n%s" % (thread_name, args.exc_type, args.exc_value, "".join(traceback.format_list(traceback.extract_tb(args.exc_traceback))))
    logger.error(msg, stacklevel = 3)

    return

def main():
    """
    Main control block

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        rc (int): The return code for the script
    """
    global args
    global logger
    global settings
    early_abort = False

    threading.excepthook = thread_exception_hook

    if args.validate:
        return(validate())

    logger = endpoints.setup_logger(args.log_level)

    endpoints.log_env()
    endpoints.log_cli(args)
    settings = endpoints.init_settings(settings, args)

    settings = endpoints.load_settings(settings,
                                       endpoint_name = "remotehosts",
                                       run_file = args.run_file,
                                       rickshaw_dir = args.rickshaw_dir,
                                       endpoint_index = args.endpoint_index,
                                       endpoint_normalizer_callback = normalize_endpoint_settings,
                                       crucible_dir = args.crucible_dir)
    if settings is None:
        return 1

    if check_base_requirements() != 0:
        return 1
    if build_unique_remote_configs() != 0:
        return 1
    endpoints.create_local_dirs(settings)

    if endpoints.process_pre_deploy_roadblock(roadblock_id = args.roadblock_id,
                                              endpoint_label = args.endpoint_label,
                                              roadblock_password = args.roadblock_passwd,
                                              roadblock_messages_dir = settings["dirs"]["local"]["roadblock-msgs"],
                                              roadblock_timeouts = settings["rickshaw"]["roadblock"]["timeouts"],
                                              early_abort = early_abort):
        return 1

    create_remote_dirs()
    remote_image_pull_rc = remotes_pull_images()
    if remote_image_pull_rc == 0:
        set_total_cpu_partitions()
        launch_engines()
    else:
        logger.error("Skipping engine launch and signaling for job exit/abort due to fatal error during image pull");
        early_abort = True

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
                                      roadblock_password = args.roadblock_passwd,
                                      new_followers = settings["engines"]["new-followers"],
                                      roadblock_messages_dir = settings["dirs"]["local"]["roadblock-msgs"],
                                      roadblock_timeouts = settings["rickshaw"]["roadblock"]["timeouts"],
                                      max_sample_failures = args.max_sample_failures,
                                      engine_commands_dir = settings["dirs"]["local"]["engine-cmds"],
                                      endpoint_dir = settings["dirs"]["local"]["endpoint"],
                                      early_abort = early_abort)

    logger.info("Logging 'final' settings data structure")
    endpoints.log_settings(settings, mode = "settings")
    logger.info("remotehosts endpoint exiting")
    return rc

if __name__ == "__main__":
    args = endpoints.process_options()
    logger = None
    settings = dict()
    exit(main())
