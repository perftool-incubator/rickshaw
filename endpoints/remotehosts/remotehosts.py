#!/usr/bin/python3

"""
Endpoint to run 1 or more engines on 1 or more remotehost systems
"""

import argparse
import base64
import calendar
import copy
from fabric import Connection
from invoke import run
import jsonschema
import logging
import os
from paramiko import ssh_exception
from pathlib import Path
import queue
import re
import sys
import tempfile
import threading
import time

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
    "maximum-worker-threads-count": None
}

roadblock_exits = {
    "success": 0,
    "timeout": 3,
    "abort": 4,
    "input": 2
}

def get_result_log_level(result):
    """
    Determine the log level to return based on the provided result's return code.

    Args:
        result (Fabric/Invoke result): From a Fabric/Invoke run command

    Globals:
        None

    Returns:
        str: The appropriate log level to use to describe the result
    """
    if result.exited != 0:
        return "error"
    else:
        return "info"

def remote_connection(host, user):
    """
    Create a Fabric connection and open it

    Args:
       host (str): The IP address or hostname to connect to using Fabric
       user (str): The username to connect as

    Globals:
        None

    Returns:
       an open Fabric Connection
    """
    connection = Connection(host = host, user = user)
    attempts = 5
    attempt = 0
    while attempt < attempts:
        try:
            attempt += 1
            connection.open()
            if attempt > 1:
                msg = "Connected to remote '%s' as user '%s' after %d attempts" % (host, user, attempt)
                if args.validate:
                    validate_comment(msg)
                else:
                    log.info(msg)
            break
        except (ssh_exception.AuthenticationException, ssh_exception.NoValidConnectionsError) as e:
            msg = "Failed to connect to remote '%s' as user '%s' on attempt %d due to '%s'" % (host, user, attempt, str(e))
            if args.validate:
                validate_comment(msg)
            else:
                log.warning(msg)

            if attempt == attempts:
                msg = "Failed to connect to remote '%s' as user '%s' and maximum number of attempts (%d) has been exceeded.  Reraising exception '%s'" % (host, user, attempts, str(e))
                if args.validate:
                    validate_error(msg)
                else:
                    log.error(msg)

                    raise e
            else:
                time.sleep(1)
    return connection

def run_remote(connection, command):
    """
    Run a command on a remote server using an existing Fabric connection

    Args:
        connection (Fabric Connection): The connection to use to run the command remotely
        command (str): The command to run

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance

    Returns:
        a Fabric run result
    """
    debug_msg = "on remote '%s' as '%s' running command '%s'" % (connection.host, connection.user, command)
    if args.validate:
        if args.log_level == "debug":
            validate_debug(debug_msg)
    else:
        log.debug(debug_msg, stacklevel = 2)

    return connection.run(command, hide = True, warn = True)

def run_local(command):
    """
    Run a command on the local machine using Invoke

    Args:
        command (str): The command to run on the local system

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance

    Returns:
        an Invoke run result
    """
    debug_msg = "running local command '%s'" % (command)
    if args.validate:
        if args.log_level == "debug":
            validate_debug(debug_msg)
    else:
        log.debug(debug_msg, stacklevel = 2)

    return run(command, hide = True, warn = True)

def process_options():
    """
    Handle the CLI argument parsing options

    Args:
        None

    Globals:
        None

    Returns:
        args (namespace): The CLI parameters
    """
    parser = argparse.ArgumentParser(description = "Endpoint to run 1 or more engines on 1 or more remotehost systems",
                                     formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--base-run-dir",
                        dest = "base_run_dir",
                        help = "The base directory where the run (and all it's components, data, results, etc.) are stored.",
                        required = True,
                        type = str)

    parser.add_argument("--endpoint-deploy-timeout",
                        dest = "endpoint_deploy_timeout",
                        help = "How long should the timeout be for the endpoint deployment phase.",
                        required = False,
                        type = int,
                        default = 300)

    parser.add_argument("--endpoint-index",
                        dest = "endpoint_index",
                        help = "What is the index into the run-file's endpoints array that is assigned to this instance of the endpoint.",
                        required = True,
                        type = int)

    parser.add_argument("--endpoint-label",
                        dest = "endpoint_label",
                        help = "The name assigned to the endpoint, likely in the form <endpoint-type>-<id>.",
                        required = True,
                        type = str)

    parser.add_argument("--engine-script-start-timeout",
                        dest = "engine_script_start_timeout",
                        help = "How long should the timeout be for the engine start phase.",
                        required = False,
                        type = int,
                        default = 300)

    parser.add_argument("--image",
                        dest = "images",
                        help = "Comma separated list of images to use for particular tools/workloads.",
                        required = False,
                        type = str)

    parser.add_argument("--log-level",
                        dest = "log_level",
                        help = "Allow the user to control the degree of verbosity of the output.",
                        required = False,
                        type = str,
                        choices = [ "debug", "normal" ],
                        default = "normal")

    parser.add_argument("--max-rb-attempts",
                        dest = "max_rb_attempts",
                        help = "The maximum number of times a roadblock should be attempted if it fails.",
                        required = False,
                        type = int,
                        default = 1)

    parser.add_argument("--max-sample-failures",
                        dest = "max_sample_failures",
                        help = "The maximum number of times an iteration's samples can fail before the iteration fails.",
                        required = False,
                        type = int,
                        default = 1)

    parser.add_argument("--packrat-dir",
                        dest = "packrat_dir",
                        help = "Path to the packrat directory so that the endpoint can use it.",
                        required = False,
                        type = str)

    parser.add_argument("--rickshaw-dir",
                        dest = "rickshaw_dir",
                        help = "Path to the root of the rickshaw project directory.",
                        required = True,
                        type = str)

    parser.add_argument("--roadblock-id",
                        dest = "roadblock_id",
                        help = "The roadblock ID to use to build roadblock names.",
                        required = False,
                        type = str)

    parser.add_argument("--roadblock-passwd",
                        dest = "roadblock_passwd",
                        help = "The password to pass to roadblock to make redis connections.",
                        required = False,
                        type = str)

    parser.add_argument("--run-id",
                        dest = "run_id",
                        help = "The run identifier (generally a UUID) that is assigned to the run.",
                        required = False,
                        type = str)

    parser.add_argument("--run-file",
                        dest = "run_file",
                        help = "The user supplied run-file that specifies all settings for the run.",
                        required = True,
                        type = str)

    parser.add_argument("--validate",
                        dest = "validate",
                        help = "Signal that endpoint validation should be performed instead of actually running the endpoint.",
                        required = False,
                        action = "store_true")

    args = parser.parse_args()
    
    return args

def validate_log(msg):
    """
    Log a validation message

    Args:
        msg (str): The message to log

    Globals:
        None

    Returns:
        None
    """
    return print(msg)

def validate_comment(msg):
    """
    Log a validation comment message

    Args:
        msg (str): The message to log

    Globals:
        None

    Returns:
        None
    """
    return print("#" + msg)

def validate_error(msg):
    """
    Log a validation error message

    Args:
        msg (str): The message to log

    Globals:
        None

    Returns:
        None
    """
    return print("ERROR: " + msg)

def validate_debug(msg):
    """
    Log a vlidation debug message

    Args:
        msg (str): The message to log

    Globals:
        None

    Returns:
        None
    """
    return print("# DEBUG: " + msg)

def cli_stream():
    """
    Recreate the argument list that the script was called with

    Args:
        None

    Globals:
        sys.argv (list): The user provided CLI arguments

    Returns:
        stream (str): A formatted string of the user provided CLI arguments
    """
    stream = ""
    for i in range(1, len(sys.argv)):
        stream += " %s" % sys.argv[i]
    return stream

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
    validate_comment("params: %s" % (cli_stream()))

    validate_comment("argparse: %s" % (args))

    validate_comment("run-file: %s" % (args.run_file))
    validate_comment("endpoint-index: %d" % (args.endpoint_index))

    json, err = load_json_file(args.run_file)
    if json is None:
        validate_error(err)
        return 1
    validate_comment("run-file: %s" % (json))

    valid, err = validate_schema(json, args.rickshaw_dir + "/util/JSON/schema.json")
    if not valid:
        validate_error(err)
        return 1

    if args.endpoint_index >= len(json["endpoints"]):
        validate_error("endpoint_index %d does not exist in endpoints array from run-file" % (args.endpoint_index))
        return 1

    endpoint_settings = json["endpoints"][args.endpoint_index]
    validate_comment("endpoint-settings: %s" % (endpoint_settings))

    valid, err = validate_schema(endpoint_settings, args.rickshaw_dir + "/schema/remotehosts.json")
    if not valid:
        validate_error(err)
        return 1

    for endpoint_idx,endpoint in enumerate(json["endpoints"]):
        if endpoint_idx == args.endpoint_index:
            continue

        if json["endpoints"][endpoint_idx]["type"] == "remotehosts":
            validate_error("You can only specify one instance of the remotehosts endpoint.  If there is a reason you need multiple remotehosts endpoints then it should be seen as a bug in the remotehosts endpoint.")

    rickshaw_settings, err = load_json_file(args.base_run_dir + "/config/rickshaw-settings.json.xz", uselzma = True)
    if rickshaw_settings is None:
        validate_error(err)
        return 1
    validate_comment("rickshaw-settings: %s" % rickshaw_settings)

    endpoint_settings = normalize_endpoint_settings(endpoint_settings, rickshaw_settings)
    if endpoint_settings is None:
        return 1
    validate_comment("normalized endpoint-settings: %s" % (endpoint_settings))

    benchmark_engine_mapping = build_benchmark_engine_mapping(json["benchmarks"])
    validate_comment("benchmark-engine-mapping: %s" % (benchmark_engine_mapping))

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

    validate_comment("engines: %s" % (engines))
    for role in engines.keys():
        validate_log("%s %s" % (role, " ".join(map(str, engines[role]))))
        if len(engines[role]) != len(set(engines[role])):
            validate_error("There are duplicate IDs present for %s" % (role))
        for engine_id in engines[role]:
            found_engine = False
            for benchmark in benchmark_engine_mapping.keys():
                if engine_id in benchmark_engine_mapping[benchmark]["ids"]:
                    found_engine = True
                    break
            if not found_engine:
                validate_error("Could not find a benchmark mapping for engine ID %d" % (engine_id))

    validate_comment("userenvs: %s" % (userenvs))
    for userenv in userenvs:
        validate_log("userenv %s" % (userenv))

    remotes = dict()
    for remote in endpoint_settings["remotes"]:
        if not remote["config"]["host"] in remotes:
            remotes[remote["config"]["host"]] = dict()
        if not remote["config"]["settings"]["remote-user"] in remotes[remote["config"]["host"]]:
            remotes[remote["config"]["host"]][remote["config"]["settings"]["remote-user"]] = True
    validate_comment("remotes: %s" % (remotes))

    for remote in remotes.keys():
        for remote_user in remotes[remote].keys():
            try:
                with remote_connection(remote, remote_user) as c:
                    result = run_remote(c, "uptime")
                    validate_comment("remote login verification for %s with user %s: rc=%d and stdout=[%s] annd stderr=[%s]" % (remote, remote_user, result.exited, result.stdout.rstrip('\n'), result.stderr.rstrip('\n')))

                    result = run_remote(c, "podman --version")
                    validate_comment("remote podman presence check for %s: rc=%d and stdout=[%s] and stderr=[%s]" % (remote, result.exited, result.stdout.rstrip('\n'), result.stderr.rstrip('\n')))
                    if result.exited != 0:
                        result = run_remote(c, "yum install -y podman")
                        validate_comment("remote podman installation for %s: rc=%d" % (remote, result.exited))
                        if result.exited != 0:
                            validate_error("Could not install podman to remote %s" % (remote))
                            validate_error("stdout:\n%s" % (result.stdout))
                            validate_error("stderr:\n%s" % (result.stderr))
            except ssh_exception.AuthenticationException as e:
                validate_error("remote login verification for %s with user %s resulted in an authentication exception '%s'" % (remote, remote_user, str(e)))
            except ssh_exception.NoValidConnectionsError as e:
                validate_error("remote login verification for %s with user %s resulted in an connection exception '%s'" % (remote, remote_user, str(e)))

    return 0

def log_cli():
    """
    Log the script invocation details in a readable form

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance

    Returns:
        0
    """
    log.info("Logging CLI")

    log.info("CLI parameters:\n%s" % (cli_stream()))

    the_args = dict()
    for arg in args.__dict__:
        the_args[str(arg)] = args.__dict__[arg]
    log.info("argparse:\n %s" % (dump_json(the_args)))

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
            return log.info("settings[benchmark-mapping]:\n%s" % (dump_json(settings["engines"]["benchmark-mapping"])), stacklevel = 2)
        case "engines":
            return log.info("settings[engines]:\n%s" % (dump_json(settings["engines"])), stacklevel = 2)
        case "misc":
            return log.info("settings[misc]:\n%s" % (dump_json(settings["misc"])), stacklevel = 2)
        case "dirs":
            return log.info("settings[dirs]:\n%s" % (dump_json(settings["dirs"])), stacklevel = 2)
        case "endpoint":
            return log.info("settings[endpoint]:\n%s" % (dump_json(settings["run-file"]["endpoints"][args.endpoint_index])), stacklevel = 2)
        case "rickshaw":
            return log.info("settings[rickshaw]:\n%s" % (dump_json(settings["rickshaw"])), stacklevel = 2)
        case "run-file":
            return log.info("settings[run-file]:\n%s" % (dump_json(settings["run-file"])), stacklevel = 2)
        case "all" | _:
            return log.info("settings:\n%s" % (dump_json(settings)), stacklevel = 2)

def not_json_serializable(obj):
    """
    Convert non-serializable variables into something that can be handled by the JSON conversion process'

    Args:
        obj: a non-serializable variable

    Globals:
        None

    Returns:
        str: a representation of the object that can be serialized into a json property value
    """
    try:
        return(obj.to_dictionary())
    except AttributeError:
        return(repr(obj))

def dump_json(obj):
    """
    Convert a variable into a formatted JSON string

    Args:
        obj: A variable of potentially many types to convert into a JSON string

    Globals:
        None

    Returns:
        str: A formatted string containing the JSON representation of obj
    """
    return json.dumps(obj, indent = 4, separators=(',', ': '), sort_keys = True, default = not_json_serializable)

def my_make_dirs(mydir):
    """
    Created the requested directory (recursively if necessary)

    Args:
        mydir (str): The directory that needs to be created

    Globals:
        log: a logger instance

    Returns:
        None
    """
    log.info("Creating directory %s (recurisvely if necessary)" % (mydir), stacklevel = 2)
    return os.makedirs(mydir, exist_ok = True)

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
    my_make_dirs(settings["dirs"]["local"]["run"])
    my_make_dirs(settings["dirs"]["local"]["engine-logs"])
    my_make_dirs(settings["dirs"]["local"]["roadblock-msgs"])
    my_make_dirs(settings["dirs"]["local"]["sysinfo"])
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
    settings["engines"]["benchmark-mapping"] = build_benchmark_engine_mapping(settings["run-file"]["benchmarks"])
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

def expand_id_range(id_range):
    """
    Helper function to expand an ID range

    Args:
        id_range (str): An ID range in one of the following forms [ "A-E", "F" ]

    Globals:
        None

    Returns:
        expanded_ids (list): A list containing the broken out IDs in integer format such as [ A, B, C, D, E ] or [ F ]
    """
    expanded_ids = []

    split_id_range = id_range.split("-")
    if len(split_id_range) == 1:
        expanded_ids.append(int(split_id_range[0]))
    else:
        split_id_range = list(map(int, split_id_range))
        if split_id_range[0] >= 0 and split_id_range[0] < split_id_range[1]:
            for subid in range(split_id_range[0], split_id_range[1]+1):
                expanded_ids.append(subid)
        else:
            raise ValueError("Invalid id range: %s" % (id_range))

    return expanded_ids

def expand_ids(ids):
    """
    Take a IDs specification and expand it

    Args:
        ids (str, int, or list): A complex variable containing one of many forms of ID representation
                                 such as [ "A-E", "A-E+H-K+M", "A", A, [ A, "C-E", "H" ] ]

    Globals:
        None

    Returns:
        new_ids (list): A list containing the broken out IDs in integer format such as [ A, B, C, D, E] or
                        [ A, B, C, D, E, H, J, K, M ] or [ A ] or [ A ] or [ A, C, D, E, H ]
    """
    new_ids = []

    if isinstance(ids, str):
        subids = ids.split("+")
        for subid in subids:
            new_ids.extend(expand_id_range(subid))
    elif isinstance(ids, int):
        new_ids.append(ids)
    elif isinstance(ids, list):
        for id in ids:
            if isinstance(id, int):
                new_ids.append(id)
            elif isinstance(id, str):
                new_ids.extend(expand_id_range(id))

    new_ids.sort()

    return new_ids

def is_ip(ip_address):
    """
    Determine if the provided variable contains an IPv4 of IPv6 address

    Args:
        ip_address (str): A string to check if it is a valid IPv4 or IPv5 address (it could be a hostname)

    Globals:
        None

    Returns:
        True or False
    """
    # check for IPv4
    m = re.search(r"[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}", ip_address)
    if m:
        return True

    # check for IPv6
    m.re.search(r"[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]", ip_address)
    if m:
        return True

    return False

def get_controller_ip(host):
    """
    Determine the correct controller IP address for the provided host to contact the controller at

    Args:
        host (str): Either a hostname or an IP address for a remote

    Globals:
        None

    Returns:
        controller_ip (str): The controller's IP address that the specified remote can use to contact it
    """
    controller_ip = None

    if host == "localhost":
        cmd = "ip addr show lo"
        result = run_local(cmd)
        if result.exited != 0:
            raise ValueError("Failed to successfully run '%s'" % (cmd))
        for line in result.stdout.splitlines():
            # looking for something like the following line
            #    inet 127.0.0.1/8 scope host lo
            m = re.search(r"int\s", line)
            if m:
                split_line = line.split(" ")
                if len(split_line) > 1:
                    split_line = split_line[1]
                    if len(split_line) > 1:
                        controller_ip = split_line[0]
                        break
        if controller_ip is None or not is_ip(controller_ip):
            raise ValueError("Failed to map localhost to loopback IP address (got '%s')" % controller_ip)
        else:
            host = controller_ip
            controller_ip = None

    remote_ip = None
    if is_ip(host):
        cmd = "ip addr"
        result = run_local(cmd)
        if result.exited != 0:
            raise ValueError("Failed to successfully run '%s'" % (cmd))
        for line in result.stdout.splitlines():
            m = re.search(host, line)
            if m:
                # the provided ip address is mine so it is the one to use
                return host
        # the provided ip address is not mine -- so it is the remotes's
        remote_ip = host
    else:
        cmd = "host %s" % (host)
        result = run_local(cmd)
        if result.exited != 0:
            raise ValueError("Failed to successfully run '%s'" % (cmd))
        for line in result.stdout.splitlines():
            m = re.search(r"has address", line)
            if m:
                split_line = line.split("has address ")
                if len(split_line) > 1:
                    remote_ip = split_line[1]
                    break
        if remote_ip is None or not is_ip(remote_ip):
            raise ValueError("Failed to determine remote IP address (got '%s')" % (endpoint_ip))

    # now that we have the remote's ip address, figure out what
    # controller ip this remote will need to use to contact the
    # controller
    cmd = "ip route get %s" % (remote_ip)
    result = run_local(cmd)
    if result.exited != 0:
        raise ValueError("Failed to successfully run '%s'" % (cmd))
    for line in result.stdout.splitlines():
        m = re.search(r"src ", line)
        if m:
            split_line = line.split("src ")
            if len(split_line) > 1:
                split_line = split_line[1].split(" ")
                if len(split_line) > 1:
                    controller_ip = split_line[0]
                    break
    if controller_ip is None or not is_ip(controller_ip):
        raise ValueError("Failed to determine controller IP address (got '%s')" % (controller_ip))

    return controller_ip

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
                    remote["config"]["settings"]["controller-ip-address"] = get_controller_ip(remote["config"]["host"])
                    cached_controller_ips[remote["config"]["host"]] = remote["config"]["settings"]["controller-ip-address"]
                except ValueError as e:
                    msg = "While determining controller IP address for remote '%s' encountered exception '%s'" % (remote["config"]["host"], str(e))
                    if args.validate:
                        validate_error(msg)
                    else:
                        log.error(msg)
                    return None

        for engine in remote["engines"]:
            if engine["role"] == "profiler":
                continue
            try:
                engine["ids"] = expand_ids(engine["ids"])
            except ValueError as e:
                msg = "While expanding '%s' encountered exception '%s'" % (engine["ids"], str(e))
                if args.validate:
                    validate_error(msg)
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

def build_benchmark_engine_mapping(benchmarks):
    """
    Build the benchmark-to-engine mapping

    Args:
        benchmarks (list): The list of benchmarks from the run-file

    Globals:
        None

    Returns:
        mapping (dict): A collection of dictionaries which maps particular client/server IDs to a particular benchmark
    """
    mapping = dict()

    for benchmark_idx,benchmark in enumerate(benchmarks):
        benchmark_id = benchmark["name"] + "-" + str(benchmark_idx)
        mapping[benchmark_id] = {
            "name": benchmark["name"],
            "ids": expand_ids(benchmark["ids"])
        }

    return mapping

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

        with remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as c:
            for image in my_unique_remote["images"]:
                result = run_remote(c, "podman pull " + image)
                loglevel = "info"
                if result.exited != 0:
                    loglevel = "error"
                thread_logger(thread_name, "Attempted to pull %s with return code %d:\nstdout:\n%sstderr:\n%s" % (image, result.exited, result.stdout, result.stderr), log_level = loglevel, remote_name = remote)
                rc += result.exited

                result = run_remote(c, "echo '" + image + " " + str(int(time.time())) + " " + args.run_id + "' >> " + settings["dirs"]["remote"]["base"] + "/remotehosts-container-image-census")
                loglevel = "info"
                if result.exited != 0:
                    loglevel = "error"
                thread_logger(thread_name, "Recorded usage for %s in the census with return code %d:\nstdout:\n%sstderr:\n%s" % (image, result.exited, result.stdout, result.stderr), log_level = loglevel, remote_name = remote)
                rc += result.exited

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

    thread_logger("MAIN", "Return codes for each %s:\n%s" % (acronym, dump_json(worker_threads_rcs)))

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

        with remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as con:
            for remote_dir in settings["dirs"]["remote"].keys():
                result = run_remote(con, "mkdir --parents --verbose " + settings["dirs"]["remote"][remote_dir])
                thread_logger(thread_name, "Remote %s attempted to mkdir %s with return code %d:\nstdout:\n%sstderr:\n%s" % (remote, settings["dirs"]["remote"][remote_dir], result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result))
                rc += result.exited

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

def create_podman(thread_name, remote_name, engine_name, container_name, connection, remote, controller_ip, role, image, cpu_partitioning, numa_node, host_mounts):
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
                   "--ipc=host",
                   "--pid=host",
                   "--net=host",
                   "--security-opt=label=disable" ]

    for mount in mandatory_mounts + host_mounts:
        if not "dest" in mount:
            mount["dest"] = mount["src"]

        create_cmd.append("--mount=type=bind,source=" + mount["src"] + ",destination=" + mount["dest"])

    create_cmd.append(image)

    thread_logger(thread_name, "Podman create command is:\n%s" % (dump_json(create_cmd)), remote_name = remote_name, engine_name = engine_name)

    result = run_remote(connection, " ".join(create_cmd))
    thread_logger(thread_name, "Creating container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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

    thread_logger(thread_name, "Podman create command is:\n%s" % (dump_json(create_cmd)), remote_name = remote_name, engine_name = engine_name)

    result = run_remote(connection, " ".join(create_cmd))
    thread_logger(thread_name, "Creating container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)
    if result.exited == 0:
        create_info["id"] = result.stdout.rstrip('\n')
    else:
        return create_info

    mount_cmd = [ "podman",
                  "mount",
                  container_name ]

    result = run_remote(connection, " ".join(mount_cmd))
    thread_logger(thread_name, "Mounting container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)
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

        thread_logger(thread_name, "Procesing mount:\n%s" % (dump_json(mount)), remote_name = remote_name, engine_name = engine_name)

        result = run_remote(connection, "mkdir --parents --verbose " + mount["dest"])
        thread_logger(thread_name, "Creating '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["dest"], result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

        if mount["rbind"]:
            result = run_remote(connection, "mount --verbose --options rbind " + mount["src"] + " " + mount["dest"])
            thread_logger(thread_name, "rbind mounting '%s' to '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["src"], mount["dest"], result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

            result = run_remote(connection, "mount --verbose --make-rslave " + mount["dest"])
            thread_logger(thread_name, "making rslave '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["dest"], result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

            create_info["mounts"]["rbind"].append(mount["dest"])
        else:
            result = run_remote(connection, "mount --verbose --options bind " + mount["src"] + " " + mount["dest"])
            thread_logger(thread_name, "bind mounting '%s' to '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (mount["src"], mount["dest"], result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

            create_info["mounts"]["regular"].append(mount["dest"])

    local_ssh_private_key_file = settings["dirs"]["local"]["conf"] + "/rickshaw_id.rsa"
    remote_ssh_private_key_file = create_info["mount"] + "/tmp/" + "rickshaw_id.rsa"
    result = connection.put(local_ssh_private_key_file, remote_ssh_private_key_file)
    thread_logger(thread_name, "Copied %s to %s:%s" % (local_ssh_private_key_file, connection.host, remote_ssh_private_key_file), remote_name = remote_name, engine_name = engine_name)

    for etc_file in [ "hosts", "resolv.conf" ]:
        src_etc_file = "/etc/" + etc_file
        dst_etc_dir = create_info["mount"] + "/etc"
        result = run_remote(connection, "cp --verbose " + src_etc_file + " " + dst_etc_dir)
        thread_logger(thread_name, "Remotely copied %s to %s" % (src_etc_file, dst_etc_dir), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    thread_logger(thread_name, "chroot create info:\n%s" % (dump_json(create_info)), remote_name = remote_name, engine_name = engine_name)

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

    result = run_remote(connection, " ".join(start_cmd))
    thread_logger(thread_name, "Starting container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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

    thread_logger(thread_name, "chroot start command is:\n%s" % (dump_json(start_cmd)), remote_name = remote_name, engine_name = engine_name)

    result = run_remote(connection, " ".join(start_cmd))
    thread_logger(thread_name, "Starting chroot with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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

        with remote_connection(remote["config"]["host"], remote["config"]["settings"]["remote-user"]) as con:
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

                    result = run_remote(con, "podman ps --all --filter 'name=" + container_name + "' --format '{{.Names}}'")
                    thread_logger(thread_name, "Check for existing container with name '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (container_name, result.exited, result.stdout, result.stderr), remote_name = remote_name, engine_name = engine_name)
                    if result.exited != 0:
                        thread_logger(thread_name, "Check for existing container exited with non-zero return code %d" % (result.exited), log_level = "error", remote_name = remote_name, engine_name = engine_name)
                    if result.stdout.rstrip('\n') == container_name:
                        thread_logger(thread_name, "Found existing container '%s'" % (container_name), log_level = "warning", remote_name = remote_name, engine_name = engine_name)

                        result = run_remote(con, "podman rm --force " + container_name)
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

                            create_podman(thread_name, remote_name, engine_name, container_name, con, remote["config"]["host"], remote["config"]["settings"]["controller-ip-address"], engine["role"], image, cpu_partitioning, numa_node, remote["config"]["settings"]["host-mounts"])
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

def create_roadblock_msg(recipient_type, recipient_id, payload_type, payload):
    """
    Create a user built roadblock message

    Args:
        recipient_type (str): What type of roadblock participant ("leader" or "follower" or "all") is the message for
        recipient_id (str): What is the specific name/ID of the intended message recipient

    Globals:
        log: a logger instance

    Returns:
        msg (dict): The generated message
    """
    msg = [
        {
            "recipient": {
                "type": recipient_type,
                "id": recipient_id,
            },
            payload_type: payload
        }
    ]

    json_msg = dump_json(msg)
    log.info("Creating new roadblock message for recipient type '%s' with recipient id '%s':\n%s" % (recipient_type, recipient_id, json_msg), stacklevel = 2)

    return msg

def do_roadblock(label = None, timeout = None, messages = None, wait_for = None, abort = None):
    """
    Run a roadblock

    Args:
        label (str): The name of the roadblock to participate in
        timeout (int): An optional timeout value to use for the roadblock
        messages (str): An optional messages file to send
        wait_for (str): An optional command to wait on to complete the roadblock
        abort (bool): An optional parameter specifying that a abort should be sent

    Globals:
        log: a logger instance

    Returns:
        rc (int): The return code for the roadblock
    """
    if label is None:
        log.error("No roadblock label specified", stacklevel = 2)
        raise ValueError("No roadblock label specified")

    log.info("Processing roadblock '%s'" % (label), stacklevel = 2)
    uuid = "%s:%s" % (args.roadblock_id, label)
    log.info("[%s] Roadblock uuid is '%s'" % (label, uuid))

    if timeout is None:
        timeout = 300
        log.info("[%s] No roadblock timeout specified, defaulting to %d" % (label, timeout))
    else:
        log.info("[%s] Roadblock timeout set to %d" % (label, timeout))

    if messages is None:
        log.info("[%s] No roadblock messages to send" % (label))
    else:
        log.info("[%s] Sending roadblock messages %s" % (label, messages))

    if wait_for is None:
        log.info("[%s] No roadblock wait-for" % (label))
    else:
        wait_for_log = tempfile.mkstemp(suffix = "log")
        wait_for_log[0].close()
        wait_for_log = wait_for_log[1]
        log.info("[%s] Going to run this wait-for command: %s" % (label, wait_For))
        log.info("[%s] Going to log wait-for to this file: %s" % (label, wait_for_log))

    if not abort is None:
        log.info("[%s] Going to send an abort")

    msgs_log_file = settings["dirs"]["local"]["roadblock-msgs"] + "/" + label + ".json"
    log.info("[%s] Logging messages to: %s" % (label, msgs_log_file))

    redis_server = "localhost"
    leader = "controller"

    result = run_local("ping -w 10 -c 4 " + redis_server)
    ping_log_msg = "[%s] Pinged redis server '%s' with return code %d:\nstdout:\n%sstderr:\n%s" % (label, redis_server, result.exited, result.stdout, result.stderr)
    if result.exited != 0:
        log.error(ping_log_mesg)
    else:
        log.info(ping_log_msg)

    cmd = [
        "/usr/local/bin/roadblocker.py",
        "--role=follower",
        "--redis-server=" + redis_server,
        "--leader-id=" + leader,
        "--timeout=" + str(timeout),
        "--redis-password=" + args.roadblock_passwd,
        "--follower-id=" + args.endpoint_label,
        "--message-log=" + msgs_log_file
    ]

    if not messages is None:
        cmd.append("--user-message=" + messages)

    if not wait_for is None:
        cmd.append("--wait-for=" + wait_for)
        cmd.append("--wait-for-log=" + wait_for_log)

    if not abort is None:
        cmd.append("--abort")

    attempts = 0
    rc = -1
    while attempts < args.max_rb_attempts and rc != roadblock_exits["success"] and rc != roadblock_exits["abort"]:
        attempts += 1
        log.info("[%s] Attempt number: %d" % (label, attempts))

        rb_cmd = copy.deepcopy(cmd)
        rb_cmd.append("--uuid=%d:%s" % (attempts, uuid))
        log.info("[%s] Going to run this command:\n%s" % (label, dump_json(rb_cmd)))

        result = run_local(" ".join(rb_cmd))
        result_log_msg = "[%s] Roadblock attempted with return code %d:\nstdout:\n%sstderr:\n%s" % (label, result.exited, result.stdout, result.stderr)
        if result.exited != 0:
            log.error(result_log_msg)
        else:
            log.info(result_log_msg)
        rc = result.exited

        stream = ""
        with open(msgs_log_file, "r", encoding = "ascii") as msgs_log_file_fp:
            for line in msgs_log_file_fp:
                stream += line
        log.info("[%s] Logged messages from roadblock:\n%s" % (label, stream))

        if not wait_for is None:
            stream = ""
            with open(wait_for_log, "r", encoding = "ascii") as wait_for_log_fp:
                for line in wait_for_log_fp:
                    stream += line
            log.info("[%s] Wait-for log from raodblock:\n%s" % (label, stream))

    log.info("[%s] Total attempts: %d" % (label, attempts))
    log.info("[%s] Returning %d" % (label, rc))
    return rc

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

                env_vars_msgs.extend(create_roadblock_msg("follower", engine_name, "user-object", env_vars_payload))

    env_vars_msg_file = settings["dirs"]["local"]["roadblock-msgs"] + "/env-vars.json"
    log.info("Writing follower env-vars messages to %s" % (env_vars_msg_file))
    env_vars_msgs_json = dump_json(env_vars_msgs)
    with open(env_vars_msg_file, "w", encoding = "ascii") as env_vars_msg_file_fp:
        env_vars_msg_file_fp.write(env_vars_msgs_json)
    log.info("Contents of %s:\n%s" % (env_vars_msg_file, env_vars_msgs_json))

    return env_vars_msg_file

def prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, roadblock_name):
    """
    Prepare queued messages for distribution via roadblock

    Args:
        iteration_sample_dir (str): The directory where the iteration sample's files are stored
        engine_tx_msgs_dir (str): Where to write messages to send
        roadblock_name (str): The name of the roadblock that the messages should be sent for

    Globals:
        log: a logger instance

    Returns:
        user_msgs_file (str): The file containing the user messages if there are queued messages
        or
        None: If there are no queued messages
    """
    queued_msg_files = []
    with os.scandir(engine_tx_msgs_dir) as tx_msgs_dir:
        for entry in tx_msgs_dir:
            queued_msg_files.append(entry.name)

    if len(queued_msg_files) > 0:
        log.info("Found queued messages in %s, preparing them to send" % (engine_tx_msgs_dir))

        tx_sent_dir = engine_tx_msgs_dir + "-sent"
        my_make_dirs(tx_sent_dir)

        user_msgs = []
        for msg_file in queued_msg_files:
            msg_file_full_path = engine_tx_msgs_dir + "/" + msg_file
            log.info("Importing %s" % (msg_file_full_path))
            msg_file_json,err = load_json_file(msg_file_full_path)
            if msg_file_json is None:
                log.error("Failed to load user messages from %s with error '%s'" % (msg_file_full_path, err))
            else:
                log.info("Adding user messages from %s" % (msg_file_full_path))
                user_msgs.extend(msg_file_json)

                new_msg_file_full_path = tx_sent_dir + "/" + msg_file
                log.info("Moving user message file from %s to %s" % (msg_file_full_path, new_msg_file_full_path))
                os.replace(msg_file_full_path, new_msg_file_full_path)
        payload = {
            "sync": roadblock_name
        }
        user_msgs.extend(create_roadblock_msg("all", "all", "user-object", payload))

        user_msgs_file = "%s/rb-msgs-%s.json" % (iteration_sample_dir, roadblock_name)
        log.info("Writing user messages to %s" % (user_msgs_file))
        user_msgs_file_json = dump_json(user_msgs)
        with open(user_msgs_file, "w", encoding = "ascii") as user_msgs_file_fp:
            user_msgs_file_fp.write(user_msgs_file_json)
        log.info("Contents of %s:\n%s" % (user_msgs_file, user_msgs_file_json))

        return user_msgs_file
    else:
        log.info("No queued messages found in %s" % (engine_tx_msgs_dir))
        return None

def evaluate_roadblock(quit, abort, roadblock_name, roadblock_rc, iteration_sample, engine_rx_msgs_dir):
    """
    Evaluate the status of a completed roadblock and it's affect on the test

    Args:
        quit (bool): Should the entire test be quit
        abort (bool): Should the iteration be aborted
        roadblock_name (str): The name of the roadblock being evaluated
        iteration_sample (dict): The data structure representing the specific iteration sample being evaluated
        engine_rx_msgs_dir (str): Where to look for received messages

    Globals:
        log: a logger instance
        roadblock_exits (dict): A mapping of specific roadblock "events" to their associated return code

    Returns:
        abort (bool): The current abort value
        quit (bool): The current quit value
    """
    if roadblock_rc != 0:
        if roadblock_rc == roadblock_exits["timeout"]:
            log.error("Roadblock '%s' timed out, attempting to exit and cleanly finish the run" % (roadblock_name))
            quit = True
        elif roadblock_rc == roadblock_exits["abort"]:
            log.warning("Roadblock '%s' received an abort, stopping sample" % (roadblock_name))

            iteration_sample["attempt-fail"] = 1
            iteration_sample["failures"] += 1
            log.info("iteration sample failures is now %d" % (iteration_sample["failures"]))

            if iteration_sample["failures"] >= args.max_sample_failures:
                iteration_sample["complete"] = True
                log.error("A maximum of %d failures for iteration %d has been reached" % (iteration_sample["failures"], iteration_sample["iteration-id"]))

            abort = True

    msgs_log_file = engine_rx_msgs_dir + "/" + roadblock_name + ".json"
    path = Path(msgs_log_file)
    if path.exists() and path.is_file():
        log.info("Found received messages file: %s" % (msgs_log_file))

        split_roadblock_name = roadblock_name.split(":")
        roadblock_label = split_roadblock_name[1]

        msgs_json,err = load_json_file(msgs_log_file)
        if not msgs_json is None:
            if "received" in msgs_json:
                counter = 0
                for msg in msgs_json["received"]:
                    if msg["payload"]["message"]["command"] == "user-object":
                        counter += 1
                        msg = "%s:%d" % (roadblock_label, counter)
                        msg_outfile = engine_rx_msgs_dir + "/" + msg
                        msg_outfile_json = dump_json(msg["paylod"]["message"]["user-object"])
                        log.info("Found user-object message and saved it to %s:\nmessage:\n%s" % (msg_outfile, msg_outfile_json))
                        with open(msg_outfile, "w", encoding = "ascii") as msg_outfile_fp:
                            msg_outfile_fp.write(msg_outfile_json)
        else:
            log.error("Failed to load %s due to error '%s'" % (msgs_log_file, str(err)))

    return quit,abort

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
                                log.info("Processing received message:\n%s" % (dump_json(msg["payload"])))

                                # punching a hole through the fireall would go here

                                log.info("Creating a message to send to the client engine '%s' with IP and port info" % (client_engine))
                                msg = create_roadblock_msg("follower", client_engine, "user-object", msg["payload"]["message"]["user-object"])

                                msg_file = tx_msgs_dir + "/server-ip-" + server_engine + ".json"
                                log.info("Writing follower service-ip message to '%s'" % (msg_file))
                                with open(msg_file, "w", encoding = "ascii") as msg_file_fp:
                                    msg_file_fp.write(dump_json(msg))
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

def process_bench_roadblocks():
    """
    Handle the running and evaluation of roadblocks while looping through the iterations and samples

    Args:
        None

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Starting to process benchmark roadblocks")

    rc = do_roadblock(label = "setup-bench-begin",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])
    if rc != 0:
        return rc

    iteration_sample_data = []

    log.info("Initializing data structures")
    with open(settings["dirs"]["local"]["engine-cmds"] + "/client/1/start") as bench_cmds_fp:
        for line in bench_cmds_fp:
            split = line.split(" ")
            iteration_sample = split[0]
            split = iteration_sample.split("-")
            iteration_id = int(split[0])
            sample_id = int(split[1])

            log.info("iteration_sample=%s iteration_id=%s sample_id=%s" % (iteration_sample, iteration_id, sample_id))

            obj = {
                "iteration-sample": iteration_sample,
                "iteration-id": iteration_id,
                "sample-id": sample_id,
                "failures": 0,
                "complete": False,
                "attempt-num": 0,
                "attempt-fail": 0
            }
            iteration_sample_data.append(obj)

    log.info("Total tests: %d" % (len(iteration_sample_data)))

    rc = do_roadblock(label = "setup-bench-end",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])
    if rc != 0:
        return rc

    quit = False
    abort = False
    timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"]
    current_test = 0

    for iteration_sample_idx,iteration_sample in enumerate(iteration_sample_data):
        if quit:
            break

        current_test += 1

        iteration_sample_dir = "%s/iteration-%d/sample-%d" % (settings["dirs"]["local"]["endpoint"], iteration_sample["iteration-id"], iteration_sample["sample-id"])
        engine_msgs_dir = "%s/msgs" % (iteration_sample_dir)
        engine_tx_msgs_dir = "%s/tx" % (engine_msgs_dir)
        engine_rx_msgs_dir = "%s/rx" % (engine_msgs_dir)
        log.info("Creating iteration+sample directories")
        for current_dir in [ iteration_sample_dir, engine_msgs_dir, engine_tx_msgs_dir, engine_rx_msgs_dir ]:
            my_make_dirs(current_dir)

        abort = False
        while not quit and not abort and not iteration_sample["complete"] and iteration_sample["failures"] < args.max_sample_failures:
            iteration_sample["attempt-fail"] = 0
            iteration_sample["attempt-num"] += 1

            log.info("Starting iteration %d sample %d (test %d of %d) attempt number %d of %d" %
                     (
                         iteration_sample["iteration-id"],
                         iteration_sample["sample-id"],
                         current_test,
                         len(iteration_sample_data),
                         iteration_sample["attempt-num"],
                         args.max_sample_failures
                     ))

            rb_name = None
            test_id = "%d-%d-%d" % (iteration_sample["iteration-id"], iteration_sample["sample-id"], iteration_sample["attempt-num"])
            rb_prefix = "%s:" % (test_id)

            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "infra-start-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)

            rb_name = "%s%s" % (rb_prefix, "infra-start-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "server-start-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)

            rb_name = "%s%s" % (rb_prefix, "server-start-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "endpoint-start-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)

            test_start(settings["dirs"]["local"]["roadblock-msgs"], test_id, engine_tx_msgs_dir)

            rb_name = "%s%s" % (rb_prefix, "endpoint-start-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "client-start-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)

            msgs_log_file = settings["dirs"]["local"]["roadblock-msgs"] + "/" + rb_name + ".json"
            path = Path(msgs_log_file)
            if path.exists() and path.is_file():
                msgs_json,err = load_json_file(msgs_log_file)
                if not msgs_json is None:
                    new_timeout = None
                    if "received" in msgs_json:
                        for msg in msgs_json["received"]:
                            if msg["payload"]["message"]["command"] == "user-object" and "timeout" in msg["payload"]["message"]["user-object"]:
                                new_timeout = msg["payload"]["message"]["user-object"]["timeout"]
                                break
                    if new_timeout is None:
                        log.warning("Could not find new client-start-end timeout value")
                    else:
                        timeout = int(new_timeout)
                        log.info("Found new client-start-end timeout value: %d" % (timeout))
                else:
                    log.error("Failed to load %s due to error '%s'" % (msgs_log_file, str(err)))
            else:
                log.warning("Could not find %s" % (msgs_log_file))

            rb_name = "%s%s" % (rb_prefix, "client-start-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)
            ####################################################################
            if timeout != settings["rickshaw"]["roadblock"]["timeouts"]["default"]:
                timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"]
                log.info("Resetting timeout value: %s" % (timeout))

            rb_name = "%s%s" % (rb_prefix, "client-stop-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)

            rb_name = "%s%s" % (rb_prefix, "client-stop-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "endpoint-stop-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)

            rb_name = "%s%s" % (rb_prefix, "endpoint-stop-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "server-stop-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)

            test_stop()

            rb_name = "%s%s" % (rb_prefix, "server-stop-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "infra-stop-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)

            rb_name = "%s%s" % (rb_prefix, "infra-stop-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir)
            ####################################################################

            sample_result = None
            if iteration_sample["attempt-fail"] == 0 and not abort and not quit:
                iteration_sample["complete"] = True
                sample_result = "successfully"
            else:
                sample_result = "unsuccessfully"

                if abort:
                    log.warning("An abort signal has been encountered for this sample")

                if quit:
                    log.error("A quit signal has been encountered")

            log.info("Completed iteration %d sample %d (test %d of %d) attempt number %d of %d %s" %
                     (
                         iteration_sample["iteration-id"],
                         iteration_sample["sample-id"],
                         current_test,
                         len(iteration_sample_data),
                         iteration_sample["attempt-num"],
                         args.max_sample_failures,
                         sample_result
                     ))

    log.info("Final summary of iteration sample data:\n%s" % (dump_json(iteration_sample_data)))

    return 0

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
        result = run_remote(connection, "podman logs --timestamps " + container_name + " | xz -c | base64")
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
    result = run_remote(connection, "cat " + remote_log_file + " | xz -c | base64")
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

        result = run_remote(connection, "rm --verbose " + remote_log_file)
        thread_logger(thread_name, "Removal of engine log for '%s' gave return code %d:\nstdout:\n%stderr:\n%s" %
                      (
                          engine_name,
                          result.exited,
                          result.stdout,
                          result.stderr
                      ), log_level =  get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)
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
    result = run_remote(connection, "if [ -e \"" + remote_rickshaw_settings + "\" ]; then rm --verbose \"" + remote_rickshaw_settings + "\"; else echo \"rickshaw settings already removed\"; fi")
    thread_logger(thread_name, "Removal of rickshaw settings '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (remote_rickshaw_settings, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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
    result = run_remote(connection, "if [ -e \"" + remote_ssh_private_key + "\" ]; then rm --verbose \"" + remote_ssh_private_key + "\"; else echo \"ssh private key already removed\"; fi")
    thread_logger(thread_name, "Removal of ssh private key '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (remote_ssh_private_key, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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

    result = run_remote(connection, "podman rm --force " + container_name)
    thread_logger(thread_name, "Removal of pod '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (engine_name, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    remote_env_file = settings["dirs"]["remote"]["cfg"] + "/" + engine_name + "_env.txt"
    result = run_remote(connection, "rm --verbose " + remote_env_file)
    thread_logger(thread_name, "Removal of env file  '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (remote_env_file, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

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
        result = run_remote(connection, "umount --verbose " + mount)
        thread_logger(thread_name, "regular unmounting of '%s' resulted in record code %d:\nstdout:\n%sstderr:\n%s" % (mount, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    for mount in chroot_info["mounts"]["rbind"]:
        result = run_remote(connection, "umount --verbose --recursive " + mount)
        thread_logger(thread_name, "recursive unmounting of '%s' resulted in record code %d:\nstdout:\n%sstderr:\n%s" % (mount, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    result = run_remote(connection, "podman rm --force " + container_name)
    thread_logger(thread_name, "Removal of pod '%s' resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (engine_name, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, engine_name = engine_name)

    remove_ssh_private_key(connection, thread_name, remote_name, engine_name)

    remove_rickshaw_settings(connection, thread_name, remote_name, engine_name)

    return

def gmtimestamp_to_gmepoch(gmtimestamp):
    """
    Convert the provided formatted UTC timestamp to a UTC epoch timestamp

    Args:
        gmtimestamp (str): A formatted UTC tiemstamp such as "2024-04-11 14:30:39 +0000 UTC"

    Globals:
        None

    Returns:
        gmepoch (int): A UTC epoch timestamp such as 1712949457
    """
    gmepoch = None

    time_struct = time.strptime(gmtimestamp, "%Y-%m-%d %H:%M:%S %z %Z")

    gmepoch = calendar.timegm(time_struct)

    return gmepoch

def image_expiration_gmepoch():
    """
    Determine the UTC epoch timetamp that any image created before is expired

    Args:
        None

    Globals:
        None

    Returns:
        gmepoch (int): A UTC epoch timestamp such as 1712949457 from 2 weeks ago.  Any image with a creation
                       date older than this is expired
    """
    #           seconds/min  minutes/hour  hours/day  days
    two_weeks = 60           * 60          * 24       * 14

    gmepoch = calendar.timegm(time.gmtime()) - two_weeks

    return gmepoch

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
    thread_logger(thread_name, "Removing image '%s'" % (image), log_level = "warning", remote_name = remote_name, log_prefix = log_prefix)

    result = run_remote(connection, "podman rmi " + image)
    thread_logger(thread_name, "Removing podman image '%s' gave return code %d:\nstdout:\n%sstderr:\n%s" % (image, result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, log_prefix = log_prefix)

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
        None

    Returns:
        None
    """
    log_prefix = "RIM"
    thread_logger(thread_name, "Performing container image management", remote_name = remote_name, log_prefix = log_prefix)

    result = run_remote(connection, "podman images --all")
    thread_logger(thread_name, "All podman images on this remote host before running image manager:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

    images = dict()
    images["rickshaw"] = dict()
    images["podman"] = dict()

    result = run_remote(connection, "cat /var/lib/crucible/remotehosts-container-image-census")
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
    thread_logger(thread_name, "images[rickshaw]:\n%s" % (dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)

    result = run_remote(connection, "podman images --format='{{.Repository}}:{{.Tag}}|{{.CreatedAt}}'")
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
                "created": gmtimestamp_to_gmepoch(image_timestamp)
            }
    thread_logger(thread_name, "images[podman]:\n%s" % (dump_json(images["podman"])), remote_name = remote_name, log_prefix = log_prefix)

    image_expiration = image_expiration_gmepoch()
    thread_logger(thread_name, "Images created before %d will be considered expired" % (image_expiration), remote_name = remote_name, log_prefix = log_prefix)

    deletes = []
    for image in images["rickshaw"].keys():
        if not image in images["podman"]:
            thread_logger(thread_name, "Rickshaw image '%s' is no longer present in podman images, removing from consideration" % (image), log_level = "warning", remote_name = remote_name, log_prefix = log_prefix)
            deletes.append(image)
            continue

        images["rickshaw"][image]["timestamps"].sort(reverse = True)

        images["rickshaw"][image]["uses"] = len(images["rickshaw"][image]["timestamps"])

        images["rickshaw"][image]["latest-usage"] = images["rickshaw"][image]["timestamps"][0]

        del images["rickshaw"][image]["timestamps"]
    for image in deletes:
        del images["rickshaw"][image]
    thread_logger(thread_name, "images[rickshaw]:\n%s" % (dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)

    deletes = dict()
    deletes["rickshaw"] = []
    deletes["podman"] = []
    for image in images["podman"].keys():
        m = re.search(r"client-server", image)
        if m is None:
            thread_logger(thread_name, "Podman image '%s' is not a client-server image, ignoring" % (image), remote_name = remote_name, log_prefix = log_prefix)
            deletes["podman"].append(image)
            continue

        if not image in images["rickshaw"]:
            thread_logger(thread_name, "Podman image '%s' is not present in rickshaw container image census, removing it from the image cache" % (image), remote_name = remote_name, log_prefix = log_prefix)
            deletes["podman"].append(image)
            remove_image(thread_name, remote_name, log_prefix, connection, image)
        elif images["podman"][image]["created"] < image_expiration:
            thread_logger(thread_name, "Podman image '%s' has expired, removing it from the image cache" % (image), remote_name = remote_name, log_prefix = log_prefix)
            deletes["podman"].append(image)
            deletes["rickshaw"].append(image)
            remove_image(thread_name, remote_name, log_prefix, connection, image)
        else:
            thread_logger(thread_name, "Podman image '%s' is valid and remains under consideration" % (image), remote_name = remote_name, log_prefix = log_prefix)
    for kind in deletes.keys():
        for image in deletes[kind]:
            del images[kind][image]
    thread_logger(thread_name, "images[rickshaw]:\n%s" % (dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)
    thread_logger(thread_name, "images[podman]:\n%s" % (dump_json(images["podman"])), remote_name = remote_name, log_prefix = log_prefix)

    cache_size = 0

    if cache_size < image_max_cache_size:
        thread_logger(thread_name, "Attemping to cache the most recently used image(s)", remote_name = remote_name, log_prefix = log_prefix)
        sorted_images = sorted(images["rickshaw"].items(), key = lambda x: (x[1]["latest-usage"], x[0]), reverse = True)
        thread_logger(thread_name, "latest sorted images[rickshaw]:\n%s" % (dump_json(sorted_images)), remote_name = remote_name, log_prefix = log_prefix)

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

        thread_logger(thread_name, "images[rickshaw]:\n%s" % (dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)
    else:
        thread_logger(thread_name, "No images can be cached due to maximum image cache size of %d" % (image_max_cache_size), remote_name = remote_name, log_prefix = log_prefix)

    if cache_size < image_max_cache_size:
        thread_logger(thread_name, "Attempting to cache the most used image(s)", remote_name = remote_name, log_prefix = log_prefix)
        sorted_images = sorted(images["rickshaw"].items(), key = lambda x: (x[1]["uses"], x[0]), reverse = True)
        thread_logger(thread_name, "usage sorted images[rickshaw]:\n%s" % (dump_json(sorted_images)), remote_name = remote_name, log_prefix = log_prefix)

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

        thread_logger(thread_name, "images[rickshaw]:\n%s" % (dump_json(images["rickshaw"])), remote_name = remote_name, log_prefix = log_prefix)
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

    result = run_remote(connection, "podman image prune -f")
    thread_logger(thread_name, "Pruning dangling images resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote_name, log_prefix = log_prefix)

    result = run_remote(connection, "podman images --all")
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

        with remote_connection(remote["config"]["host"], remote["config"]["settings"]["remote-user"]) as con:
            result = run_remote(con, "mount")
            thread_logger(thread_name, "All mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = run_remote(con, "podman ps --all")
            thread_logger(thread_name, "All podman pods on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = run_remote(con, "podman mount")
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

            result = run_remote(con, "mount")
            thread_logger(thread_name, "All mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = run_remote(con, "podman ps --all")
            thread_logger(thread_name, "All podman pods on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

            result = run_remote(con, "podman mount")
            thread_logger(thread_name, "All podman container mounts on this remote host:\nstdout:\n%sstderr:\n%s" % (result.stdout, result.stderr), remote_name = remote_name)

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

        with remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as con:
            remote_image_manager(thread_name, remote, con, my_run_file_remote["config"]["settings"]["image-cache-size"])

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

        with remote_connection(remote, my_run_file_remote["config"]["settings"]["remote-user"]) as con:
            local_dir = settings["dirs"]["local"]["sysinfo"] + "/" + remote
            my_make_dirs(local_dir)

            local_packrat_file = args.packrat_dir + "/packrat"
            remote_packrat_file = settings["dirs"]["remote"]["run"] + "/packrat"
            result = con.put(local_packrat_file, remote_packrat_file)
            thread_logger(thread_name, "Copied %s to %s:%s" % (local_packrat_file, remote, remote_packrat_file), remote_name = remote)

            result = run_remote(con, remote_packrat_file + " " + settings["dirs"]["remote"]["sysinfo"])
            thread_logger(thread_name, "Running packrat resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote)

            result = run_remote(con, "tar --create --directory " + settings["dirs"]["remote"]["sysinfo"]  + " packrat-archive | xz --stdout | base64")
            thread_logger(thread_name, "Transferring packrat files resulted in return code %d:\nstderr:\n%s" % (result.exited, result.stderr), log_level = get_result_log_level(result), remote_name = remote)

            archive_file = local_dir + "/packrat.tar.xz"
            with open(archive_file, "wb") as archive_file_fp:
                archive_file_fp.write(base64.b64decode(result.stdout))
            thread_logger(thread_name, "Wrote packrat archive to '%s'" % (archive_file), remote_name = remote)

            result = run_local("xz --decompress --stdout " + archive_file + " | tar --extract --verbose --directory " + local_dir)
            thread_logger(thread_name, "Unpacking packrat archive resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote)

            path = Path(archive_file)
            path.unlink()
            thread_logger(thread_name, "Removed packrat archive '%s'" % (archive_file), remote_name = remote)

            result = run_remote(con, "rm --recursive --force " + remote_packrat_file + " " + settings["dirs"]["remote"]["sysinfo"] + "/packrat-archive")
            thread_logger(thread_name, "Removing remote packrat files resulted in return code %d:\nstdout:\n%sstderr:\n%s" % (result.exited, result.stdout, result.stderr), log_level = get_result_log_level(result), remote_name = remote)

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

def process_roadblocks():
    """
    Process the beginning and ending roadblocks associated with synchronizing a test

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Starting to process roadblocks")

    new_followers_msg_payload = {
        "new-followers": settings["engines"]["new-followers"]
    }
    new_followers_msg = create_roadblock_msg("all", "all", "user-object", new_followers_msg_payload)

    new_followers_msg_file = settings["dirs"]["local"]["roadblock-msgs"] + "/new-followers.json"
    log.info("Writing new followers message to %s" % (new_followers_msg_file))
    with open(new_followers_msg_file, "w", encoding = "ascii") as new_followers_msg_file_fp:
        new_followers_msg_file_fp.write(dump_json(new_followers_msg))

    rc = do_roadblock(label = "endpoint-deploy-begin",
                      timeout = args.endpoint_deploy_timeout,
                      messages = new_followers_msg_file)
    if rc != 0:
        return rc
    rc = do_roadblock(label = "endpoint-deploy-end",
                      timeout = args.endpoint_deploy_timeout)
    if rc != 0:
        return rc

    rc = do_roadblock(label = "engine-init-begin",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["engine-start"])
    if rc != 0:
        return rc
    engine_init_msgs = engine_init()
    rc = do_roadblock(label = "engine-init-end",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["engine-start"],
                      messages = engine_init_msgs)
    if rc != 0:
        return rc

    rc = do_roadblock(label = "get-data-begin",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])
    if rc != 0:
        return rc
    rc = do_roadblock(label = "get-data-end",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])
    if rc != 0:
        return rc

    rc = do_roadblock(label = "collect-sysinfo-begin",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["collect-sysinfo"])
    if rc != 0:
        return rc
    collect_sysinfo()
    rc = do_roadblock(label = "collect-sysinfo-end",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["collect-sysinfo"])
    if rc != 0:
        return rc

    rc = do_roadblock(label = "start-tools-begin",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])
    if rc != 0:
        return rc
    rc = do_roadblock(label = "start-tools-end",
                      timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])
    if rc != 0:
        return rc

    rc = process_bench_roadblocks()
    if rc != 0:
        return rc

    do_roadblock(label = "stop-tools-begin",
                 timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])
    do_roadblock(label = "stop-tools-end",
                 timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])

    do_roadblock(label = "send-data-begin",
                 timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])
    do_roadblock(label = "send-data-end",
                 timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])

    do_roadblock(label = "endpoint-cleanup-begin",
                 timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])
    remote_cleanup()
    do_roadblock(label = "endpoint-cleanup-end",
                 timeout = settings["rickshaw"]["roadblock"]["timeouts"]["default"])

    return 0

def setup_logger():
    """
    Setup the logging infrastructure that is used for everything except validation

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters

    Returns:
        a logging instance
    """
    log_format = '[LOG %(asctime)s %(levelname)s %(module)s %(funcName)s:%(lineno)d] %(message)s'
    match args.log_level:
        case "debug":
            logging.basicConfig(level = logging.DEBUG, format = log_format, stream = sys.stdout)
        case "normal" | _:
            logging.basicConfig(level = logging.INFO, format = log_format, stream = sys.stdout)

    return logging.getLogger(__file__)

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

    if args.validate:
        return(validate())

    log = setup_logger()

    log_cli()
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
    rc = process_roadblocks()

    log.info("Logging 'final' settings data structure")
    log_settings(mode = "settings")
    log.info("remotehosts endpoint exiting")
    return rc

if __name__ == "__main__":
    args = process_options()
    log = None
    settings = dict()
    exit(main())
