#!/usr/bin/python3

"""
Endpoint to connect to a k8s cluster (1 or more nodes)
"""

import copy
from fabric import Connection
import jsonschema
import logging
import lzma
import os
from paramiko import ssh_exception
from pathlib import Path
import re
import sys
import tempfile
import threading
import time

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

endpoint_default_settings = {
    "controller-ip-address": None,
    "cpu-partitioning": False,
    "osruntime": "pod",
    "disable-tools": {
        "all": False,
        "masters": False,
        "workers": False
    },
    "prefix": {
        "namespace": "crucible-rickshaw",
        "pod": "rickshaw"
    }
}

# this data structure is specifically created to be separate from
# everything else so that will not be logged due to the nature of it's
# contents (authorization tokens)
image_pull_secret_crds = dict()

endpoint_default_verifications = dict()

def normalize_endpoint_settings(endpoint, rickshaw):
    """
    Normalize the endpoint settings by determining where default settings need to be applied and expanding ID ranges

    Args:
        endpoint (dict): The specific endpoint dictionary from the run-file that this endpoint instance is handling
        rickshaw (dict): The rickshaw settings dictionary

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        endpoint_default_settings (dict): the endpoint default settings

    Returns:
        endpoint (dict): The normalized endpoint dictionary
    """
    endpoint["engines"]["defaults"] = dict()
    endpoint["engines"]["defaults"]["settings"] = {
        "controller-ip-address": endpoint_default_settings["controller-ip-address"],
        "cpu-partitioning": endpoint_default_settings["cpu-partitioning"],
        "osruntime": endpoint_default_settings["osruntime"],
        "userenv": rickshaw["userenvs"]["default"]["benchmarks"]
    }
    endpoint["engines"]["defaults"]["verifications"] = dict()

    if not "disable-tools" in endpoint:
        endpoint["disable-tools"] = copy.deepcopy(endpoint_default_settings["disable-tools"])
    else:
        for key in endpoint_default_settings["disable-tools"].keys():
            if not key in endpoint["disable-tools"]:
                endpoint["disable-tools"][key] = endpoint_default_settings["disable-tools"][key]

    if not "namespace" in endpoint:
        # the user didn't request any specific namespace settings so
        # configure the default
        endpoint["namespace"] = {
            "type": "crucible"
        }
    if endpoint["namespace"]["type"] == "unique":
        prefix = endpoint_default_settings["prefix"]["namespace"]
        if "prefix" in endpoint["namespace"]:
            prefix = endpoint["namespace"]["prefix"]
        endpoint["namespace"]["name"] = "%s__%s" % (prefix, args.run_id)
    elif endpoint["namespace"]["type"] == "crucible":
        endpoint["namespace"]["name"] = endpoint_default_settings["prefix"]["namespace"]
    elif endpoint["namespace"]["type"] == "custom":
        # there should be nothing to do here as the namespace name
        # should already be defined
        pass

    default_cfg_block_idx = None
    found_defaults = False
    if "config" in endpoint:
        for cfg_block_idx,cfg_block in enumerate(endpoint["config"]):
            if isinstance(cfg_block["targets"], str) and cfg_block["targets"] == "default":
                if found_defaults:
                    msg = "Found more than one defaults target"
                    if args.validate:
                        endpoints.validate_error(msg)
                    else:
                        logger.error(msg)
                    return None
                found_defaults = True
                default_cfg_block_idx = cfg_block_idx
                for key in cfg_block["settings"].keys():
                    endpoint["engines"]["defaults"]["settings"][key] = cfg_block["settings"][key]
                if "verifications" in cfg_block:
                    for key in cfg_block["verifications"].keys():
                        endpoint["engines"]["defaults"]["verifications"][key] = cfg_block["verifications"][key]
    if not default_cfg_block_idx is None:
        del endpoint["config"][default_cfg_block_idx]

    if endpoint["engines"]["defaults"]["settings"]["controller-ip-address"] is None:
        if "controller-ip-address" in endpoint:
            endpoint["engines"]["defaults"]["settings"]["controller-ip-address"] = endpoint["controller-ip-address"]
        else:
            try:
                endpoint["engines"]["defaults"]["settings"]["controller-ip-address"] = endpoints.get_controller_ip(endpoint["host"])
            except ValueError as e:
                msg = "While determining default controller IP address encountered exception '%s'" % (str(e))
                if args.validate:
                    endpoints.validate_error(msg)
                else:
                    logger.error(msg)
                return None

    for engine_role in [ "client", "server" ]:
        if engine_role in endpoint["engines"]:
            try:
                endpoint["engines"][engine_role] = endpoints.expand_ids(endpoint["engines"][engine_role])
            except ValueError as e:
                msg = "While expanding endpoint '%s' engines encountered exception '%s'" % (engine_role, str(e))
                if args.validate:
                    endpoints.validate_error(msg)
                else:
                    logger.error(msg)
                return None

    if "config" in endpoint:
        for cfg_block_idx,cfg_block in enumerate(endpoint["config"]):
            for target in cfg_block["targets"]:
                try:
                    target["ids"] = endpoints.expand_ids(target["ids"])
                except ValueError as e:
                    msg = "While expanding IDs for '%s' in config block at index %d encounterd exception '%s'" % (target["role"], cfg_block_idx, str(e))
                    if args.validate:
                        endpoints.validate_error(msg)
                    else:
                        logger.error(msg)
                    return None

    endpoint["engines"]["settings"] = dict()
    endpoint["engines"]["verifications"] = dict()
    if "config" in endpoint:
        for cfg_block_idx,cfg_block in enumerate(endpoint["config"]):
            for target in cfg_block["targets"]:
                if not target["role"] in endpoint["engines"]:
                    msg = "Found engine role '%s' in config block at index %d but not in the endpoint engines config" % (target["role"], cfg_block_idx)
                    if args.validate:
                        endpoints.validate_error(msg)
                    else:
                        logger.error(msg)
                    return None

                if not target["role"] in endpoint["engines"]["settings"]:
                    endpoint["engines"]["settings"][target["role"]] = dict()

                if not target["role"] in endpoint["engines"]["verifications"]:
                    endpoint["engines"]["verifications"][target["role"]] = dict()

                for engine_id in target["ids"]:
                    if not engine_id in endpoint["engines"][target["role"]]:
                        msg = "Found engine with ID %d and role '%s' in config block at index %d that is not owned by this endpoint" % (engine_id, target["role"], cfg_block_idx)
                        if args.validate:
                            endpoints.validate_error(msg)
                        else:
                            logger.error(msg)
                        return None

                    if engine_id not in endpoint["engines"]["settings"][target["role"]]:
                        endpoint["engines"]["settings"][target["role"]][engine_id] = dict()

                    if engine_id not in endpoint["engines"]["verifications"][target["role"]]:
                        endpoint["engines"]["verifications"][target["role"]][engine_id] = dict()

                    for key in cfg_block["settings"].keys():
                        if key in endpoint["engines"]["settings"][target["role"]][engine_id]:
                            msg = "Overriding previously defined value for key '%s' for engine ID %d with role '%s' while processing config block at index %d" % (key, engine_id, target["role"], cfg_block_idx)
                            if args.validate:
                                endpoints.validate_warning(msg)
                            else:
                                logger.warning(msg)
                        endpoint["engines"]["settings"][target["role"]][engine_id][key] = cfg_block["settings"][key]

                    if "verifications" in cfg_block:
                        for key in cfg_block["verifications"].keys():
                            if key in endpoint["engines"]["verifications"][target["role"]][engine_id]:
                                msg = "Overriding previously defined value for key '%s' for engine ID %s with role '%s' while processing config block at index %d" % (key, engine_id, target["role"], cfg_block_idx)
                                if args.validate:
                                    endpoints.validate_warning(msg)
                                else:
                                    logger.warning(msg)
                            endpoint["engines"]["verifications"][target["role"]][engine_id][key] = cfg_block["verifications"][key]

                    for key in endpoint["engines"]["defaults"]["settings"].keys():
                        if not key in endpoint["engines"]["settings"][target["role"]][engine_id]:
                            endpoint["engines"]["settings"][target["role"]][engine_id][key] = endpoint["engines"]["defaults"]["settings"][key]

                    for key in endpoint["engines"]["defaults"]["verifications"].keys():
                        if not key in endpoint["engines"]["settings"][target["role"]][engine_id]:
                            endpoint["engines"]["settings"][target["role"]][engine_id][key] = endpoint["engines"]["defaults"]["verifications"][key]

    for engine_role in [ "client", "server" ]:
        if engine_role in endpoint["engines"]:
            if engine_role not in endpoint["engines"]["settings"]:
                endpoint["engines"]["settings"][engine_role] = dict()

            if engine_role not in endpoint["engines"]["verifications"]:
                endpoint["engines"]["verifications"][engine_role] = dict()
            
            for engine_id in endpoint["engines"][engine_role]:
                if engine_id not in endpoint["engines"]["settings"][engine_role]:
                    endpoint["engines"]["settings"][engine_role][engine_id] = endpoint["engines"]["defaults"]["settings"]

                if engine_id not in endpoint["engines"]["verifications"][engine_role]:
                    endpoint["engines"]["verifications"][engine_role][engine_id] = endpoint["engines"]["defaults"]["verifications"]

    return endpoint

def find_k8s_bin(validate, connection, remote_env):
    """
    Figure out what the K8S control binary is for this environment

    Args:
        validate (bool): Is the caller the validate function
        connection (Fabric): The Fabric connection to use to run commands
        remote_env (dict): a dictionary of environment variables to present on the remote side

    Globals:
        args (namespace): the script's CLI parameters

    Returns"
        None: No control binary could be identified
        k8s_bin (str): The name of the K8S control binary
    """
    debug_output = False
    if args.log_level == "debug":
        debug_output = True

    result = endpoints.run_remote(connection, "oc", validate = validate, debug = debug_output, env = remote_env)
    if result.exited == 0:
        return "oc"

    result = endpoints.run_remote(connection, "kubectl", validate = validate, debug = debug_output, env = remote_env)
    if result.exited == 0:
        return "kubectl"

    result = endpoints.run_remote(connection, "microk8s kubectl", validate = validate, debug = debug_output, env = remote_env)
    if result.exited == 0:
        return "microk8s kubectl"

    return None

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

    valid, err = validate_schema(endpoint_settings, args.rickshaw_dir + "/schema/kube.json")
    if not valid:
        endpoints.validate_error(err)
        return 1

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
    for engine_role in endpoint_settings["engines"]["settings"].keys():
        engine_types.add(engine_role)
    if not endpoint_settings["disable-tools"]["all"] and not endpoint_settings["disable-tools"]["masters"]:
        engine_types.add("master")
    if not endpoint_settings["disable-tools"]["all"] and not endpoint_settings["disable-tools"]["workers"]:
        engine_types.add("worker")
    
    endpoints.validate_comment("engine types that this endpoint is using")
    endpoints.validate_log("engine-types %s" % (" ".join(list(engine_types))))

    engines = dict()
    userenvs = []
    for engine_role in endpoint_settings["engines"]["settings"].keys():
        if not engine_role in engines:
            engines[engine_role] = []
        
        for engine_id in endpoint_settings["engines"]["settings"][engine_role].keys():
            engines[engine_role].append(engine_id)

            if not endpoint_settings["engines"]["settings"][engine_role][engine_id]["userenv"] in userenvs:
                userenvs.append(endpoint_settings["engines"]["settings"][engine_role][engine_id]["userenv"])

    endpoints.validate_comment("engines: %s" % (engines))
    for engine_role in engines.keys():
        engines[engine_role].sort()
        endpoints.validate_log("%s %s" % (engine_role, " ".join(map(str, engines[engine_role]))))

        if len(engines[engine_role]) != len(set(engines[engine_role])):
            endpoints.validate_error("There are duplicate IDs prsent for '%s'" % (engine_role))

        for engine_id in engines[engine_role]:
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

    debug_output = False
    if args.log_level == "debug":
        debug_output = True
    try:
        with endpoints.remote_connection(endpoint_settings["host"], endpoint_settings["user"], validate = True) as con:
            result = endpoints.run_remote(con, "uptime", validate = True, debug = debug_output)
            endpoints.validate_comment("remote login verification for %s with user %s: rc=%d and stdout=[%s] and stderr=[%s]" % (endpoint_settings["host"], endpoint_settings["user"], result.exited, result.stdout.rstrip('\n'), result.stderr.rstrip('\n')))

            remote_env = {}
            if "kubeconfig" in endpoint_settings:
                endpoints.validate_comment("found kubeconfig user override with value '%s'" % (endpoint_settings["kubeconfig"]))
                remote_env["KUBECONFIG"] = endpoint_settings["kubeconfig"]

            k8s_bin = find_k8s_bin(True, con, remote_env)
            if k8s_bin is None:
                endpoints.validate_error("Failed to determine the k8s control binary")
            else:
                endpoints.validate_comment("determined k8s control binary is '%s'" % (k8s_bin))

                result = endpoints.run_remote(con, k8s_bin + " version", validate = True, debug = debug_output, env = remote_env)
                result.stdout = re.sub(r'\n', ' | ', result.stdout)
                endpoints.validate_comment("remote '%s' presence check for %s: rc=%d stdout=[%s] and stderr=[%s]" % (k8s_bin, endpoint_settings["host"], result.exited, result.stdout.rstrip(' | '), result.stderr.rstrip('\n')))
                if result.exited != 0:
                    endpoints.validate_error("Failed to run '%s version' on endpoint host '%s' as user '%s'" % (k8s_bin, endpoint_settings["host"], endpoint_settings["user"]))

                result = endpoints.run_remote(con, k8s_bin + " get nodes", validate = True, debug = debug_output, env = remote_env)
                result.stdout = re.sub(r'\n', ' | ', result.stdout)
                endpoints.validate_comment("remote '%s' functional check for %s: rc=%d stdout=[%s] and stderr=[%s]" % (k8s_bin, endpoint_settings["host"], result.exited, result.stdout.rstrip(' | '), result.stderr.rstrip('\n')))
                if result.exited != 0:
                    endpoints.validate_error("Failed to functionally verify '%s' on endpoint host '%s' as user '%s'" % (k8s_bin, endpoint_settings["host"], endpoint_settings["user"]))
    except ssh_exception.AuthenticationException as e:
        endpoints.validate_error("remote login verification for %s with user %s resulted in an authentication exception '%s'" % (endpoint_settings["host"], endpoint_settings["user"], str(e)))
    except ssh_exception.NoValidConnectionsError as e:
        endpoints.validate_error("remote login verification for %s with user %s resulted in an connection exception '%s'" % (endpoint_settings["host"], endpoint_settings["user"], str(e)))

    return 0

def check_base_requirements():
    """
    Check if the base requirements to perform a non-validation run were provided

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
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

    settings["misc"]["remote-env"] = {}
    if "kubeconfig" in settings["run-file"]["endpoints"][args.endpoint_index]:
        logger.info("found kubeconfig user override with value '%s'" % (settings["run-file"]["endpoints"][args.endpoint_index]["kubeconfig"]))
        settings["misc"]["remote-env"]["KUBECONFIG"] = settings["run-file"]["endpoints"][args.endpoint_index]["kubeconfig"]

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"], validate = False) as con:
        settings["misc"]["k8s-bin"] = find_k8s_bin(False, con, settings["misc"]["remote-env"])
        if settings["misc"]["k8s-bin"] is None:
            return 1
        else:
            logger.info("determined k8s control binary is '%s'" % (settings["misc"]["k8s-bin"]))

    return 0

def clean_k8s_namespace(connection):
    """
    Clean the namespace by deleting all components inside it

    Args:
        connection (Fabric): The Fabric connection to use to run commands

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0: success
        1: failure
    """
    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    logger.info("Cleaning namespace '%s'" % (endpoint["namespace"]["name"]))

    component_errors = False
    for component in [ "pods", "services", "secrets" ]:
        logger.info("Cleaning component: %s" % (component))
        cmd = "%s delete --namespace %s %s --all" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"], component)
        result = endpoints.run_remote(connection, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        endpoints.log_result(result)
        if result.exited != 0:
            component_errors = True

    if component_errors:
        return 1
    else:
        return 0

def init_k8s_namespace():
    """
    Initialize the K8S namespace.  What this entails could vary depending on
    configuration settings and the current state of the cluster.

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0: success
        1: failure
    """
    logger.info("Initializing K8S Namespace")

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"]) as con:
        namespace_exists = False
        cmd = "%s get namespace %s" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        endpoints.log_result(result, level = "info")
        if result.exited == 0:
            namespace_exists = True

        if namespace_exists:
            logger.info("Found existing namespace '%s', going to clean it" % (endpoint["namespace"]["name"]))
            if clean_k8s_namespace(con):
                return 1
        else:
            logger.info("No namespace '%s' found, creating it" % (endpoint["namespace"]["name"]))
            cmd = "%s create namespace %s" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
            endpoints.log_result(result)
            if result.exited != 0:
                return 1

    return 0

def get_k8s_config():
    """
    Collect information from K8S that will be used to construct the config files for the test

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0: success
        1: failure
    """
    logger.info("Getting K8S config")

    settings["misc"]["k8s"] = dict()
    settings["misc"]["k8s"]["nodes"] = dict()

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"]) as con:
        cmd = "%s get nodes" % (settings["misc"]["k8s-bin"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        endpoints.log_result(result)
        if result.exited != 0:
            return 1

        cmd = "%s get nodes --output json" % (settings["misc"]["k8s-bin"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        endpoints.log_result(result)
        if result.exited == 0:
            settings["misc"]["k8s"]["nodes"]["cluster"] = json.loads(result.stdout)
            settings["misc"]["k8s"]["nodes"]["endpoint"] = dict()
        else:
            return 1

        nr_nodes = len(settings["misc"]["k8s"]["nodes"]["cluster"]["items"])
        if nr_nodes > 0:
            logger.info("Discovered %d nodes" % (nr_nodes))
        else:
            logger.error("Did not find any nodes")
            return 1

        settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"] = []
        settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"] = []
        for node in settings["misc"]["k8s"]["nodes"]["cluster"]["items"]:
            name = node["metadata"]["name"]

            # OCP
            if "node-role.kubernetes.io/worker" in node["metadata"]["labels"]:
                settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"].append(name)
            if "node-role.kubernetes.io/master" in node["metadata"]["labels"]:
                settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"].append(name)

            # microk8s
            if "node.kubernetes.io/microk8s-controlplane" in node["metadata"]["labels"]:
                settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"].append(name)
                settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"].append(name)

        node_count_fault = False
        logger.info("Found %d masters: %s" % (len(settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]), settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]))
        if len(settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]) == 0:
            logger.error("No masters found")
            node_count_fault = True
        logger.info("Found %d workers: %s" % (len(settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]), settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]))
        if len(settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]) == 0:
            logger.error("No workers found")
            node_count_fault = True
        if node_count_fault:
            return 1

    return 0

def compile_object_configs():
    """
    Process endpoint settings and compile the config information for all k8s entities

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0
    """
    logger.info("Compiling object configs")

    if not "engines" in settings:
        settings["engines"] = dict()
    settings["engines"]["endpoint"] = dict()
    settings["engines"]["profiler-mapping"] = dict()
    settings["engines"]["new-followers"] = []

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]
    roles = [ "client", "server" ]

    settings["engines"]["endpoint"]["roles"] = dict()
    for role in roles:
        if not role in settings["engines"]["endpoint"]["roles"]:
            settings["engines"]["endpoint"]["roles"][role] = []

        if role in endpoint["engines"]:
            for csid in endpoint["engines"][role]:
                settings["engines"]["endpoint"]["roles"][role].append(csid)

    logger.info("This endpoint will run these clients: %s" % (list(map(lambda x: "client-" + str(x), settings["engines"]["endpoint"]["roles"]["client"]))))
    logger.info("This endpoint will run these servers: %s" % (list(map(lambda x: "server-" + str(x), settings["engines"]["endpoint"]["roles"]["server"]))))

    settings["engines"]["endpoint"]["classes"] = dict()

    settings["engines"]["endpoint"]["classes"]["cpu-partitioning"] = dict()
    settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["with"] = []
    settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["without"] = []
    for role in roles:
        if role in endpoint["engines"]["settings"]:
            csids = list(endpoint["engines"]["settings"][role].keys())
            csids.sort()
            for csid in csids:
                engine = {
                    "role": role,
                    "id": int(csid)
                }

                if endpoint["engines"]["settings"][role][csid]["cpu-partitioning"]:
                    settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["with"].append(engine)
                else:
                    settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["without"].append(engine)

    endpoints.log_settings(settings, mode = "engines")

    return 0

def create_pod_crd(role = None, id = None, node = None):
    """
    Create a pod CRD

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule them all
        image_pull_secret_crds (dict): a data structure to hold image pull secret CRDs so that they are not logged

    Returns:
        crd, 0: success
        crd, 1: failure
    """
    logger.info("Creating CRD for engine %s-%d" % (role, id))

    if role is None or id is None:
        logger.error("You must define role and id when calling this function")
        return None, 1

    if role == "master" or role == "worker":
        if node is None:
            logger.error("You must define node when role is either 'master' or 'worker'")
            return None, 1

    name = "%s-%d" % (role, id)
    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    pod_settings = None
    if role == "client" or role == "server":
        pod_settings = endpoint["engines"]["settings"][role][id]
    elif role == "worker" or role == "master":
        pod_settings = endpoint["engines"]["defaults"]["settings"]
    if pod_settings is None:
        logger.error("Could not find mapping for pod settings")
        return None,1

    crd = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "%s-%s" % (endpoint_default_settings["prefix"]["pod"], name),
            "namespace": endpoint["namespace"]["name"],
        },
        "spec": {
            "restartPolicy": "Never"
        }
    }

    if role == "client" or role == "server":
        crd["metadata"]["labels"] = {
            "app": crd["metadata"]["name"]
        }

        if "annotations" in pod_settings:
            crd["metadata"]["annotations"] = copy.deepcopy(pod_settings["annotations"])

    if role == "master":
        # ensure master pod can be scheduled on the master node in case
        # it is not scheduable
        crd["spec"]["tolerations"] = [
            {
                "key": "node-role.kubernetes.io/master",
                "effect": "NoSchedule"
            }
        ]

    crd["spec"]["volumes"] = [
        {
            "name": "shared-engines-dir",
            "emptyDir": {
                "sizeLimit": "10Mi"
            }
        },
        {
            "name": "hostfs-firmware",
            "hostPath": {
                "path": "/lib/firmware",
                "type": "Directory"
                }
        }
    ]

    if role == "worker" or role == "master":
        # guarantee specific node placement
        crd["spec"]["nodeSelector"] = {
            "kubernetes.io/hostname": node
        }

        # we want to simulate a host like environment
        crd["spec"]["hostPID"] = True
        crd["spec"]["hostNetwork"] = True
        crd["spec"]["hostIPC"] = True

        # specific filesystem access is required
        crd["spec"]["volumes"].extend(
            [
                {
                    "name": "hostfs-run",
                    "hostPath": {
                        "path": "/var/run",
                        "type": "Directory"
                    }
                },
                {
                    "name": "hostfs-modules",
                    "hostPath": {
                        "path": "/lib/modules",
                        "type": "Directory"
                    }
                }
            ]
        )

    has_hugepages = False

    if role == "client" or role == "server":
        if "securityContext" in pod_settings and "pod" in pod_settings["securityContext"]:
            crd["spec"]["securityContext"] = copy.deepcopy(pod_settings["securityContext"]["pod"])

        if "runtimeClassName" in pod_settings:
            crd["spec"]["runtimeClassName"] = pod_settings["runtimeClassName"]

        if "nodeSelector" in pod_settings:
            crd["spec"]["nodeSelector"] = copy.deepcopy(pod_settings["nodeSelector"])

        if "hostNetwork" in pod_settings:
            crd["spec"]["hostNetwork"] = pod_settings["hostNetwork"]

        if "volumes" in pod_settings:
            for volume in pod_settings["volumes"]:
                new_volume = {
                    "name": volume["name"]
                }

                # this loop should only execute once; the JSON schema
                # defines the min and max properties as 1, but by
                # writing it this way we don't have to handle the
                # individual values (of which there are many) -- the
                # loop is generic and should adapt to the different
                # values automatically
                for key in volume["volume"].keys():
                    new_volume[key] = copy.deepcopy(volume["volume"][key])

                crd["spec"]["volumes"].append(new_volume)

        if "resources" in pod_settings:
            for key in pod_settings["resources"].keys():
                if "hugepages" in key:
                    has_hugepages = True

                    crd["spec"]["volumes"].append({
                        "name": "hugepage",
                        "emptyDir": {
                            "medium": "HugePages"
                        }
                    })

                    break

    container_names = []
    if role == "client" or role == "server":
        container_names.append(name)
    if role == "worker" or role == "master":
        for tool in settings["engines"]["profiler-mapping"].keys():
            mapping = settings["engines"]["profiler-mapping"][tool]
            engine_name = "%s-%s-%d" % ("profiler", mapping["label"], id)
            mapping["ids"].append(engine_name)
            container_names.append(engine_name)
        pass

    crd["spec"]["containers"] = []
    for container_name in container_names:
        logger.info("Adding container '%s' to pod" % (container_name))
        image = None
        if role == "client" or role == "server":
            image = endpoints.get_engine_id_image(settings, role, id, pod_settings["userenv"])
        elif role == "worker" or role == "master":
            userenv = endpoints.get_profiler_userenv(settings, container_name)
            if userenv is None:
                logger.error("Could not find userenv for engine '%s'" % (container_name))
                return crd, 1
            else:
                logger.info("Found userenv '%s' for engine '%s'" % (userenv, container_name))
            image = endpoints.get_engine_id_image(settings, role, container_name, userenv)
        if image is None:
            logger.error("Could not find image for container %s" % (container_name))
            return crd, 1
        else:
            logger.info("Found image '%s' for container '%s'" % (image, container_name))

        if "::" in image:
            # this image has a pull token that must be handled

            fields = image.split("::")
            image = fields[0]
            token_file = fields[1]

            logger.info("Processing image pull secret '%s' for image '%s'" % (token_file, image))

            if not "imagePullSecrets" in crd["spec"]:
                crd["spec"]["imagePullSecrets"] = []

            if not "pull-secrets" in settings["misc"]:
                settings["misc"]["pull-secrets"] = {}

            secret_name = endpoints.sha256_hash(token_file)
            logger.info("Image pull secret is named '%s'" % (secret_name))

            if not secret_name in settings["misc"]["pull-secrets"]:
                logger.info("Creating CRD for image pull secret named '%s'" % (secret_name))

                settings["misc"]["pull-secrets"][secret_name] = {
                    "validated": False,
                    "created": False,
                    "failed": False
                }

                # the actual pull secret CRDs are stored outside of
                # the settings data structure to prevent the secret
                # itself from being logged
                image_pull_secret_crds[secret_name] = create_pull_secret_crd(secret_name, token_file)
            else:
                logger.info("CRD for image pull secret named '%s' already exists" % (secret_name))

            add_secret = True
            for secret in crd["spec"]["imagePullSecrets"]:
                if secret["name"] == secret_name:
                    add_secret = False

            if add_secret:
                logger.info("Adding image pull secret '%s' to pod spec" % (secret_name))
                crd["spec"]["imagePullSecrets"].append({ "name": secret_name })
            else:
                logger.info("Image pull secret '%s' already added to pod spec" % (secret_name))

        container = {
            "name": container_name,
            "image": image,
            "imagePullPolicy": "Always",
            "env": [
                {
                    "name": "rickshaw_host",
                    "value": pod_settings["controller-ip-address"]
                },
                {
                    "name": "base_run_dir",
                    "value": settings["dirs"]["local"]["base"]
                },
                {
                    "name": "cs_label",
                    "value": container_name
                },
                {
                    "name": "endpoint_run_dir",
                    "value": "/endpoint-run"
                },
                {
                    "name": "endpoint",
                    "value": "kube"
                },
                {
                    "name": "osruntime",
                    "value": pod_settings["osruntime"]
                },
                {
                    "name": "roadblock_passwd",
                    "value": args.roadblock_passwd
                },
                {
                    "name": "roadblock_id",
                    "value": args.roadblock_id
                },
                {
                    "name": "max_sample_failures",
                    "value": str(args.max_sample_failures)
                },
                {
                    "name": "engine_script_start_timeout",
                    "value": str(args.engine_script_start_timeout)
                },
                {
                    "name": "ssh_id",
                    "value": settings["misc"]["ssh-private-key"]
                }
            ]
        }

        if role == "client" or role == "server":
            cpu_partitioning = None
            if pod_settings["cpu-partitioning"]:
                cpu_partitioning = 1
            else:
                cpu_partitioning = 0
            container["env"].append({
                "name": "cpu_partitioning",
                "value": str(cpu_partitioning)
            })

        container["volumeMounts"] = [
            {
                "mountPath": "/shared-engines-dir",
                "name": "shared-engines-dir"
            },
            {
                "mountPath": "/lib/firmware",
                "name": "hostfs-firmware"
            }
        ]

        if role == "worker" or role == "master":
            container["securityContext"] = {
                "privileged": True
            }

            container["volumeMounts"].extend(
                [
                    {
                        "mountPath": "/var/run",
                        "name": "hostfs-run"
                    },
                    {
                        "mountPath": "/lib/modules",
                        "name": "hostfs-modules"
                    }
                ]
            )

        if role == "client" or role == "server":
            if "securityContext" in pod_settings and "container" in pod_settings["securityContext"]:
                container["securityContext"] = copy.deepcopy(pod_settings["securityContext"]["container"])

            if "volumes" in pod_settings:
                for volume in pod_settings["volumes"]:
                    new_volume_mount = {
                        "name": volume["name"]
                    }

                    for key in volume["volumeMount"].keys():
                        new_volume_mount[key] = copy.deepcopy(volume["volumeMount"][key])

                    container["volumeMounts"].append(new_volume_mount)

            if has_hugepages:
                container["volumeMounts"].append({
                    "mountPath": "/dev/hugepages",
                    "name": "hugepage"
                })

            if "resources" in pod_settings:
                container["resources"] = copy.deepcopy(pod_settings["resources"])

        crd["spec"]["containers"].append(container)

    return crd, 0

def verify_pods_running(connection, pods, pod_details, abort_event):
    """
    Take the list of pods and verify that they are running and collect information about them

    Args:
       connection (Fabric): The Fabric connection to use to run commands
       pods (list): The list of pods that are to be verified to be in the running state
       pod_details (dict): Additional metadata that can be used to verify that the pod is properly configured
       abort_event (threading.Event): a threading.Event that signals if the deployment should be aborted

    Globals:
       log
       settings
       args

    Returns:
       engines_info (dict): 
       None: error
    """
    logger.info("Verifying that these pods are running: %s" % (pods))
    logger.info("Received these additional pod details:\n%s" % (endpoints.dump_json(pod_details)))
    pods_info = dict()
    verified_pods = []
    unverified_pods = []
    invalid_pods = []
    unverified_pods.extend(pods)

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    count = 1
    while len(unverified_pods) > 0:
        if abort_event.is_set():
            logger.info("Aborting pod verification due to an abort event")
            break

        logger.info("Loop pass %d" % (count))
        logger.info("Unverified pods: %s" % (unverified_pods))
        logger.info("Verified pods:   %s" % (verified_pods))
        logger.info("Invalid pods:    %s" % (invalid_pods))
        count += 1

        logger.info("Collecting pod status for namespace '%s'" % (endpoint["namespace"]["name"]))
        cmd = "%s get pods --namespace %s --output json" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
        result = endpoints.run_remote(connection, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        endpoints.log_result(result)
        if result.exited != 0:
            return None

        pod_status = json.loads(result.stdout)

        logger.info("Collected pod status for %d pods in namespace '%s'" % (len(pod_status["items"]), endpoint["namespace"]["name"]))

        logger.info("Analyzing pod status for namespace '%s'" % (endpoint["namespace"]["name"]))
        for pod in pod_status["items"]:
            pod_name = pod["metadata"]["name"]
            # strip off the prefix
            pod_name = re.sub(r"%s-" % (endpoint_default_settings["prefix"]["pod"]), r"", pod_name)
            logger.debug("Processing engine '%s'" % (pod_name))

            if pod_name not in pods:
                logger.info("Encountered pod '%s' that is not in my current verification list, skipping" % (pod_name))
                continue

            if pod_name in unverified_pods:
                if not "containerStatuses" in pod["status"]:
                    logger.info("containerStatuses is not yet available for pod '%s'" % (pod_name))
                    break

                logger.info("Checking status of pod '%s'" % (pod_name))
                running_containers = []
                valid_configuration = True
                for container in pod["status"]["containerStatuses"]:
                    logger.info("Checking status of container '%s:\n%s" % (container["name"], endpoints.dump_json(container["state"])))
                    if "running" in container["state"]:
                        logger.info("Container '%s' is running" % (container["name"]))
                        running_containers.append(container["name"])

                        pod_verifications = None
                        if pod_details[pod_name]["role"] in [ "client", "server" ]:
                            pod_verifications = endpoint["engines"]["verifications"][pod_details[pod_name]["role"]][pod_details[pod_name]["id"]]
                        elif pod_details[pod_name]["role"] in [ "worker", "master" ]:
                            pod_verifications = endpoint["engines"]["defaults"]["verifications"]

                        if pod_verifications is None:
                            logger.error("Could not find mapping for pod verifications")
                            valid_configuration = False
                        else:
                            if "qosClass" in pod_verifications:
                                logger.info("Pod has a qosClass verification defined")
                                if pod_verifications["qosClass"] != pod["status"]["qosClass"]:
                                    logger.error("Pod '%s' desired qosClass of '%s' but is running with qosClass of '%s'" % (pod_name, pod_verifications["qosClass"], pod["status"]["qosClass"]))
                                    valid_configuration = False
                                else:
                                    logger.info("Verified pod '%s' has desired qosClass of '%s'" % (pod_name, pod["status"]["qosClass"]))
                            else:
                                logger.info("Pod does not have a qosClass verification defined")
                    else:
                        logger.info("Container '%s' is not running" % (container["name"]))

                if len(running_containers) == len(pod["status"]["containerStatuses"]):
                    if valid_configuration:
                        logger.info("All containers in pod '%s' are running -> it is verified" % (pod_name))
                        unverified_pods.remove(pod_name)
                        verified_pods.append(pod_name)
                        pods_info[pod_name] = {
                            "name": pod_name,
                            "node": pod["spec"]["nodeName"],
                            "containers": copy.deepcopy(running_containers),
                            "pod-ip": pod["status"]["podIP"],
                            "node-ip": pod["status"]["hostIP"]
                        }
                    else:
                        logger.info("All containers in pod '%s' are running, but the pod or one of it's containers has an invalid configuration" % (pod_name))
                        unverified_pods.remove(pod_name)
                        invalid_pods.append(pod_name)
                else:
                    logger.info("There are %d containers in pod '%s' that are not yet running -> it is not verified" % ((len(pod["status"]["containerStatuses"]) - len(running_containers)), pod_name))
                    if not valid_configuration:
                        logger.error("The pod '%s' or one of it's containers has an invalid configuration" % (pod_name))

            elif pod_name in verified_pods:
                logger.info("Skipping pod '%s' because it is already verified" % (pod_name))
            elif pod_name in invalid_pods:
                logger.warning("Skipping pod '%s' because is is invalid" % (pod_name))
            else:
                logger.warning("Pod '%s' is untracked" % (pod_name))

        if len(unverified_pods) > 0:
            sleep_time = 10
            logger.info("There are %d unverified pods, sleeping for %d seconds before checking again" % (len(unverified_pods), sleep_time))
            time.sleep(sleep_time)

    if len(verified_pods) != len(pods):
        logger.error("Unable to verify all pods")
        logger.error("all        - %d: %s" % (len(pods), pods))
        logger.error("unverified - %d: %s" % (len(unverified_pods), unverified_pods))
        logger.error("verified   - %d: %s" % (len(verified_pods), verified_pods))
        logger.error("invalid    - %d: %s" % (len(invalid_pods), invalid_pods))
    else:
        logger.info("Verified all %d pods: %s" % (len(pods), pods))

    logger.info("Returning pod info:\n%s" % (endpoints.dump_json(pods_info)))

    return pods_info

def create_cs_pods(cpu_partitioning = None, abort_event = None):
    """
    Generate Validate, and Create Pods for the client/server engines with the given CPU partitioning configuration

    Args:
        cpu_partitioning (bool): whether or not to start pods with CPU partitioning enabled
        abort_event (threading.Event): a threading.Event that signals if the deployment should be aborted

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule them all
        image_pull_secret_crds (dict): a data structure to hold image pull secret CRDs so that they are not logged

    Returns:
        0
        1
        2
    """
    logger.info("Creating Client/Server Pods: cpu_partitioning=%s" % (cpu_partitioning))

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    engines = None
    if cpu_partitioning is None:
        logger.error("You must define cpu_partitioning when calling this function")
        return 1
    elif cpu_partitioning:
        engines = settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["with"]
    else:
        engines = settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["without"]

    if len(engines) > 0:
        logger.info("Going to create %d engines" % (len(engines)))
    else:
        logger.info("No engines to create")
        return 0

    for engine in engines:
        logger.info("Creating engine %s-%d" % (engine["role"], engine["id"]))

        engine["crd"], rc = create_pod_crd(engine["role"], engine["id"])
        if rc == 1:
            logger.error("Failed to create CRD for %s-%d" % (engine["role"], engine["id"]))
            if crd is None:
                logger.error("No CRD available for %s-%d" % (engine["role"], engine["id"]))
            else:
                logger.error("CRD generated for %s-%d is:\n%s" % (engine["role"], engine["id"], endpoints.dump_json(engine["crd"])))
        else:
            crd_json_str = endpoints.dump_json(engine["crd"])
            
            logger.info("Created CRD for %s-%d:\n%s" % (engine["role"], engine["id"], crd_json_str))

            crd_filename = settings["dirs"]["local"]["crds"]["pods"] + "/%s-%s.json" % (engine["role"], engine["id"])
            with open(crd_filename, "w", encoding = "ascii") as crd_fp:
                crd_fp.write(crd_json_str)
            logger.info("Wrote CRD for %s-%s to %s" % (engine["role"], engine["id"], crd_filename))

    if not "validation" in settings["engines"]["endpoint"]:
        settings["engines"]["endpoint"]["validation"] = {
            "valid": [],
            "invalid": []
        }

    if not "created" in settings["engines"]["endpoint"]:
        settings["engines"]["endpoint"]["created"] = {
            "succeeded": [],
            "failed": []
        }

    if not "verification" in settings["engines"]["endpoint"]:
        settings["engines"]["endpoint"]["verification"] = {
            "verified": [],
            "unverified": []
        }

    if not "hosting-nodes" in settings["engines"]["endpoint"]:
        settings["engines"]["endpoint"]["hosting-nodes"] = []

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"], validate = False) as con:
        if "pull-secrets" in settings["misc"] and len(settings["misc"]["pull-secrets"]):
            logger.info("Validating image pull secret CRDs")
            for secret in settings["misc"]["pull-secrets"].keys():
                if settings["misc"]["pull-secrets"][secret]["failed"] or settings["misc"]["pull-secrets"][secret]["validated"]:
                    continue

                logger.info("Validating image pull secret CRD for '%s'" % (secret))
                cmd = "%s create --filename - --dry-run=server --validate=strict" % (settings["misc"]["k8s-bin"])
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(image_pull_secret_crds[secret]), env = settings["misc"]["remote-env"])
                endpoints.log_result(result)
                if result.exited != 0:
                    logger.error("Did not validate image pull secret CRD for '%s'" % (secret))
                    settings["misc"]["pull-secrets"][secret]["failed"] = True
                else:
                    logger.info("Validated image pull secret CRD for '%s'" % (secret))
                    settings["misc"]["pull-secrets"][secret]["validated"] = True

        logger.info("Validating CRDs")
        invalid_crds = []
        valid_crds = []
        for engine in engines:
            engine_name = "%s-%d" % (engine["role"], engine["id"])
            logger.info("Validating CRD for '%s'" % (engine_name))
            cmd = "%s create --filename - --dry-run=server --validate=strict" % (settings["misc"]["k8s-bin"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(engine["crd"]), env = settings["misc"]["remote-env"])
            endpoints.log_result(result)
            if result.exited != 0:
                logger.error("Did not validate CRD for '%s'" % (engine_name))
                invalid_crds.append(engine_name)
            else:
                logger.info("Validated CRD for '%s'" % (engine_name))
                valid_crds.append(engine_name)
        settings["engines"]["endpoint"]["validation"]["valid"].extend(valid_crds)
        settings["engines"]["endpoint"]["validation"]["invalid"].extend(invalid_crds)
        if len(valid_crds) > 0:
            logger.info("Validated the CRDs for these %d engines: %s" % (len(valid_crds), valid_crds))
        if len(invalid_crds) > 0:
            logger.error("Failed to validate the CRDs for these %d engines: %s" % (len(invalid_crds), invalid_crds))
            return 1

        if "pull-secrets" in settings["misc"] and len(settings["misc"]["pull-secrets"]):
            logger.info("Creating image pull secret CRDs")
            for secret in settings["misc"]["pull-secrets"].keys():
                if settings["misc"]["pull-secrets"][secret]["failed"] or settings["misc"]["pull-secrets"][secret]["created"]:
                    continue

                logger.info("Creating image pull secret CRD for '%s'" % (secret))
                cmd = "%s create --filename -" % (settings["misc"]["k8s-bin"])
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(image_pull_secret_crds[secret]), env = settings["misc"]["remote-env"])
                endpoints.log_result(result)
                if result.exited != 0:
                    logger.error("Did not create image pull secret CRD for '%s'" % (secret))
                    settings["misc"]["pull-secrets"][secret]["failed"] = True
                else:
                    logger.info("Created image pull secret CRD for '%s'" % (secret))
                    settings["misc"]["pull-secrets"][secret]["created"] = True

        logger.info("Creating CRDs")
        failed_crds = []
        created_crds = []
        engine_details = dict()
        for engine in engines:
            engine_name = "%s-%d" % (engine["role"], engine["id"])
            logger.info("Creating CRD for '%s'" % (engine_name))
            cmd = "%s create --filename -" % (settings["misc"]["k8s-bin"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(engine["crd"]), env = settings["misc"]["remote-env"])
            endpoints.log_result(result)
            if result.exited != 0:
                logger.error("Did not create CRD for '%s'" % (engine_name))
                failed_crds.append(engine_name)
            else:
                logger.info("Created CRD for '%s'" % (engine_name))
                created_crds.append(engine_name)
                engine_details[engine_name] = {
                    "role": engine["role"],
                    "id": engine["id"]
                }
        settings["engines"]["endpoint"]["created"]["succeeded"].extend(created_crds)
        settings["engines"]["endpoint"]["created"]["failed"].extend(failed_crds)
        if len(created_crds) > 0:
            logger.info("Created the CRDs for these %d engines: %s" % (len(created_crds), created_crds))
        if len(failed_crds) > 0:
            logger.error("Failed to create the CRDs for these %d engines: %s" % (len(failed_crds), failed_crds))
            return 2

        if not "pods" in settings["engines"]["endpoint"]:
            settings["engines"]["endpoint"]["pods"] = dict()

        pod_status = verify_pods_running(con, created_crds, engine_details, abort_event)
        if pod_status is None:
            logger.error("Encountered fatal error while verifying pods")
            return 2
        logger.info("Reviewing pods")
        for pod in pod_status.keys():
            logger.info("Pod '%s' is running on node '%s'" % (engine, pod_status[pod]["node"]))

            if pod_status[pod]["node"] not in settings["engines"]["endpoint"]["hosting-nodes"]:
                logger.info("Adding node '%s' to the list of hosting nodes" % (pod_status[pod]["node"]))
                settings["engines"]["endpoint"]["hosting-nodes"].append(pod_status[pod]["node"])

            settings["engines"]["endpoint"]["verification"]["verified"].append(pod_status[pod]["name"])

            settings["engines"]["endpoint"]["pods"][pod] = copy.deepcopy(pod_status[pod])
        unverified = 0
        for pod in created_crds:
            if pod not in settings["engines"]["endpoint"]["verification"]["verified"]:
                settings["engines"]["endpoint"]["verification"]["unverified"].append(pod)
                logger.error("Pod '%s' has not been verified" % (pod))
                unverified += 1
        if unverified > 0:
            logger.error("Could not verify that all pods were running")
            return 2

        logger.info("There are %d hosting nodes: %s" % (len(settings["engines"]["endpoint"]["hosting-nodes"]), settings["engines"]["endpoint"]["hosting-nodes"]))
    
    return 0

def create_tools_pods(abort_event):
    """
    Create tools pods where appropriate based on the input parameters and the location of launched client/server pods

    Args:
        abort_event (threading.Event): a threading.Event that signals if the deployment should be aborted

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule them all
        image_pull_secret_crds (dict): a data structure to hold image pull secret CRDs so that they are not logged

    Returns:
        0
    """
    logger.info("Creating Tools Pods")

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    if endpoint["disable-tools"]["all"]:
        logger.info("Tools on all node types are disabled")
        return 0

    profiled_nodes = []

    if not endpoint["disable-tools"]["masters"]:
        logger.info("Profiling all master nodes: %s" % (settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]))
        for node in settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]:
            logger.info("Adding master node '%s' to the list of profiled nodes" % (node))
            profiled_nodes.append(node)
    else:
        logger.info("Profiling of master nodes is disabled")

    if not endpoint["disable-tools"]["workers"]:
        logger.info("Profiling active worker nodes")
        logger.info("Worker nodes: %s" % (settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]))
        logger.info("Active worker nodes: %s" % (settings["engines"]["endpoint"]["hosting-nodes"]))

        for node in settings["engines"]["endpoint"]["hosting-nodes"]:
            logger.info("Analyzing active worker node '%s'" % (node))
            if node in profiled_nodes:
                if not endpoint["disable-tools"]["masters"] and node in settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]:
                    logger.info("Already profiling worker node '%s' -- it is also a master node so that is probably why" % (node))
                else:
                    logger.info("Already profiling worker node '%s' and I'm not really sure why..." % (node))
            else:
                logger.info("Adding worker node '%s' to the list of profiled nodes because is hosting engine pods" % (node))
                settings["engines"]["endpoint"]["classes"]["profiled-nodes"].append(node)
                profiled_nodes.append(node)

        for node in settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]:
            if not node in profiled_nodes:
                logger.info("Not adding worker node '%s' to the list of profiled nodes because it is not hosting engine pods" % (node))
    else:
        logger.info("Profiling of worker nodes is disabled")

    logger.info("Going to launch profiler pods on these nodes: %s" % (profiled_nodes))

    if len(profiled_nodes) == 0:
        logger.info("No nodes to profile found")
        return 0

    logger.info("Loading tools information and creating profiler mapping")
    tools = []
    try:
        with open(settings["dirs"]["local"]["tool-cmds"] + "/profiler/start") as tool_cmd_file:
            for line in tool_cmd_file:
                split_line = line.split(":")
                tool = split_line[0]
                tools.append(tool)
                logger.info("Adding tool '%s' to the list of tools" % (tool))
    except IOError as e:
        logger.error("Failed to load the start tools command file")
        return 1
    for tool in tools:
        if not tool in settings["engines"]["profiler-mapping"]:
            settings["engines"]["profiler-mapping"][tool] = {
                "name": tool,
                "label": "%s-%s" % (args.endpoint_label, tool),
                "ids": []
            }
            logger.info("Created profiler mapping for tool '%s':\n%s" % (tool, endpoints.dump_json(settings["engines"]["profiler-mapping"][tool])))
        else:
            logger.info("Profiler mapping for tool '%s' already exists" % (tool))

    logger.info("Creating node profiling pods")

    settings["engines"]["endpoint"]["classes"]["profiled-nodes"] = []
    tools_pod_id = 1
    for node in profiled_nodes:
        pod = {
            "crd": None,
            "id": tools_pod_id,
            "node": node,
            "role": None
        }
        tools_pod_id += 1

        if pod["node"] in settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]:
            pod["role"] = "master"
        elif pod["node"] in settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]:
            pod["role"] = "worker"
        else:
            logger.error("Unknown role for tools pod to be run on node '%s'" % (pod["node"]))
            return 1

        logger.info("Creating pod '%s-%d' to run on node '%s'" % (pod["role"], pod["id"], pod["node"]))

        pod["crd"], rc = create_pod_crd(pod["role"], pod["id"], node = pod["node"])
        if rc == 1:
            logger.error("Failed to create CRD for '%s-%d'" % (pod["role"], pod["id"]))
            if pod["crd"] is None:
                logger.error("No CRD available for '%s-%d'" % (pod["role"], pod["id"]))
            else:
                logger.error("CRD generated for '%s-%d':\n%s" % (pod["role"], pod["id"], endpoints.dump_json(pod["crd"])))
        else:
            crd_json_str = endpoints.dump_json(pod["crd"])

            logger.info("Created CRD for '%s-%d':\n%s" % (pod["role"], pod["id"], crd_json_str))

            crd_filename = settings["dirs"]["local"]["crds"]["pods"] + "/%s-%s.json" % (pod["role"], pod["id"])
            with open(crd_filename, "w", encoding = "ascii") as crd_fp:
                crd_fp.write(crd_json_str)
            logger.info("Wrote CRD for %s-%s to %s" % (pod["role"], pod["id"], crd_filename))
        
        settings["engines"]["endpoint"]["classes"]["profiled-nodes"].append(pod)

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"], validate = False) as con:
        if "pull-secrets" in settings["misc"] and len(settings["misc"]["pull-secrets"]):
            logger.info("Validating image pull secret CRDs")
            for secret in settings["misc"]["pull-secrets"].keys():
                if settings["misc"]["pull-secrets"][secret]["failed"] or settings["misc"]["pull-secrets"][secret]["validated"]:
                    continue

                logger.info("Validating image pull secret CRD for '%s'" % (secret))
                cmd = "%s create --filename - --dry-run=server --validate=strict" % (settings["misc"]["k8s-bin"])
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(image_pull_secret_crds[secret]), env = settings["misc"]["remote-env"])
                endpoints.log_result(result)
                if result.exited != 0:
                    logger.error("Did not validate image pull secret CRD for '%s'" % (secret))
                    settings["misc"]["pull-secrets"][secret]["failed"] = True
                else:
                    logger.info("Validated image pull secret CRD for '%s'" % (secret))
                    settings["misc"]["pull-secrets"][secret]["validated"] = True

        logger.info("Validating CRDs")
        invalid_crds = []
        valid_crds = []
        for pod in settings["engines"]["endpoint"]["classes"]["profiled-nodes"]:
            pod_name = "%s-%d" % (pod["role"], pod["id"])
            logger.info("Validating CRD for pod '%s'" % (pod_name))
            cmd = "%s create --filename - --dry-run=server --validate=strict" % (settings["misc"]["k8s-bin"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(pod["crd"]), env = settings["misc"]["remote-env"])
            endpoints.log_result(result)
            if result.exited != 0:
                logger.error("Did not validate CRD for pod '%s'" % (pod_name))
                invalid_crds.append(pod_name)
            else:
                logger.info("Validated CRD for pod '%s'" % (pod_name))
                valid_crds.append(pod_name)
        settings["engines"]["endpoint"]["validation"]["valid"].extend(valid_crds)
        settings["engines"]["endpoint"]["validation"]["invalid"].extend(invalid_crds)
        if len(valid_crds) > 0:
            logger.info("Validated the CRDs for these %d pods: %s" % (len(valid_crds), valid_crds))
        if len(invalid_crds) > 0:
            logger.error("Failed to validate the CRDs for these %d pods: %s" % (len(invalid_crds), invalid_crds))
            return 1

        if "pull-secrets" in settings["misc"] and len(settings["misc"]["pull-secrets"]):
            logger.info("Creating image pull secret CRDs")
            for secret in settings["misc"]["pull-secrets"].keys():
                if settings["misc"]["pull-secrets"][secret]["failed"] or settings["misc"]["pull-secrets"][secret]["created"]:
                    continue

                logger.info("Creating image pull secret CRD for '%s'" % (secret))
                cmd = "%s create --filename -" % (settings["misc"]["k8s-bin"])
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(image_pull_secret_crds[secret]), env = settings["misc"]["remote-env"])
                endpoints.log_result(result)
                if result.exited != 0:
                    logger.error("Did not create image pull secret CRD for '%s'" % (secret))
                    settings["misc"]["pull-secrets"][secret]["failed"] = True
                else:
                    logger.info("Created image pull secret CRD for '%s'" % (secret))
                    settings["misc"]["pull-secrets"][secret]["created"] = True

        logger.info("Creating CRDs")
        failed_crds = []
        created_crds = []
        pod_details = dict()
        for pod in settings["engines"]["endpoint"]["classes"]["profiled-nodes"]:
            pod_name = "%s-%d" % (pod["role"], pod["id"])
            logger.info("Creating CRD for pod '%s'" % (pod_name))
            cmd = "%s create --filename -" % (settings["misc"]["k8s-bin"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(pod["crd"]), env = settings["misc"]["remote-env"])
            endpoints.log_result(result)
            if result.exited != 0:
                logger.error("Did not create CRD for pod '%s'" % (pod_name))
                failed_crds.append(engine_name)
            else:
                logger.info("Created CRD for pod '%s'" % (pod_name))
                created_crds.append(pod_name)
                pod_details[pod_name] = {
                    "role": pod["role"],
                    "id": pod["id"]
                }
        settings["engines"]["endpoint"]["created"]["succeeded"].extend(created_crds)
        settings["engines"]["endpoint"]["created"]["failed"].extend(failed_crds)
        if len(created_crds) > 0:
            logger.info("Created the CRDs for these %d pods: %s" % (len(created_crds), created_crds))
        if len(failed_crds) > 0:
            logger.error("Failed to create the CRDs for these %d pods: %s" % (len(failed_crds), failed_crds))
            return 2

        if not "pods" in settings["engines"]["endpoint"]:
            settings["engines"]["endpoint"]["pods"] = dict()

        pod_status = verify_pods_running(con, created_crds, pod_details, abort_event)
        if pod_status is None:
            logger.error("Enclountered fatal error while verifying pods")
            return 2
        logger.info("Reviewing pods")
        for pod in pod_status.keys():
            logger.info("Pod '%s' is running on node '%s'" % (pod, pod_status[pod]["node"]))

            logger.info("Adding the containers in pod '%s' to the new followers list: %s" % (pod_status[pod]["name"], pod_status[pod]["containers"]))
            settings["engines"]["new-followers"].extend(pod_status[pod]["containers"])

            settings["engines"]["endpoint"]["verification"]["verified"].append(pod_status[pod]["name"])

            settings["engines"]["endpoint"]["pods"][pod] = copy.deepcopy(pod_status[pod])
        unverified = 0
        for pod in created_crds:
            if pod not in settings["engines"]["endpoint"]["verification"]["verified"]:
                settings["engines"]["endpoint"]["verification"]["unverified"].append(pod)
                logger.error("Pod '%s' has not been verified" % (pod))
                unverified += 1
        if unverified > 0:
            logger.error("Could not verify that all pods were running")
            return 2
            
    return 0

def kube_cleanup():
    """
    Attempt to cleanup the K8S namespace by collecting logs from the pods and then deleting everything

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    logger.info("Running cleanup")

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    cleanup_error = "An error has been encountered during cleanup -> the namespace ('%s') will be left untouched for inspection" % (endpoint["namespace"]["name"])

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"]) as con:
        errors = False

        logger.info("Current K8S namespace '%s' status" % (endpoint["namespace"]["name"]))
        cmd = "%s get all --namespace %s --output wide" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        endpoints.log_result(result)
        if result.exited != 0:
            logger.error(cleanup_error)
            errors = True

        logger.info("Collecting engine logs")
        pods = list(settings["engines"]["endpoint"]["pods"].keys())
        pods.sort()
        for pod in pods:
            pod_name = settings["engines"]["endpoint"]["pods"][pod]["name"]
            node_name = settings["engines"]["endpoint"]["pods"][pod]["node"]
            logger.info("Processing pod '%s' on node '%s'" % (pod_name, node_name))
            for engine in settings["engines"]["endpoint"]["pods"][pod]["containers"]:
                logger.info("Collecting log for engine '%s'" % (engine))
                cmd = "%s logs %s-%s --namespace %s --container %s" % (settings["misc"]["k8s-bin"],
                                                                       endpoint_default_settings["prefix"]["pod"],
                                                                       pod,
                                                                       endpoint["namespace"]["name"],
                                                                       engine)
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
                if result.exited == 0:
                    log_file = "%s/%s.txt.xz" % (settings["dirs"]["local"]["engine-logs"], engine)
                    with lzma.open(log_file, "wt", encoding="ascii") as lfh:
                        lfh.write(result.stdout)
                    logger.info("Wrote log for engine '%s' in pod '%s' to '%s'" % (engine, pod, log_file))
                else:
                    logger.error("Failed to collect log for engine '%s' in pod '%s'" % (engine, pod))
                    endpoints.log_result(result)
                    if not errors:
                        logger.error(cleanup_error)
                        errors = True

        if not errors:
            if clean_k8s_namespace(con):
                logger.error(cleanup_error)
                errors = True
            else:
                logger.info("Deleting namepsace: %s" % (endpoint["namespace"]["name"]))
                cmd = "%s delete namespace %s" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
                endpoints.log_result(result)
                if result.exited != 0:
                    logger.error("Failed to delete namespace: %s" % (endpoint["namespace"]["name"]))
                    logger.error(cleanup_error)
                    errors = True
        else:
            logger.warning("Skipping namespace cleanup due to prior errors")

    return 0

def engine_init():
    """
    Construct messages to initialize the engines with metadata specific to them

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        env_vars_msg_file (str): A file containing all the messages to send to the engines
    """
    logger.info("Building messages to send engine-specific metadata to the engines")
    env_vars_msgs = []
    pods = list(settings["engines"]["endpoint"]["pods"].keys())
    pods.sort()
    for pod in pods:
        pod_name = settings["engines"]["endpoint"]["pods"][pod]["name"]
        node_name = settings["engines"]["endpoint"]["pods"][pod]["node"]
        logger.info("Processing pod '%s' on node '%s'" % (pod_name, node_name))
        for engine in settings["engines"]["endpoint"]["pods"][pod]["containers"]:
            logger.info("Processing engine '%s'" % (engine))

            userenv = None
            osruntime = None

            # engine is in one of two forms:
            #   1. <role>-<id> -- these are clients or servers
            #   2. profiler-<endpoint-label>-<tool>-<id>
            fields = engine.split("-")
            if fields[0] == "profiler":
                userenv = endpoints.get_profiler_userenv(settings, engine)
                osruntime = "pod"
            else:
                role = fields[0]
                id = int(fields[1])

                engine_settings = settings["run-file"]["endpoints"][args.endpoint_index]["engines"]["settings"][role][id]
                userenv = engine_settings["userenv"]
                osruntime = engine_settings["osruntime"]
            
            env_vars_payload = {
                "env-vars": {
                    "endpoint_label": args.endpoint_label,
                    "hosted_by": node_name,
                    "hypervisor_host": None,
                    "userenv": userenv,
                    "osruntime": osruntime
                }
            }

            env_vars_msgs.extend(endpoints.create_roadblock_msg("follower", engine, "user-object", env_vars_payload))
    
    env_vars_msg_file = settings["dirs"]["local"]["roadblock-msgs"] + "/env-vars.json"
    logger.info("Writing follower env-vars messages to %s" % (env_vars_msg_file))
    env_vars_msgs_json = endpoints.dump_json(env_vars_msgs)
    with open(env_vars_msg_file, "w", encoding = "ascii") as env_vars_msg_file_fp:
        env_vars_msg_file_fp.write(env_vars_msgs_json)
    logger.info("Contents of %s:\n%s" % (env_vars_msg_file, env_vars_msgs_json))

    return env_vars_msg_file

def collect_sysinfo():
    """
    Collect information about the K8S cluster/environment

    Args:
        None

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0
    """
    logger.info("Collecting sysinfo")

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"], validate = False) as con:
        cmd = "%s cluster-info" % (settings["misc"]["k8s-bin"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        if result.exited != 0:
            logger.error("Failed to collect basic cluster-info")
            endpoints.log_result(result)
        else:
            out_file = settings["dirs"]["local"]["sysinfo"] + "/cluster-info.txt.xz"
            with lzma.open(out_file, "wt", encoding="ascii") as ofh:
                ofh.write(result.stdout)
            logger.info("Wrote basic cluster-info to '%s'" % (out_file))

        cmd = "%s cluster-info dump" % (settings["misc"]["k8s-bin"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        if result.exited != 0:
            logger.error("Failed to collect cluster-info dump")
            endpoints.log_result(result)
        else:
            out_file = settings["dirs"]["local"]["sysinfo"] + "/cluster-info.dump.json.xz"
            with lzma.open(out_file, "wt", encoding="ascii") as ofh:
                ofh.write("STDOUT:\n")
                ofh.write(result.stdout)
                ofh.write("STDERR:\n")
                ofh.write(result.stderr)
            logger.info("Wrote cluster-info dump to '%s'" % (out_file))

        collect_must_gather = False
        if "sysinfo" in settings["run-file"]["endpoints"][args.endpoint_index]:
            if "collect-must-gather" in settings["run-file"]["endpoints"][args.endpoint_index]["sysinfo"]:
                if settings["run-file"]["endpoints"][args.endpoint_index]["sysinfo"]["collect-must-gather"]:
                    collect_must_gather = True
        if collect_must_gather:
            logger.info("Going to collect OpenShift must-gather as requested")

            result = endpoints.run_remote(con, "mktemp --directory", debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
            if result.exited == 0:
                remote_temp_directory = result.stdout.strip()
                logger.info("Created remote temporary directory '%s'" % (remote_temp_directory))

                logger.info("Running OpenShift must-gather and logging to remote directory '%s'" % (remote_temp_directory))
                cmd = "%s adm must-gather --dest-dir=%s" % (settings["misc"]["k8s-bin"], remote_temp_directory)
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
                out_file = settings["dirs"]["local"]["sysinfo"] + "/must-gather.txt.xz"
                logger.info("Logging OpenShift must-gather output to '%s'" % (out_file))
                with lzma.open(out_file, "wt", encoding="ascii") as ofh:
                    ofh.write("STDOUT:\n")
                    ofh.write(result.stdout)
                    ofh.write("STDERR:\n")
                    ofh.write(result.stderr)
                if result.exited != 0:
                    logger.error("OpenShift must-gather completed with errors")
                else:
                    logger.info("OpenShift must-gather completed without errors")

                result = endpoints.run_remote(con, "mktemp", debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
                if result.exited == 0:
                    remote_temp_file = result.stdout.strip()

                    logger.info("Creating remote archive '%s' of OpenShift must-gather data" % (remote_temp_file))
                    # choosing to use gzip compression here for what
                    # is perceived to be maximum compatibility with
                    # what is available on the remote side
                    cmd = "tar --create --gzip --directory %s --file %s ." % (remote_temp_directory, remote_temp_file)
                    result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
                    if result.exited != 0:
                        logger.error("Failed to create remote archive")
                        endpoints.log_result(result)
                    else:
                        logger.info("Created remote archive")

                        fd, local_temp_filename = tempfile.mkstemp(prefix = "kube_", suffix=".tar.gz")
                        logger.info("Transferring remote temp file '%s' to local temp file '%s'" % (remote_temp_file, local_temp_filename))
                        con.get(remote_temp_file, local_temp_filename)
                        logger.info("Transfer complete")

                        logger.info("Extracting must-gather data from temporary file '%s' to '%s'" % (local_temp_filename, settings["dirs"]["local"]["sysinfo"]))
                        cmd = "tar --extract --gzip --directory %s --file %s" % (settings["dirs"]["local"]["sysinfo"], local_temp_filename)
                        result = endpoints.run_local(cmd, debug = settings["misc"]["debug-output"])
                        if result.exited == 0:
                            logger.info("Extracted must-gather data from temporary file")
                        else:
                            logger.error("Failed to extract must-gather data from temporary file")
                            endpoints.log_result(result)

                        logger.info("Removing must-gather temporary file '%s'" % (local_temp_filename))
                        os.remove(local_temp_filename)

                    logger.info("Deleting remote temporary file '%s'" % (remote_temp_file))
                    cmd = "rm %s" % (remote_temp_file)
                    result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
                    if result.exited != 0:
                        logger.error("Failed to delete remote temporary file '%s'" % (remote_temp_file))
                        endpoints.log_result(result)
                    else:
                        logger.info("Deletion of remote temporary file '%s' succeeded" % (remote_temp_file))
                else:
                    logger.error("Failed to create a remote temporary file")
                    endpoints.log_result(result)

                logger.info("Delete remote temporary directory '%s'" % (remote_temp_directory))
                cmd = "rm --recursive %s" % (remote_temp_directory)
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
                if result.exited != 0:
                    logger.error("Failed to delete remote temporary directory '%s'" % (remote_temp_directory))
                    endpoints.log_result(result)
                else:
                    logger.info("Delete of remote temporary directory '%s' succeeded" % (remote_temp_directory))
            else:
                logger.error("Failed to create temporary directory to store must-gather output")
                endpoints.log_result(result)
        else:
            logger.info("OpenShift must-gather collection not requested")

    return 0

def deployment_roadblock_function(roadblock_id, follower_name, endpoint_deploy_timeout, roadblock_password, roadblock_messages_dir, abort_deployment_event):
    logger.info("This is the deployment roadblock thread.  My name is '%s'" % (follower_name))

    rc = endpoints.do_roadblock(roadblock_id = roadblock_id,
                                follower_id = follower_name,
                                label = "endpoint-deploy-begin",
                                timeout = endpoint_deploy_timeout,
                                redis_password = roadblock_password,
                                msgs_dir = roadblock_messages_dir,
                                connection_watchdog = settings["rickshaw"]["roadblock"]["connection-watchdog"])
    if rc == 0:
        logger.info("endpoint-deploy-begin roadblock succeeded")
    else:
        logger.error("endpoint-deploy-begin roadblock failed")

        logger.critical("Setting abort deployment event")
        abort_deployment_event.set()

    logger.info("Ending the deployment roadblock thread")

    return 0

def build_network_crd_obj(crd_type, crd):
    """
    Build a network service CRD

    Args:
        crd_type (str): the type of network service crd this is
        crd (dict): the actual crd to embed in the object

    Globals:
        None

    Returns:
        crd_obj (dict): a network crd object
    """
    crd_obj = {
        "type": crd_type,
        "crd": crd,
        "validated": False,
        "created": False,
        "deleted": False
    }

    return crd_obj

def build_service_crd(engine, ports):
    """
    Build a network service CRD

    Args:
        engine (str): the server engine name that this service is to be associated with
        ports (list): a list of ports to include in the CRD

    Globals:
        settings (dict): the one data structure to rule then all
        endpoint_default_settings (dict): default settings for this endpoint

    Returns:
        crd: a K8S service CRD
    """
    crd = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "%s-%s" % (endpoint_default_settings["prefix"]["pod"], engine),
            "namespace": settings["run-file"]["endpoints"][args.endpoint_index]["namespace"]["name"]
        },
        "spec": {
            "ports": []
        }
    }

    for port in ports:
        for protocol in [ "TCP", "UDP" ]:
            port_obj = {
                "name": "port-" + str(port) + "-" + protocol.lower(),
                "port": port,
                "protocol": protocol,
                "targetPort": port
            }
            crd["spec"]["ports"].append(port_obj)

    return crd

def build_service_endpoints_crd(engine, engine_ip, ports):
    """
    Build an endpoints CRD for a network service

    Args:
        engine (str): the server engine name that this service is to be associated with
        engine_ip (str): the server engine pod's IP address
        ports (list): a list of ports to include in the CRD

    Globals:
        settings (dict): the one data structure to rule then all
        endpoint_default_settings (dict): default settings for this endpoint

    Returns:
        crd: a K8S endpoints CRD
    """
    crd = {
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "%s-%s" % (endpoint_default_settings["prefix"]["pod"], engine),
            "namespace": settings["run-file"]["endpoints"][args.endpoint_index]["namespace"]["name"]
        },
        "subsets": [
            {
                "addresses": [
                    {
                        "ip": engine_ip
                    }
                ],
            "ports": []
            }
        ]
    }

    for port in ports:
        for protocol in [ "TCP", "UDP" ]:
            port_obj = {
                "name": "port-" + str(port) + "-" + protocol.lower(),
                "port": port,
                "protocol": protocol
            }
            crd["subsets"][0]["ports"].append(port_obj)

    return crd

def build_metallb_crd(engine, ports, pool_name):
    """
    Build a MetalLB service CRD

    Args:
        engine (str): the server engine name that this service is to be associated with
        ports (list): a list of ports to include in the CRD
        pool_name (str): the name of the MetalLB address pool to use

    Globals:
        settings (dict): the one data structure to rule then all
        endpoint_default_settings (dict): default settings for this endpoint

    Returns:
        crd: a K8S MetalLB service CRD
    """
    crd = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "%s-%s-metallb" % (endpoint_default_settings["prefix"]["pod"], engine),
            "namespace": settings["run-file"]["endpoints"][args.endpoint_index]["namespace"]["name"],
            "annotations": {
                "metallb.universe.tf/address-pool": pool_name
            }
        },
        "spec": {
            "selector": {
                "app": "%s-%s" % (endpoint_default_settings["prefix"]["pod"], engine),
            },
            "type": "LoadBalancer",
            "ports": []
        }
    }

    for port in ports:
        for protocol in [ "TCP", "UDP" ]:
            port_obj = {
                "name": "port-" + str(port) + "-" + protocol.lower(),
                "nodePort": port,
                "port": port,
                "protocol": protocol,
                "targetPort": port
            }
            crd["spec"]["ports"].append(port_obj)

    return crd


def build_nodeport_crd(engine, ports):
    """
    Build a nodeport CRD

    Args:
        engine (str): the server engine name that this service is to be associated with
        ports (list): a list of ports to include in the CRD

    Globals:
        settings (dict): the one data structure to rule then all
        endpoint_default_settings (dict): default settings for this endpoint

    Returns:
        crd: a K8S nodeport CRD
    """
    crd = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "%s-%s-nodeport" % (endpoint_default_settings["prefix"]["pod"], engine),
            "namespace": settings["run-file"]["endpoints"][args.endpoint_index]["namespace"]["name"]
        },
        "spec": {
            "type": "NodePort",
            "ports": []
        }
    }

    for port in ports:
        for protocol in [ "TCP", "UDP" ]:
            port_obj = {
                "name": "port-" + str(port) + "-" + protocol.lower(),
                "nodePort": port,
                "port": port,
                "protocol": protocol,
                "targetPort": port
            }
            crd["spec"]["ports"].append(port_obj)

    return crd

def build_nodeport_endpoints_crd(engine, engine_ip, ports):
    """
    Build an endpoints CRD for a network nodeport

    Args:
        engine (str): the server engine name that this service is to be associated with
        engine_ip (str): the server engine pod's IP address
        ports (list): a list of ports to include in the CRD

    Globals:
        settings (dict): the one data structure to rule then all
        endpoint_default_settings (dict): default settings for this endpoint

    Returns:
        crd: a K8S endpoints CRD
    """
    crd = {
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "%s-%s-nodeport" % (endpoint_default_settings["prefix"]["pod"], engine),
            "namespace": settings["run-file"]["endpoints"][args.endpoint_index]["namespace"]["name"]
        },
        "subsets": [
            {
                "addresses": [
                    {
                        "ip": engine_ip
                    }
                ],
            "ports": []
            }
        ]
    }

    for port in ports:
        for protocol in [ "TCP", "UDP" ]:
            port_obj = {
                "name": "port-" + str(port) + "-" + protocol.lower(),
                "port": port,
                "protocol": protocol
            }
            crd["subsets"][0]["ports"].append(port_obj)

    return crd

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

    This function runs right after a server starts any service and right before a client starts
    and tries to contect the server's service.  The purpose of this function is to do any
    work which ensures the client can contact the server.  In some cases there may be nothing
    to do.  Regardless of the work, the endpoint needs to relay what IP & ports the client
    needs to use in order to reach the server.  In some cases that may be the information the
    server has provided to the endpoint, or this information has changed because the endpoint
    created some sort of proxy to reach the server.

    In the case of the k8s endpoint, there are two possible actions, and this depends on where
    the client is in relation to the server.  If the client is within the same k8s cluster,
    we create a k8s-service, so the client can use an IP which is more persistent
    than a pod's IP (this allows pods to come and go while keeping the same IP).  This is not
    absolutely necessary for our  benchmarks, but it is a best practice for cloud-native
    aps, so we do it anyway.  If the client is not in the k8s cluster, then we must assume 
    it does not have direct access to the pod cluster network, and some form of 'ingress' must
    be set up.  Currently, this endpoint implements 'NodePort' and Loadbalancer svc. For NodePort.
    which provides a port for the service which can be accessed on any of the cluster's nodes,
    we provide the IP address of the node which happens to host the server pod. For LoadBalancer,
    the external IP is assigned dynamically from the LB AddressPool when the svc is created. 
    For baremetal, the MetalLB LoadBalancer setup is outside crucible. We just need the PoolName 
    in the k8s endpoint option lbSvc="PoolName".
    """
    logger.info("Running test_start() for '%s' (<iteration>-<sample>-<attempt>)" % (test_id))

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    send_messages = False

    if not "networking" in settings:
        settings["networking"] = {}

    settings["networking"][test_id] = {}
    settings["networking"][test_id]["ingress-lb"] = []
    settings["networking"][test_id]["other"] = []
    settings["networking"][test_id]["nodeport"] = []
    settings["networking"][test_id]["service"] = []

    this_msg_file = msgs_dir + "/" + test_id + ":server-start-end.json"
    path = Path(this_msg_file)

    if path.exists() and path.is_file():
        logger.info("Found '%s'" % (this_msg_file))

        msgs_json,err = load_json_file(this_msg_file)
        if not msgs_json is None:
            if "received" in msgs_json:
                logger.info("Checking received messages for service requests")
                for msg in msgs_json["received"]:
                    if msg["payload"]["message"]["command"] == "user-object":
                        if "svc" in msg["payload"]["message"]["user-object"] and "ports" in msg["payload"]["message"]["user-object"]["svc"]:
                            server_engine = msg["payload"]["sender"]["id"]
                            client_engine = re.sub(r"server", r"client", server_engine)

                            logger.info("Found a service message from server engine %s to client engine %s:\n%s" % (server_engine, client_engine, endpoints.dump_json(msg["payload"])))

                            if not server_engine in settings["engines"]["endpoint"]["pods"]:
                                logger.info("This server engine (%s) is not owned by this endpoint so it is being ignored" % (server_engine))
                                continue
                            else:
                                logger.info("This server engine (%s) is owned by this endpoint so it will be handled" % (server_engine))

                            obj = {
                                "server-engine": server_engine,
                                "client-engine": client_engine,
                                "test-ip": msg["payload"]["message"]["user-object"]["svc"]["ip"],
                                "pod-ip": settings["engines"]["endpoint"]["pods"][server_engine]["pod-ip"],
                                "service-ip": None,
                                "ports": msg["payload"]["message"]["user-object"]["svc"]["ports"],
                                "crds": [],
                                "validated": False,
                                "created": False,
                                "deleted": False
                            }

                            if obj["test-ip"] != obj["pod-ip"]:
                                logger.info("The test IP address (%s) and the pod IP address (%s) are not the same for server engine %s" % (obj["test-ip"], obj["pod-ip"], obj["server-engine"]))

                                logger.info("Since the two IP addresses do not match there is nothing for me to do -- assuming something like SRIOV+multus is being used")

                                settings["networking"][test_id]["other"].append(obj)
                            else:
                                logger.info("The test IP address (%s) and the pod IP address (%s) are the same for server engine %s" % (obj["test-ip"], obj["pod-ip"], obj["server-engine"]))

                                if client_engine in settings["engines"]["endpoint"]["pods"]:
                                    logger.info("Client %s is inside the cluster" % (obj["client-engine"]))

                                    # if the client is hosted in the cluster then a clusterIP service will
                                    # be created for the server and an endpoint will be created to ensure
                                    # the service forwards connections to the correct pod

                                    logger.info("Building service")

                                    crd = build_network_crd_obj("service",
                                                                build_service_crd(obj["server-engine"],
                                                                                  obj["ports"]))

                                    logger.info("Created service CRD:\n%s" % (endpoints.dump_json(crd)))
                                    obj["crds"].append(crd)

                                    # Instead of relying on k8s to make an association between the service and
                                    # the pod, we explicitly connect the two by creating an endpoint, linking
                                    # the service to the IP of the server pod

                                    logger.info("Building endpoints")

                                    crd = build_network_crd_obj("endpoints",
                                                                build_service_endpoints_crd(obj["server-engine"],
                                                                                            obj["pod-ip"],
                                                                                            obj["ports"]))

                                    logger.info("Created endpoints CRD:\n%s" % (endpoints.dump_json(crd)))
                                    obj["crds"].append(crd)

                                    settings["networking"][test_id]["service"].append(obj)
                                else:
                                    logger.info("Client %s is outside the cluster" % (client_engine))

                                    if "metallb-pool" in endpoint:
                                        logger.info("User has requested an ingress LoadBalancer service")

                                        logger.info("Building ingress-lb using MetalLB pool '%s'" % (endpoint["metallb-pool"]))

                                        crd = build_network_crd_obj("ingress-lb",
                                                                    build_metallb_crd(obj["server-engine"]),
                                                                                      obj["ports"],
                                                                                      endpoint["metallb-pool"])

                                        logger.info("Created ingress-lb CRD:\n%s" % (endpoints.dump_json(crd)))
                                        obj["crds"].append(crd)

                                        settings["networking"][test_id]["ingress-lb"].append(obj)
                                    else:
                                        logger.info("Creating an ingress NodePort service")

                                        logger.info("Building nodeport")

                                        crd = build_network_crd_obj("nodeport",
                                                                    build_nodeport_crd(obj["server-engine"],
                                                                                       obj["ports"]))

                                        logger.info("Created nodeport CRD:\n%s" % (endpoints.dump_json(crd)))
                                        obj["crds"].append(crd)

                                        logger.info("Building endpoints")

                                        crd = build_network_crd_obj("endpoints",
                                                                    build_nodeport_endpoints_crd(obj["server-engine"],
                                                                                                 obj["pod-ip"],
                                                                                                 obj["ports"]))

                                        logger.info("Created endpoints CRD:\n%s" % (endpoints.dump_json(crd)))
                                        obj["crds"].append(crd)

                                        settings["networking"][test_id]["nodeport"].append(obj)

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"], validate = False) as con:
        logger.info("Writing networking model CRDs to disk")
        for key in settings["networking"][test_id].keys():
            logger.info("Processing networking model: %s" % (key))

            logger.info("There are %d server engines to process for this networking mode (%s)" % (len(settings["networking"][test_id][key]), key))
            for obj in settings["networking"][test_id][key]:
                logger.info("Processing server engine %s" % (obj["server-engine"]))

                if len(obj["crds"]) == 0:
                    logger.info("There are no CRDs to process")
                    continue

                for crd in obj["crds"]:
                    logger.info("Processing %s CRD" % (crd["type"]))

                    crd_filename = settings["dirs"]["local"]["crds"]["networking"] + "/%s--%s--%s--%s.json" % (test_id, key, obj["server-engine"], crd["type"])
                    crd_json_str = endpoints.dump_json(crd["crd"])
                    with open(crd_filename, "w", encoding = "ascii") as crd_fp:
                        crd_fp.write(crd_json_str)
                    logger.info("Wrote %s CRD to %s" % (crd["type"], crd_filename))

        logger.info("Validating networking model CRDs")
        for key in settings["networking"][test_id].keys():
            logger.info("Processing networking model: %s" % (key))

            logger.info("There are %d server engines to process for this networking mode (%s)" % (len(settings["networking"][test_id][key]), key))
            for obj in settings["networking"][test_id][key]:
                logger.info("Processing server engine %s" % (obj["server-engine"]))

                if len(obj["crds"]) == 0:
                    logger.info("There are no CRDs to process")
                    continue

                for crd in obj["crds"]:
                    logger.info("Processing %s CRD" % (crd["type"]))

                    cmd = "%s create --filename - --dry-run=server --validate=strict" % (settings["misc"]["k8s-bin"])
                    result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(crd["crd"]), env = settings["misc"]["remote-env"])
                    endpoints.log_result(result)
                    if result.exited != 0:
                        logger.error("Failed to validate %s CRD" % (crd["type"]))
                    else:
                        logger.info("Validated %s CRD" % (crd["type"]))
                        crd["validated"] = True

        logger.info("Creating validated networking model CRDs")
        for key in settings["networking"][test_id].keys():
            logger.info("Processing networking model: %s" % (key))

            logger.info("There are %d server engines to process for this networking mode (%s)" % (len(settings["networking"][test_id][key]), key))
            for obj in settings["networking"][test_id][key]:
                logger.info("Processing server engine %s" % (obj["server-engine"]))

                if len(obj["crds"]) == 0:
                    logger.info("There are no CRDs to process")
                    continue

                obj["validated"] = True
                for crd in obj["crds"]:
                    if not crd["validated"]:
                        logger.error("CRD %s previously failed validation" % (crd["type"]))
                        obj["validated"] = False

                if not obj["validated"]:
                    logger.error("Skipping CRD creation for this server engine (%s) since one or more CRDs failed validation" % (obj["server-engine"]))
                else:
                    for crd in obj["crds"]:
                        logger.info("Processing %s CRD" % (crd["type"]))

                        cmd = "%s create --filename -" % (settings["misc"]["k8s-bin"])
                        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(crd["crd"]), env = settings["misc"]["remote-env"])
                        endpoints.log_result(result)
                        if result.exited != 0:
                            logger.error("Failed to create %s CRD" % (crd["type"]))
                        else:
                            logger.info("Created %s CRD" % (crd["type"]))
                            crd["created"] = True

                    obj["created"] = True
                    for crd in obj["crds"]:
                        if not crd["created"]:
                            logger.error("CRD %s previously failed creation" % (crd["type"]))
                            obj["created"] = False

                if not obj["created"]:
                    logger.error("CRD creation for this server engine (%s) failed since one or more CRDs were not created" % (obj["server-engine"]))

        logger.info("Collecting service IP address for networking models")
        for key in settings["networking"][test_id].keys():
            logger.info("Processing networking model: %s" % (key))

            logger.info("There are %d server engines to process for this networking mode (%s)" % (len(settings["networking"][test_id][key]), key))
            for obj in settings["networking"][test_id][key]:
                logger.info("Processing server engine %s" % (obj["server-engine"]))

                if key == "other":
                    logger.info("For the 'other' networking model an assumption is made that the service IP address is the test IP address (%s)" % (obj["test-ip"]))

                    obj["service-ip"] = obj["test-ip"]

                    logger.info("IP address is %s" % (obj["service-ip"]))

                    send_messages = True
                elif obj["created"]:
                    if key == "service":
                        logger.info("Getting IP address for a service")

                        cmd = "%s get svc/%s --namespace %s --output json" % (settings["misc"]["k8s-bin"], obj["crds"][0]["crd"]["metadata"]["name"], endpoint["namespace"]["name"])
                        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
                        endpoints.log_result(result)
                        if result.exited != 0:
                            logger.error("Failed to retrieve service information")
                        else:
                            logger.info("Retrieved service information")

                            service_obj = json.loads(result.stdout)

                            if "spec" in service_obj and "clusterIP" in service_obj["spec"]:
                                obj["service-ip"] = service_obj["spec"]["clusterIP"]

                                logger.info("IP address is %s" % (obj["service-ip"]))

                                send_messages = True
                            else:
                                logger.error("Failed to decode service information or service information is incomplete")
                    elif key == "ingress-lb":
                        logger.info("Getting IP address for a ingress-lb")

                        cmd = "%s get svc/%s --namespace %s --output json" % (settings["misc"]["k8s-bin"], obj["crds"][0]["crd"]["metadata"]["name"], endpoint["namespace"]["name"])
                        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
                        endpoints.log_result(result)
                        if result.exited != 0:
                            logger.error("Failed to retrieve service information")
                        else:
                            logger.info("Retrieved service information")

                            service_obj = json.loads(result.stdout)

                            if "status" in service_obj and "loadBalancer" in service_obj["status"] and "ingress" in service_obj["status"]["loadBalancer"] and "ip" in service_obj["status"]["loadBalancer"]["ingress"][0]:
                                obj["service-ip"] = service_obj["status"]["loadBalancer"]["ingress"][0]["ip"]

                                logger.info("IP address is %s" % (obj["service-ip"]))

                                send_messages = True
                            else:
                                logger.error("Failed to decode service information or service information is incomplete")
                    elif key == "nodeport":
                        logger.info("Getting IP address for a nodeport")

                        # a NodePort is available on -any- woker node in the cluster, however, we choose to "intelligently"
                        # provide the worker node's IP address which is currently hosting the pod

                        obj["service-ip"] = settings["engines"]["endpoint"]["pods"][obj["server-engine"]]["node-ip"]

                        logger.info("IP address is %s" % (obj["service-ip"]))

                        send_messages = True

        service_status(con)

    if send_messages:
        logger.info("Sending IP address messages to client engines via roadblock")

        for key in settings["networking"][test_id].keys():
            logger.info("Processing networking model: %s" % (key))

            logger.info("There are %d client engines to process for this networking mode (%s)" % (len(settings["networking"][test_id][key]), key))
            for obj in settings["networking"][test_id][key]:
                logger.info("Processing client engine %s" % (obj["client-engine"]))

                if obj["service-ip"] is not None:
                    logger.info("Creating a message to send to the client engine (%s) with the service IP address information" % (obj["client-engine"]))

                    user_object = {
                        "svc": {
                            "ip": obj["service-ip"],
                            "ports": obj["ports"]
                        }
                    }

                    msg = endpoints.create_roadblock_msg("follower", obj["client-engine"], "user-object", user_object)

                    msg_file = tx_msgs_dir + "/service-ip-" + obj["server-engine"] + ".json"
                    logger.info("Writing follower service-ip message to '%s'" % (msg_file))
                    with open(msg_file, "w", encoding = "ascii") as msg_file_fp:
                        msg_file_fp.write(endpoints.dump_json(msg))
                else:
                    logger.warning("A service IP address is not available for this client engine (%s) which likely indicates a prior error during process a message from it's matching server engine (%s)" % (obj["client-engine"], obj["server-engine"]))
    else:
        logger.info("No IP address messages to send to clients")

    logger.info("Returning from test_start() for '%s' (<iteration>-<sample>-<attempt>)" % (test_id))
    return

def test_stop(test_id):
    """
    Perform endpoint responsibilties that must be completed after an iteration test sample

    Args:
        test_id (str): A string of the form "<iteration>:<sample>:<attempt>" used to identify the current test

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    logger.info("Running test_stop() for '%s' (<iteration>-<sample>-<attempt>)" % (test_id))

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"], validate = False) as con:
        logger.info("Deleting services for networking models")
        for key in settings["networking"][test_id].keys():
            logger.info("Processing networking model: %s" % (key))

            logger.info("There are %d server engines to process for this networking mode (%s)" % (len(settings["networking"][test_id][key]), key))
            for obj in settings["networking"][test_id][key]:
                logger.info("Processing server engine %s" % (obj["server-engine"]))

                if len(obj["crds"]) == 0:
                    logger.info("There are no CRDs to process")
                    continue

                for crd in obj["crds"]:
                    logger.info("Processing %s CRD" % (crd["type"]))

                    if not crd["created"]:
                        logger.info("This CRD was not created so it does not need to be deleted")
                        continue

                    cmd = "%s delete --filename -" % (settings["misc"]["k8s-bin"])
                    result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = endpoints.dump_json(crd["crd"]), env = settings["misc"]["remote-env"])
                    endpoints.log_result(result)
                    if result.exited != 0:
                        logger.error("Failed to delete %s CRD" % (crd["type"]))
                    else:
                        logger.info("Deleted %s CRD" % (crd["type"]))
                        crd["deleted"] = True

        service_status(con)

    logger.info("Returning from test_stop() for '%s' (<iteration>-<sample>-<attempt>)" % (test_id))
    return

def service_status(connection):
    """
    Get the status of network services on the cluster

    Args:
         connection (Fabric Connection): The connection to use to run the commands remotely

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    logger.info("Cluster/Namespace status:")
    for subcmd in [ "get svc", "get endpoints" ]:
        cmd = "%s %s --namespace %s --output wide" % (settings["misc"]["k8s-bin"], subcmd, endpoint["namespace"]["name"])
        result = endpoints.run_remote(connection, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        endpoints.log_result(result)

        cmd = "%s %s --namespace %s --output json" % (settings["misc"]["k8s-bin"], subcmd, endpoint["namespace"]["name"])
        result = endpoints.run_remote(connection, cmd, debug = settings["misc"]["debug-output"], env = settings["misc"]["remote-env"])
        endpoints.log_result(result)

    return

def create_pull_secret_crd(name, token_file):
    """
    Create a pull secret CRD from a JSON token authorization file

    Args:
        name (str): the name to use for the pull secret
        token_file (str): the path to the token file to use for the secret

    Globals:
        logger: a logger instance

    Returns:
        crd (dict): the final CRD that was built
    """
    logger.info("Building a image pull secret CRD called '%s' for '%s'" % (name, token_file))

    crd = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": settings["run-file"]["endpoints"][args.endpoint_index]["namespace"]["name"]
        },
        "type": "kubernetes.io/dockerconfigjson",
        "data": {
            ".dockerconfigjson": None
        }
    }

    # the CRD is intentionally printed prior to adding the token so
    # that it does not end up in the logs for security reasons
    logger.info("Created image pull secret CRD:\n%s" % (endpoints.dump_json(crd)))

    token = ""
    with open(token_file, "r", encoding = "ascii") as fh:
        for line in fh:
            token += line

    crd["data"][".dockerconfigjson"] = endpoints.base64_encode(token)

    return crd

def init_kube_settings():
    """
    Initialize settings that are unique to the kube endpoint

    Args:
        None

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    logger.info("Initializing kube endpoint specific settings")

    settings["dirs"]["local"]["crds"] = dict()

    for crd_type in [ "pods", "networking" ]:
        settings["dirs"]["local"]["crds"][crd_type] = settings["dirs"]["local"]["endpoint"] + "/crds/" + crd_type

def create_kube_dirs():
    """
    Create the kube specific directories

    Args:
        None

    Globals:
        logger: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        None
    """
    logger.info("Creating kube endpoint specific directories")

    for crd_type in settings["dirs"]["local"]["crds"].keys():
        endpoints.my_make_dirs(settings["dirs"]["local"]["crds"][crd_type])

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

    if args.validate:
        return(validate())

    logger = endpoints.setup_logger(args.log_level)

    endpoints.log_cli(args)
    settings = endpoints.init_settings(settings, args)

    init_kube_settings()

    settings = endpoints.load_settings(settings,
                                       endpoint_name = "kube",
                                       run_file = args.run_file,
                                       rickshaw_dir = args.rickshaw_dir,
                                       endpoint_index = args.endpoint_index,
                                       endpoint_normalizer_callback = normalize_endpoint_settings)
    if settings is None:
        logger.error("Enabling early abort due to error in endpoints.load_settings")
        early_abort = True

    if not early_abort:
        if check_base_requirements() != 0:
            logger.error("Enabling early abort due to error in check_base_requirements")
            early_abort = True
    else:
        logger.warning("Skipping call to check_base_requirements due to early abort")

    if not early_abort:
        endpoints.create_local_dirs(settings)
        create_kube_dirs()
    else:
        logger.warning("Skipping call to endpoints.create_local_dirs due to early abort")

    if not early_abort:
        if get_k8s_config() != 0:
            logger.error("Enabling early abort due to error in get_k8s_config")
            early_abort = True
    else:
        logger.warning("Skipping call to get_k8s_config due to early abort")

    deployment_label = args.endpoint_label + "-deploy"
    rc = endpoints.process_pre_deploy_roadblock(roadblock_id = args.roadblock_id,
                                                endpoint_label = args.endpoint_label,
                                                roadblock_password = args.roadblock_passwd,
                                                deployment_followers = [ deployment_label ],
                                                roadblock_messages_dir = settings["dirs"]["local"]["roadblock-msgs"],
                                                roadblock_timeouts = settings["rickshaw"]["roadblock"]["timeouts"],
                                                early_abort = early_abort,
                                                roadblock_connection_watchdog = settings["rickshaw"]["roadblock"]["connection-watchdog"])
    if rc != 0:
        logger.error("Processing of the pre-deploy roadblocks resulted in an error")
    else:
        abort_deployment_event = threading.Event()

        try:
            logger.info("Create roadblock deployment thread with name '%s'" % (deployment_label))
            deployment_roadblock_thread = threading.Thread(target = deployment_roadblock_function,
                                                           args = (
                                                                   args.roadblock_id,
                                                                   deployment_label,
                                                                   args.endpoint_deploy_timeout,
                                                                   args.roadblock_passwd,
                                                                   settings["dirs"]["local"]["roadblock-msgs"],
                                                                   abort_deployment_event
                                                                  ),
                                                           name = deployment_label)
            deployment_roadblock_thread.start()
        except RuntimeError as e:
            logger.error("Failed to create and start the deployment roadblock thread due to exception '%s'" % (str(e)))
            early_abort = True

        if not early_abort and not abort_deployment_event.is_set():
            if init_k8s_namespace() != 0:
                logger.error("Enabling early abort due to error in init_k8s_namespace")
                early_abort = True
        else:
            logger.warning("Skipping call to init_k8s_namespace due to early abort")

        if not early_abort and not abort_deployment_event.is_set():
            if compile_object_configs() != 0:
                logger.error("Enabling early abort due to error in compile_object_configs")
                early_abort = True
        else:
            logger.warning("Skipping call to compile_object_configs due to early abort")

        if not early_abort and not abort_deployment_event.is_set():
            if create_cs_pods(cpu_partitioning = True, abort_event = abort_deployment_event) != 0:
                logger.error("Enabling early abort due to error in create_cs_pods(cpu_partitioning = True)")
                early_abort = True
        else:
            logger.warning("Skipping call to create_cs_pods(cpu_partitioning = True) due to early abort")

        if not early_abort and not abort_deployment_event.is_set():
            if create_cs_pods(cpu_partitioning = False, abort_event = abort_deployment_event) != 0:
                logger.error("Enabling early abort due to error in create_cs_pods(cpu_partitioning = False)")
                early_abort = True
        else:
            logger.warning("Skipping call to create_cs_pods(cpu_partitioning = False) due to early abort")

        if not early_abort and not abort_deployment_event.is_set():
            if create_tools_pods(abort_deployment_event) != 0:
                logger.error("Enabling early abort due to error in create_tools_pods")
                early_abort = True
        else:
            logger.warning("Skipping call to create_tools_pods due to early abort")

        if not abort_deployment_event.is_set():
            # KMR implement callbacks
            kube_callbacks = {
                "engine-init": engine_init,
                "collect-sysinfo": collect_sysinfo,
                "test-start": test_start,
                "test-stop": test_stop,
                "remote-cleanup": kube_cleanup
            }
            if early_abort and not "new-followers" in settings["engines"]:
                # in the case of an early abort the new-followers list may not
                # have been initialized yet
                settings["engines"]["new-followers"] = []
            rc = endpoints.process_roadblocks(callbacks = kube_callbacks,
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
                                              early_abort = early_abort,
                                              roadblock_connection_watchdog = settings["rickshaw"]["roadblock"]["connection-watchdog"])
        else:
            logger.warning("Skipping call to process_raodblocks due to abort deployment")

        logger.info("Joining deployment roadblock thread")
        deployment_roadblock_thread.join()
        logger.info("Joined deployment roadblock thread")

    logger.info("Logging 'final' settings data structure")
    endpoints.log_settings(settings, mode = "settings")
    logger.info("kube endpoint exiting")
    return rc

if __name__ == "__main__":
    args = endpoints.process_options()
    logger = None
    settings = dict()
    exit(main())
