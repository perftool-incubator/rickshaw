#!/usr/bin/python3

"""
Endpoint to connect to a k8s cluster (1 or more nodes)
"""

import copy
from fabric import Connection
import jsonschema
import logging
import os
from paramiko import ssh_exception
from pathlib import Path
import re
import sys
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

endpoint_defaults = {
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
    endpoint["engines"]["defaults"] = {
        "controller-ip-address": endpoint_defaults["controller-ip-address"],
        "cpu-partitioning": endpoint_defaults["cpu-partitioning"],
        "osruntime": endpoint_defaults["osruntime"],
        "userenv": rickshaw["userenvs"]["default"]["benchmarks"]
    }

    if not "disable-tools" in endpoint:
        endpoint["disable-tools"] = copy.deepcopy(endpoint_defaults["disable-tools"])
    else:
        for key in endpoint_defaults["disable-tools"].keys():
            if not key in endpoint["disable-tools"]:
                endpoint["disable-tools"][key] = endpoint_defaults["disable-tools"][key]

    if not "namespace" in endpoint:
        # the user didn't request any specific namespace settings so
        # configure the default
        endpoint["namespace"] = {
            "type": "crucible"
        }
    if endpoint["namespace"]["type"] == "unique":
        prefix = endpoint_defaults["prefix"]["namespace"]
        if "prefix" in endpoint["namespace"]:
            prefix = endpoint["namespace"]["prefix"]
        endpoint["namespace"]["name"] = "%s__%s" % (prefix, args.run_id)
    elif endpoint["namespace"]["type"] == "crucible":
        endpoint["namespace"]["name"] = endpoint_defaults["prefix"]["namespace"]
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
                        log.error(msg)
                    return None
                found_defaults = True
                default_cfg_block_idx = cfg_block_idx
                for key in cfg_block["settings"].keys():
                    endpoint["engines"]["defaults"][key] = cfg_block["settings"][key]
    if not default_cfg_block_idx is None:
        del endpoint["config"][default_cfg_block_idx]

    if endpoint["engines"]["defaults"]["controller-ip-address"] is None:
        try:
            endpoint["engines"]["defaults"]["controller-ip-address"] = endpoints.get_controller_ip(endpoint["host"])
        except ValueError as e:
            msg = "While determining default controller IP address encountered exception '%s'" % (str(e))
            if args.validate:
                endpoints.validate_error(msg)
            else:
                log.error(msg)
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
                    log.error(msg)
                return None

    for cfg_block_idx,cfg_block in enumerate(endpoint["config"]):
        for target in cfg_block["targets"]:
            try:
                target["ids"] = endpoints.expand_ids(target["ids"])
            except ValueError as e:
                msg = "While expanding IDs for '%s' in config block at index %d encounterd exception '%s'" % (target["role"], cfg_block_idx, str(e))
                if args.validate:
                    endpoints.validate_error(msg)
                else:
                    log.error(msg)
                return None

    endpoint["engines"]["settings"] = dict()
    for cfg_block_idx,cfg_block in enumerate(endpoint["config"]):
        for target in cfg_block["targets"]:
            if not target["role"] in endpoint["engines"]:
                msg = "Found engine role '%s' in config block at index %d but not in the endpoint engines config" % (target["role"], cfg_block_idx)
                if args.validate:
                    endpoints.validate_error(msg)
                else:
                    log.error(msg)
                return None

            if not target["role"] in endpoint["engines"]["settings"]:
                endpoint["engines"]["settings"][target["role"]] = dict()

            for engine_id in target["ids"]:
                if not engine_id in endpoint["engines"][target["role"]]:
                    msg = "Found engine with ID %d and role '%s' in config block at index %d that is not owned by this endpoint" % (engine_id, target["role"], cfg_block_idx)
                    if args.validate:
                        endpoints.validate_error(msg)
                    else:
                        log.error(msg)
                    return None

                if engine_id not in endpoint["engines"]["settings"][target["role"]]:
                    endpoint["engines"]["settings"][target["role"]][engine_id] = dict()

                for key in cfg_block["settings"].keys():
                    if key in endpoint["engines"]["settings"][target["role"]][engine_id]:
                        msg = "Overriding previously defined value for key '%s' for engine ID %d with role '%s' while processing config block at index %d" % (key, engine_id, target["role"], cfg_block_idx)
                        if args.validate:
                            endpoints.validate_warning(msg)
                        else:
                            log.warning(msg)
                    endpoint["engines"]["settings"][target["role"]][engine_id][key] = cfg_block["settings"][key]

                for key in endpoint["engines"]["defaults"].keys():
                    if not key in endpoint["engines"]["settings"][target["role"]][engine_id]:
                        endpoint["engines"]["settings"][target["role"]][engine_id][key] = endpoint["engines"]["defaults"][key]

    for engine_role in [ "client", "server" ]:
        if engine_role in endpoint["engines"]:
            for engine_id in endpoint["engines"][engine_role]:
                if engine_id not in endpoint["engines"]["settings"][engine_role]:
                    endpoint["engines"]["settings"][engine_role][engine_id] = endpoint["engines"]["defaults"]

    return endpoint

def find_k8s_bin(validate, connection):
    """
    Figure out what the K8S control binary is for this environment

    Args:
        validate (bool): Is the caller the validate function
        connection (Fabric): The Fabric connection to use to run commands

    Globals:
        args (namespace): the script's CLI parameters

    Returns"
        None: No control binary could be identified
        k8s_bin (str): The name of the K8S control binary
    """
    debug_output = False
    if args.log_level == "debug":
        debug_output = True

    result = endpoints.run_remote(connection, "oc", validate = validate, debug = debug_output)
    if result.exited == 0:
        return "oc"

    result = endpoint.run_remote(connection, "kubectl", validate = validate, debug = debug_output)
    if result.exited == 0:
        return "kubectl"

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

            k8s_bin = find_k8s_bin(True, con)
            if k8s_bin is None:
                endpoints.validate_error("Failed to determine the k8s control binary")
            else:
                endpoints.validate_comment("determined k8s control binary is '%s'" % (k8s_bin))

                result = endpoints.run_remote(con, k8s_bin + " version", validate = True, debug = debug_output)
                result.stdout = re.sub(r'\n', ' | ', result.stdout)
                endpoints.validate_comment("remote '%s' presence check for %s: rc=%d stdout=[%s] and stderr=[%s]" % (k8s_bin, endpoint_settings["host"], result.exited, result.stdout.rstrip(' | '), result.stderr.rstrip('\n')))
                if result.exited != 0:
                    endpoints.validate_error("Failed to run '%s version' on endpoint host '%s' as user '%s'" % (k8s_bin, endpoint_settings["host"], endpoint_settings["user"]))

                result = endpoints.run_remote(con, k8s_bin + " get nodes", validate = True, debug = debug_output)
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

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"], validate = False) as con:
        settings["misc"]["k8s-bin"] = find_k8s_bin(False, con)
        if settings["misc"]["k8s-bin"] is None:
            return 1
        else:
            log.info("determined k8s control binary is '%s'" % (settings["misc"]["k8s-bin"]))

    return 0

def clean_k8s_namespace(connection):
    """
    Clean the namespace by deleting all components inside it

    Args:
        connection (Fabric): The Fabric connection to use to run commands

    Globals:
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0: success
        1: failure
    """
    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    log.info("Cleaning namespace '%s'" % (endpoint["namespace"]["name"]))

    component_errors = False
    for component in [ "pods", "services" ]:
        log.info("Cleaning component: %s" % (component))
        cmd = "%s delete --namespace %s %s --all" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"], component)
        result = endpoints.run_remote(connection, cmd, debug = settings["misc"]["debug-output"])
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
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0: success
        1: failure
    """
    log.info("Initializing K8S Namespace")

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"]) as con:
        namespace_exists = False
        cmd = "%s get namespace %s" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"])
        endpoints.log_result(result, level = "info")
        if result.exited == 0:
            namespace_exists = True

        if namespace_exists:
            log.info("Found existing namespace '%s', going to clean it" % (endpoint["namespace"]["name"]))
            if clean_k8s_namespace(con):
                return 1
        else:
            log.info("No namespace '%s' found, creating it" % (endpoint["namespace"]["name"]))
            cmd = "%s create namespace %s" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"])
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
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0: success
        1: failure
    """
    log.info("Getting K8S config")

    settings["misc"]["k8s"] = dict()
    settings["misc"]["k8s"]["nodes"] = dict()

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"]) as con:
        cmd = "%s get nodes" % (settings["misc"]["k8s-bin"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"])
        endpoints.log_result(result)
        if result.exited != 0:
            return 1

        cmd = "%s get nodes --output json" % (settings["misc"]["k8s-bin"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"])
        endpoints.log_result(result)
        if result.exited == 0:
            settings["misc"]["k8s"]["nodes"]["cluster"] = json.loads(result.stdout)
            settings["misc"]["k8s"]["nodes"]["endpoint"] = dict()
        else:
            return 1

        nr_nodes = len(settings["misc"]["k8s"]["nodes"]["cluster"]["items"])
        if nr_nodes > 0:
            log.info("Discovered %d nodes" % (nr_nodes))
        else:
            log.error("Did not find any nodes")
            return 1

        settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"] = []
        settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"] = []
        for node in settings["misc"]["k8s"]["nodes"]["cluster"]["items"]:
            name = node["metadata"]["name"]
            if "node-role.kubernetes.io/worker" in node["metadata"]["labels"]:
                settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"].append(name)
            if "node-role.kubernetes.io/master" in node["metadata"]["labels"]:
                settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"].append(name)
        node_count_fault = False
        log.info("Found %d masters: %s" % (len(settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]), settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]))
        if len(settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]) == 0:
            log.error("No masters found")
            node_count_fault = True
        log.info("Found %d workers: %s" % (len(settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]), settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]))
        if len(settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]) == 0:
            log.error("No workers found")
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
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0
    """
    log.info("Compiling object configs")

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

        for csid in endpoint["engines"][role]:
            settings["engines"]["endpoint"]["roles"][role].append(csid)

    log.info("This endpoint will run these clients: %s" % (list(map(lambda x: "client-" + str(x), settings["engines"]["endpoint"]["roles"]["client"]))))
    log.info("This endpoint will run these servers: %s" % (list(map(lambda x: "server-" + str(x), settings["engines"]["endpoint"]["roles"]["server"]))))

    settings["engines"]["endpoint"]["classes"] = dict()

    settings["engines"]["endpoint"]["classes"]["cpu-partitioning"] = dict()
    settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["with"] = []
    settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["without"] = []
    for role in roles:
        csids = list(endpoint["engines"]["settings"][role].keys())
        csids.sort()
        for csid in csids:
            engine = {
                "role": role,
                "id": int(csid)
            }

            if not "first-engine" in settings["engines"]["endpoint"]:
                settings["engines"]["endpoint"]["first-engine"] = copy.deepcopy(engine)

            if endpoint["engines"]["settings"][role][csid]["cpu-partitioning"]:
                settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["with"].append(engine)
            else:
                settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["without"].append(engine)

    endpoints.log_settings(settings, mode = "engines")

    return 0

def create_pod_crd(role = None, id = None, node = None):
    """

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        crd, 0: success
        crd, 1: failure
    """
    log.info("Creating CRD for engine %s-%d" % (role, id))

    if role is None or id is None:
        log.error("You must define role and id when calling this function")
        return None, 1

    if role == "master" or role == "worker":
        if node is None:
            log.error("You must define node when role is either 'master' or 'worker'")
            return None, 1

    name = "%s-%d" % (role, id)
    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    pod_settings = None
    if role == "client" or role == "server":
        pod_settings = endpoint["engines"]["settings"][role][id]
    elif role == "worker" or role == "master":
        pod_settings = endpoint["engines"]["defaults"]
    if pod_settings is None:
        log.error("Could not find mapping for pod settings")
        return None,1

    crd = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "%s-%s" % (endpoint_defaults["prefix"]["pod"], name),
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

    # handle annotations here

    if role == "master":
        # ensure master pod can be scheduled on the master node in case
        # it is not scheduable
        crd["spec"]["tolerations"] = [
            {
                "key": "node-role.kubernetes.io/master",
                "effect": "NoSchedule"
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
        crd["spec"]["volumes"] = [
            {
                "name": "hostfs-run",
                "hostPath": {
                    "path": "/var/run",
                    "type": "Directory"
                }
            },
            {
                "name": "hostfs-firmware",
                "hostPath": {
                    "path": "/lib/firmware",
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

    if role == "client" or role == "server":
        # KMR handle PsecurityContext

        # KMR handle runtimeClassName

        # KMR handle nodeSelector

        # KMR handle hostNetwork

        user_volumes = False
        if user_volumes:
            # KMR handle user volumes
            pass
        else:
            crd["spec"]["volumes"] = [
                {
                    "name": "hostfs-firmware",
                    "hostPath": {
                        "path": "/lib/firmware",
                        "type": "Directory"
                    }
                }
            ]

            # KMR fixup hugepages
            hugepages = False
            if hugepages:
                crd["spec"]["volumes"].append({
                    "name": "hugepage",
                    "emptyDir": {
                        "medium": "HugePages"
                    }
                })

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
        log.info("Adding container '%s' to pod" % (container_name))
        image = None
        if role == "client" or role == "server":
            image = endpoints.get_engine_id_image(settings, role, id, pod_settings["userenv"])
        elif role == "worker" or role == "master":
            userenv = endpoints.get_profiler_userenv(settings, container_name)
            if userenv is None:
                log.error("Could not find userenv for engine '%s'" % (container_name))
                return crd, 1
            else:
                log.info("Found userenv '%s' for engine '%s'" % (userenv, container_name))
            image = endpoints.get_engine_id_image(settings, role, container_name, userenv)
        if image is None:
            log.error("Could not find image for container %s" % (container_name))
            return crd, 1
        else:
            log.info("Found image '%s' for container '%s'" % (image, container_name))

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

        if role == "worker" or role == "master":
            container["securityContext"] = {
                "privileged": True
            }

            container["volumeMounts"] = [
                {
                    "mountPath": "/var/run",
                    "name": "hostfs-run"
                },
                {
                    "mountPath": "/lib/firmware",
                    "name": "hostfs-firmware"
                },
                {
                    "mountPath": "/lib/modules",
                    "name": "hostfs-modules"
                }
            ]

        if role == "client" or role == "server":
            # KMR handle resources

            if "securityContext" in pod_settings:
                container["securityContext"] = copy.deepcopy(pod_settings["securityContext"])

            user_volumes = False
            if user_volumes:
                # KMR handle user volumes
                pass
            else:
                container["volumeMounts"] = [
                    {
                        "mountPath": "/lib/firmware",
                        "name": "hostfs-firmware"
                    }
                ]

                # KMR fixup huagepages
                hugepages = False
                if hugepages:
                    container["volumeMounts"].append({
                        "mountPath": "/dev/hugepages",
                        "name": "hugepage"
                    })

        crd["spec"]["containers"].append(container)
        
    
    return crd, 0

def verify_pods_running(connection, pods):
    """
    Take the list of pods and verify that they are running and collect information about them

    Args:
       connection (Fabric): 
       engines (list): 

    Globals:
       log
       settings
       args

    Returns:
       engines_info (dict): 
       None: error
    """
    log.info("Verifying that these pods are running: %s" % (pods))
    pods_info = dict()
    verified_pods = []
    unverified_pods = []
    unverified_pods.extend(pods)

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    count = 1
    while len(unverified_pods) > 0:
        log.info("Loop pass %d" % (count))
        log.info("Unverified pods: %s" % (unverified_pods))
        log.info("Verified pods:   %s" % (verified_pods))
        count += 1

        log.info("Collecting pod status for namespace '%s'" % (endpoint["namespace"]["name"]))
        cmd = "%s get pods --namespace %s --output json" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
        result = endpoints.run_remote(connection, cmd, debug = settings["misc"]["debug-output"])
        endpoints.log_result(result)
        if result.exited != 0:
            return None

        pod_status = json.loads(result.stdout)

        log.info("Collected pod status for %d pods in namespace '%s'" % (len(pod_status["items"]), endpoint["namespace"]["name"]))

        log.info("Analyzing pod status for namespace '%s'" % (endpoint["namespace"]["name"]))
        for pod in pod_status["items"]:
            pod_name = pod["metadata"]["name"]
            # strip off the prefix
            pod_name = re.sub(r"%s-" % (endpoint_defaults["prefix"]["pod"]), r"", pod_name)
            log.debug("Processing engine '%s'" % (pod_name))

            if pod_name not in pods:
                log.info("Encountered pod '%s' that is not in my current verification list, skipping" % (pod_name))
                continue

            if pod_name in unverified_pods:
                log.info("Checking status of pod '%s'" % (pod_name))
                running_containers = []
                for container in pod["status"]["containerStatuses"]:
                    log.info("Checking status of container '%s:\n%s" % (container["name"], endpoints.dump_json(container["state"])))
                    if "running" in container["state"]:
                        log.info("Container '%s' is running" % (container["name"]))
                        running_containers.append(container["name"])
                    else:
                        log.info("Container '%s' is not running" % (container["name"]))
                        # KMR log what the state is

                if len(running_containers) == len(pod["status"]["containerStatuses"]):
                    log.info("All containers in pod '%s' are running -> it is verified" % (pod_name))
                    unverified_pods.remove(pod_name)
                    verified_pods.append(pod_name)
                    pods_info[pod_name] = {
                        "name": pod_name,
                        "node": pod["spec"]["nodeName"],
                        "containers": copy.deepcopy(running_containers)
                    }
                else:
                    log.info("There are %d containers in pod '%s' that are not yet running -> it is not verified" % ((len(pod["status"]["containerStatuses"]) - len(running_containers)), pod_name))

            elif pod_name in verified_pods:
                log.info("Skipping pod '%s' because it is already verified" % (pod_name))
            else:
                log.warning("Pod '%s' is untracked" % (pod_name))

        if len(unverified_pods) > 0:
            sleep_time = 10
            log.info("There are %d unverified pods, sleeping for %d seconds before checking again" % (len(unverified_pods), sleep_time))
            time.sleep(sleep_time)

    if len(verified_pods) != len(pods):
        log.error("Unable to verify all pods")
        log.error("all        - %d: %s" % (pods))
        log.error("unverified - %d: %s" % (len(unverified_pods), unverified_pods))
        log.error("verified   - %d: %s" % (len(verified_pods), verified_pods))
    else:
        log.info("Verified all %d pods: %s" % (len(pods), pods))

    log.info("Returning pod info:\n%s" % (endpoints.dump_json(pods_info)))

    return pods_info

def create_cs_pods(cpu_partitioning = None):
    """
    Generate Validate, and Create Pods for the client/server engines with the given CPU partitioning configuration

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0
        1
        2
    """
    log.info("Creating Client/Server Pods: cpu_partitioning=%s" % (cpu_partitioning))

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    engines = None
    if cpu_partitioning is None:
        log.error("You must define cpu_partitioning when calling this function")
        return 1
    elif cpu_partitioning:
        engines = settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["with"]
    else:
        engines = settings["engines"]["endpoint"]["classes"]["cpu-partitioning"]["without"]

    if len(engines) > 0:
        log.info("Going to create %d engines" % (len(engines)))
    else:
        log.info("No engines to create")
        return 0

    for engine in engines:
        log.info("Creating engine %s-%d" % (engine["role"], engine["id"]))

        engine["crd"], rc = create_pod_crd(engine["role"], engine["id"])
        if rc == 1:
            log.error("Failed to create CRD for %s-%d" % (engine["role"], engine["id"]))
            if crd is None:
                log.error("No CRD available for %s-%d" % (engine["role"], engine["id"]))
            else:
                log.error("CRD generated for %s-%d is:\n%s" % (engine["role"], engine["id"], endpoints.dump_json(engine["crd"])))
        else:
            log.info("Created CRD for %s-%d:\n%s" % (engine["role"], engine["id"], endpoints.dump_json(engine["crd"])))

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
        log.info("Validating CRDs")
        invalid_crds = []
        valid_crds = []
        for engine in engines:
            engine_name = "%s-%d" % (engine["role"], engine["id"])
            log.info("Validating CRD for '%s'" % (engine_name))
            cmd = "%s create --filename - --dry-run=server --validate=strict" % (settings["misc"]["k8s-bin"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = json.dumps(engine["crd"]))
            endpoints.log_result(result)
            if result.exited != 0:
                log.error("Did not validate CRD for '%s'" % (engine_name))
                invalid_crds.append(engine_name)
            else:
                log.info("Validated CRD for '%s'" % (engine_name))
                valid_crds.append(engine_name)
        settings["engines"]["endpoint"]["validation"]["valid"].extend(valid_crds)
        settings["engines"]["endpoint"]["validation"]["invalid"].extend(invalid_crds)
        if len(valid_crds) > 0:
            log.info("Validated the CRDs for these %d engines: %s" % (len(valid_crds), valid_crds))
        if len(invalid_crds) > 0:
            log.error("Failed to validate the CRDs for these %d engines: %s" % (len(invalid_crds), invalid_crds))
            return 1

        log.info("Creating CRDs")
        failed_crds = []
        created_crds = []
        for engine in engines:
            engine_name = "%s-%d" % (engine["role"], engine["id"])
            log.info("Creating CRD for '%s'" % (engine_name))
            cmd = "%s create --filename -" % (settings["misc"]["k8s-bin"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = json.dumps(engine["crd"]))
            endpoints.log_result(result)
            if result.exited != 0:
                log.error("Did not create CRD for '%s'" % (engine_name))
                failed_crds.append(engine_name)
            else:
                log.info("Created CRD for '%s'" % (engine_name))
                created_crds.append(engine_name)
        settings["engines"]["endpoint"]["created"]["succeeded"].extend(created_crds)
        settings["engines"]["endpoint"]["created"]["failed"].extend(failed_crds)
        if len(created_crds) > 0:
            log.info("Created the CRDs for these %d engines: %s" % (len(created_crds), created_crds))
        if len(failed_crds) > 0:
            log.error("Failed to create the CRDs for these %d engines: %s" % (len(failed_crds), failed_crds))
            return 2

        if not "pods" in settings["engines"]["endpoint"]:
            settings["engines"]["endpoint"]["pods"] = dict()

        pod_status = verify_pods_running(con, created_crds)
        log.info("Reviewing pods")
        for pod in pod_status.keys():
            log.info("Pod '%s' is running on node '%s'" % (engine, pod_status[pod]["node"]))

            if pod_status[pod]["node"] not in settings["engines"]["endpoint"]["hosting-nodes"]:
                log.info("Adding node '%s' to the list of hosting nodes" % (pod_status[pod]["node"]))
                settings["engines"]["endpoint"]["hosting-nodes"].append(pod_status[pod]["node"])

            settings["engines"]["endpoint"]["verification"]["verified"].append(pod_status[pod]["name"])

            settings["engines"]["endpoint"]["pods"][pod] = copy.deepcopy(pod_status[pod])
        unverified = 0
        for pod in created_crds:
            if pod not in settings["engines"]["endpoint"]["verification"]["verified"]:
                settings["engines"]["endpoint"]["verification"]["unverified"].append(pod)
                log.error("Pod '%s' has not been verified" % (pod))
                unverified += 1
        if unverified > 0:
            log.error("Could not verify that all pods were running")
            return 2

        log.info("There are %d hosting nodes: %s" % (len(settings["engines"]["endpoint"]["hosting-nodes"]), settings["engines"]["endpoint"]["hosting-nodes"]))
    
    return 0

def create_tools_pods():
    """

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance
        settings (dict): the one data structure to rule them all

    Returns:
        0
    """
    log.info("Creating Tools Pods")

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    if endpoint["disable-tools"]["all"]:
        log.info("Tools on all node types are disabled")
        return 0

    profiled_nodes = []

    if not endpoint["disable-tools"]["masters"]:
        log.info("Profiling all master nodes: %s" % (settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]))
        for node in settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]:
            log.info("Adding master node '%s' to the list of profiled nodes" % (node))
            profiled_nodes.append(node)
    else:
        log.info("Profiling of master nodes is disabled")

    if not endpoint["disable-tools"]["workers"]:
        log.info("Profiling active worker nodes")
        log.info("Worker nodes: %s" % (settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]))
        log.info("Active worker nodes: %s" % (settings["engines"]["endpoint"]["hosting-nodes"]))

        for node in settings["engines"]["endpoint"]["hosting-nodes"]:
            log.info("Analyzing active worker node '%s'" % (node))
            if node in profiled_nodes:
                if not endpoint["disable-tools"]["masters"] and node in settings["misc"]["k8s"]["nodes"]["endpoint"]["masters"]:
                    log.info("Already profiling worker node '%s' -- it is also a master node so that is probably why" % (node))
                else:
                    log.info("Already profiling worker node '%s' and I'm not really sure why..." % (node))
            else:
                log.info("Adding worker node '%s' to the list of profiled nodes because is hosting engine pods" % (node))
                settings["engines"]["endpoint"]["classes"]["profiled-nodes"].append(node)
                profiled_nodes.append(node)

        for node in settings["misc"]["k8s"]["nodes"]["endpoint"]["workers"]:
            if not node in profiled_nodes:
                log.info("Not adding worker node '%s' to the list of profiled nodes because it is not hosting engine pods" % (node))
    else:
        log.info("Profiling of worker nodes is disabled")

    log.info("Going to launch profiler pods on these nodes: %s" % (profiled_nodes))

    if len(profiled_nodes) == 0:
        log.info("No nodes to profile found")
        return 0

    log.info("Loading tools information and creating profiler mapping")
    tools = []
    try:
        tool_cmd_dir = "%s/%s" % (settings["engines"]["endpoint"]["first-engine"]["role"], settings["engines"]["endpoint"]["first-engine"]["id"])
        with open(settings["dirs"]["local"]["tool-cmds"] + "/" + tool_cmd_dir + "/start") as tool_cmd_file:
            for line in tool_cmd_file:
                split_line = line.split(":")
                tool = split_line[0]
                tools.append(tool)
                log.info("Adding tool '%s' to the list of tools" % (tool))
    except IOError as e:
        log.error("Failed to load the start tools command file from %s" % (tool_cmd_dir))
        return 1
    for tool in tools:
        if not tool in settings["engines"]["profiler-mapping"]:
            settings["engines"]["profiler-mapping"][tool] = {
                "name": tool,
                "label": "%s-%s" % (args.endpoint_label, tool),
                "ids": []
            }
            log.info("Created profiler mapping for tool '%s':\n%s" % (tool, endpoints.dump_json(settings["engines"]["profiler-mapping"][tool])))
        else:
            log.info("Profiler mapping for tool '%s' already exists" % (tool))

    log.info("Creating node profiling pods")

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
            log.error("Unknown role for tools pod to be run on node '%s'" % (pod["node"]))
            return 1

        log.info("Creating pod '%s-%d' to run on node '%s'" % (pod["role"], pod["id"], pod["node"]))

        pod["crd"], rc = create_pod_crd(pod["role"], pod["id"], node = pod["node"])
        if rc == 1:
            log.error("Failed to create CRD for '%s-%d'" % (pod["role"], pod["id"]))
            if pod["crd"] is None:
                log.error("No CRD available for '%s-%d'" % (pod["role"], pod["id"]))
            else:
                log.error("CRD generated for '%s-%d':\n%s" % (pod["role"], pod["id"], endpoints.dump_json(pod["crd"])))
        else:
            log.info("Created CRD for '%s-%d':\n%s" % (pod["role"], pod["id"], endpoints.dump_json(pod["crd"])))
        
        settings["engines"]["endpoint"]["classes"]["profiled-nodes"].append(pod)

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"], validate = False) as con:
        log.info("Validating CRDs")
        invalid_crds = []
        valid_crds = []
        for pod in settings["engines"]["endpoint"]["classes"]["profiled-nodes"]:
            pod_name = "%s-%d" % (pod["role"], pod["id"])
            log.info("Validating CRD for pod '%s'" % (pod_name))
            cmd = "%s create --filename - --dry-run=server --validate=strict" % (settings["misc"]["k8s-bin"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = json.dumps(pod["crd"]))
            endpoints.log_result(result)
            if result.exited != 0:
                log.error("Did not validate CRD for pod '%s'" % (pod_name))
                invalid_crds.append(pod_name)
            else:
                log.info("Validated CRD for pod '%s'" % (pod_name))
                valid_crds.append(pod_name)
        settings["engines"]["endpoint"]["validation"]["valid"].extend(valid_crds)
        settings["engines"]["endpoint"]["validation"]["invalid"].extend(invalid_crds)
        if len(valid_crds) > 0:
            log.info("Validated the CRDs for these %d pods: %s" % (len(valid_crds), valid_crds))
        if len(invalid_crds) > 0:
            log.error("Failed to validate the CRDs for these %d pods: %s" % (len(invalid_crds), invalid_crds))
            return 1

        log.info("Creating CRDs")
        failed_crds = []
        created_crds = []
        for pod in settings["engines"]["endpoint"]["classes"]["profiled-nodes"]:
            pod_name = "%s-%d" % (pod["role"], pod["id"])
            log.info("Creating CRD for pod '%s'" % (pod_name))
            cmd = "%s create --filename -" % (settings["misc"]["k8s-bin"])
            result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"], stdin = json.dumps(pod["crd"]))
            endpoints.log_result(result)
            if result.exited != 0:
                log.error("Did not create CRD for pod '%s'" % (pod_name))
                failed_crds.append(engine_name)
            else:
                log.info("Created CRD for pod '%s'" % (pod_name))
                created_crds.append(pod_name)
        settings["engines"]["endpoint"]["created"]["succeeded"].extend(created_crds)
        settings["engines"]["endpoint"]["created"]["failed"].extend(failed_crds)
        if len(created_crds) > 0:
            log.info("Created the CRDs for these %d pods: %s" % (len(created_crds), created_crds))
        if len(failed_crds) > 0:
            log.error("Failed to create the CRDs for these %d pods: %s" % (len(failed_crds), failed_crds))
            return 2

        if not "pods" in settings["engines"]["endpoint"]:
            settings["engines"]["endpoint"]["pods"] = dict()

        pod_status = verify_pods_running(con, created_crds)
        log.info("Reviewing pods")
        for pod in pod_status.keys():
            log.info("Pod '%s' is running on node '%s'" % (pod, pod_status[pod]["node"]))

            log.info("Adding the containers in pod '%s' to the new followers list: %s" % (pod_status[pod]["name"], pod_status[pod]["containers"]))
            settings["engines"]["new-followers"].extend(pod_status[pod]["containers"])

            settings["engines"]["endpoint"]["verification"]["verified"].append(pod_status[pod]["name"])

            settings["engines"]["endpoint"]["pods"][pod] = copy.deepcopy(pod_status[pod])
        unverified = 0
        for pod in created_crds:
            if pod not in settings["engines"]["endpoint"]["verification"]["verified"]:
                settings["engines"]["endpoint"]["verification"]["unverified"].append(pod)
                log.error("Pod '%s' has not been verified" % (pod))
                unverified += 1
        if unverified > 0:
            log.error("Could not verify that all pods were running")
            return 2
            
    return 0

def kube_cleanup():
    """
    Attempt to cleanup the K8S namespace by collecting logs from the pods and then deleting everything

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        0
    """
    log.info("Running cleanup")

    endpoint = settings["run-file"]["endpoints"][args.endpoint_index]

    cleanup_error = "An error has been encountered during cleanup -> the namespace ('%s') will be left untouched for inspection" % (endpoint["namespace"]["name"])

    with endpoints.remote_connection(settings["run-file"]["endpoints"][args.endpoint_index]["host"],
                                     settings["run-file"]["endpoints"][args.endpoint_index]["user"]) as con:
        errors = False

        log.info("Current K8S namespace '%s' status" % (endpoint["namespace"]["name"]))
        cmd = "%s get all --namespace %s --output wide" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
        result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"])
        endpoints.log_result(result)
        if result.exited != 0:
            log.error(cleanup_error)
            errors = True

        log.info("Collecting engine logs")
        pods = list(settings["engines"]["endpoint"]["pods"].keys())
        pods.sort()
        for pod in pods:
            pod_name = settings["engines"]["endpoint"]["pods"][pod]["name"]
            node_name = settings["engines"]["endpoint"]["pods"][pod]["node"]
            log.info("Processing pod '%s' on node '%s'" % (pod_name, node_name))
            for engine in settings["engines"]["endpoint"]["pods"][pod]["containers"]:
                log.info("Collecting log for engine '%s'" % (engine))
                cmd = "%s logs %s-%s --namespace %s --container %s" % (settings["misc"]["k8s-bin"],
                                                                       endpoint_defaults["prefix"]["pod"],
                                                                       pod,
                                                                       endpoint["namespace"]["name"],
                                                                       engine)
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"])
                if result.exited == 0:
                    log_file = "%s/%s.txt" % (settings["dirs"]["local"]["engine-logs"], engine)
                    with open(log_file, "w", encoding="ascii") as lfh:
                        lfh.write(result.stdout)
                    log.info("Wrote log for engine '%s' in pod '%s' to '%s'" % (engine, pod, log_file))
                else:
                    log.error("Failed to collect log for engine '%s' in pod '%s'" % (engine, pod))
                    endpoints.log_result(result)
                    if not errors:
                        log.error(cleanup_error)
                        errors = True

        if not errors:
            if clean_k8s_namespace(con):
                log.error(cleanup_error)
                errors = True
            else:
                log.info("Deleting namepsace: %s" % (endpoint["namespace"]["name"]))
                cmd = "%s delete namespace %s" % (settings["misc"]["k8s-bin"], endpoint["namespace"]["name"])
                result = endpoints.run_remote(con, cmd, debug = settings["misc"]["debug-output"])
                endpoints.log_result(result)
                if result.exited != 0:
                    log.error("Failed to delete namespace: %s" % (endpoint["namespace"]["name"]))
                    log.error(cleanup_error)
                    errors = True
        else:
            log.warning("Skipping namespace cleanup due to prior errors")

    return 0

def engine_init():
    """
    Construct messages to initialize the engines with metadata specific to them

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        log: a logger instance
        settings (dict): the one data structure to rule then all

    Returns:
        env_vars_msg_file (str): A file containing all the messages to send to the engines
    """
    log.info("Building messages to send engine-specific metadata to the engines")
    env_vars_msgs = []
    pods = list(settings["engines"]["endpoint"]["pods"].keys())
    pods.sort()
    for pod in pods:
        pod_name = settings["engines"]["endpoint"]["pods"][pod]["name"]
        node_name = settings["engines"]["endpoint"]["pods"][pod]["node"]
        log.info("Processing pod '%s' on node '%s'" % (pod_name, node_name))
        for engine in settings["engines"]["endpoint"]["pods"][pod]["containers"]:
            log.info("Processing engine '%s'" % (engine))

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
                    "endpoint-label": args.endpoint_label,
                    "hosted_by": node_name,
                    "hypervisor_host": None,
                    "userenv": userenv,
                    "osruntime": osruntime
                }
            }

            env_vars_msgs.extend(endpoints.create_roadblock_msg("follower", engine, "user-object", env_vars_payload))
    
    env_vars_msg_file = settings["dirs"]["local"]["roadblock-msgs"] + "/env-vars.json"
    log.info("Writing follower env-vars messages to %s" % (env_vars_msg_file))
    env_vars_msgs_json = endpoints.dump_json(env_vars_msgs)
    with open(env_vars_msg_file, "w", encoding = "ascii") as env_vars_msg_file_fp:
        env_vars_msg_file_fp.write(env_vars_msgs_json)
    log.info("Contents of %s:\n%s" % (env_vars_msg_file, env_vars_msgs_json))

    return env_vars_msg_file


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
    early_abort = False

    if args.validate:
        return(validate())

    log = endpoints.setup_logger(args.log_level)

    endpoints.log_cli(args)
    settings = endpoints.init_settings(settings, args)

    settings = endpoints.load_settings(settings,
                                       endpoint_name = "kube",
                                       run_file = args.run_file,
                                       rickshaw_dir = args.rickshaw_dir,
                                       endpoint_index = args.endpoint_index,
                                       endpoint_normalizer_callback = normalize_endpoint_settings)
    if settings is None:
        return 1

    if check_base_requirements() != 0:
        return 1

    endpoints.create_local_dirs(settings)

    if get_k8s_config() != 0:
        return 1

    if init_k8s_namespace() != 0:
        return 1

    if compile_object_configs() != 0:
        return 1

    if create_cs_pods(cpu_partitioning = True) != 0:
        return 1
    if create_cs_pods(cpu_partitioning = False) != 0:
        return 1
    if create_tools_pods() != 0:
        return 1

    # KMR implement callbacks
    kube_callbacks = {
        "engine-init": engine_init,
        "collect-sysinfo": None,
        "test-start": None,
        "test-stop": None,
        "remote-cleanup": kube_cleanup
    }
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
                                      early_abort = early_abort)

    log.info("Logging 'final' settings data structure")
    endpoints.log_settings(settings, mode = "settings")
    log.info("kube endpoint exiting")
    return rc

if __name__ == "__main__":
    args = endpoints.process_options()
    log = None
    settings = dict()
    exit(main())
