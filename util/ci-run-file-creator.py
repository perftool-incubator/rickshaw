#!/usr/bin/python3

"""
Create a run-file to use for CI based on provided inputs
"""

import argparse
import json
import logging
import os
from pathlib import Path
import sys

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
    parser = argparse.ArgumentParser(description = "Script to generate run-file jobs for use by crucible-ci",
                                     formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--benchmark",
                        dest = "benchmark",
                        help = "The benchmark to return a run-file for",
                        required = True,
                        choices = [ "cyclictest", "fio", "multi", "oslat" ],
                        type = str)

    parser.add_argument("--controller-ip",
                        dest = "controller_ip",
                        help = "The controller-ip to tell the endpoint to reference",
                        required = True,
                        type = str)

    parser.add_argument("--endpoint",
                        dest = "endpoint",
                        help = "The endpoint to return a run-file for",
                        required = True,
                        choices = [ "remotehost", "k8s" ],
                        type = str)

    parser.add_argument("--endpoint-sub-type",
                        dest = "endpoint_sub_type",
                        help = "The endpoint sub type information to return a run-file for",
                        required = True,
                        choices = [ "GENERIC", "MICROK8S", "NONE", "OCP" ],
                        type = str)

    parser.add_argument("--host",
                        dest = "host",
                        help = "The host to target in the run-file",
                        required = True,
                        type = str)

    parser.add_argument("--output-file",
                        dest = "output_file",
                        help = "The output file to write the resulting run-file to",
                        required = True,
                        type = str)

    parser.add_argument("--repeat-runs",
                        dest = "repeat_runs",
                        help = "Whether or not repeat-runs are being performed",
                        required = True,
                        choices = [ "no", "yes" ],
                        type = str)

    parser.add_argument("--run-number",
                        dest = "run_number",
                        help = "The run number",
                        required = True,
                        type = int)

    parser.add_argument("--samples",
                        dest = "samples",
                        help = "The number of samples to collect",
                        required = True,
                        type = int)

    parser.add_argument("--test-order",
                        dest = "test_order",
                        help = "The test order to use",
                        required = True,
                        choices = [ "r", "s" ],
                        type = str)

    parser.add_argument("--user",
                        dest = "user",
                        help = "The user to use in the run-file",
                        required = True,
                        type = str)

    parser.add_argument("--userenv",
                        dest = "userenv",
                        help = "The userenv to target in the run-file",
                        required = True,
                        type = str)
    
    args = parser.parse_args()

    return args

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
    return json.dumps(obj, indent = 4, separators=(',', ': '), sort_keys = True)

def update_endpoint_sub_type():
    """
    Update (or add) the neccesary endpoint sub-type information

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_file (dict): the target run-file

    Returns:
        None
    """
    if "endpoint-sub-type" in run_file["tags"]:
        log.info("Updating existing endpoint-sub-type tag")
    else:
        log.info("Creating endpoint-sub-type tag")

    run_file["tags"]["endpoint-sub-type"] = args.endpoint_sub_type

    match args.endpoint:
        case "remotehost":
            if args.endpoint_sub_type != "NONE":
                msg = "%s endpoint expected sub-type of 'NONE' (not '%s')" % (args.endpoint, args.endpoint_sub_type)
                log.error(msg)
                raise ValueError(msg)
            else:
                log.info("No endpoint sub-type work necessary for endpoint type %s" % (args.endpoint))
        case "k8s":
            for endpoint_idx,endpoint in enumerate(run_file["endpoints"]):
                if "kubeconfig" in endpoint:
                    log.info("Updating existing kubconfig value for k8s endpoint %d" % (endpoint_idx))
                else:
                    log.info("Creating kubeconfig value for k8s endpoint %d" % (endpoint_idx))

                match args.endpoint_sub_type:
                    case "GENERIC"|"MICROK8S":
                        endpoint["kubeconfig"] = 0
                    case "NONE":
                        msg = "k8s endpoint expected sub-type of either 'GENERIC', 'MICROK8S', or 'OCP' (not 'NONE')"
                        log.error(msg)
                        raise ValueError(msg)
                    case "OCP":
                        endpoint["kubeconfig"] = 1

    return

def update_userenvs():
    """
    Update (or add) the neccesary userenv

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_file (dict): the target run-file

    Returns:
        None
    """
    if "userenv" in run_file["tags"]:
        log.info("Updating existing userenv tag")
    else:
        log.info("Creating userenv tag")

    run_file["tags"]["userenv"] = args.userenv

    match args.endpoint:
        case "k8s"|"remotehost":
            for endpoint_idx,endpoint in enumerate(run_file["endpoints"]):
                if "userenv" in endpoint:
                    if args.userenv == "default":
                        log.info("Found existing %s userenv for endpoint %d but since requested userenv is default it is being removed" % (args.endpoint, endpoint_idx))
                        del endpoint["userenv"]
                    else:
                        log.info("Updating existing %s userenv for endpoint %d" % (args.endpoint, endpoint_idx))
                        endpoint["userenv"] = args.userenv
                else:
                    if args.userenv == "default":
                        log.info("No %s userenv to update for endpoint %d since requested userenv is default" % (args.endpoint, endpoint_idx))
                    else:
                        log.info("Creating %s userenv for endpoint %d since requested userenv is not default" % (args.endpoint, endpoint_idx))
                        endpoint["userenv"] = args.userenv

    return

def update_controller_ip():
    """
    Update (or add) the controller-ip

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_file (dict): the target run-file

    Returns:
        None
    """
    if "controller-ip" in run_file["tags"]:
        log.info("Updating existing controller-ip tag")
    else:
        log.info("Creating controller-ip tag")

    run_file["tags"]["controller-ip"] = args.controller_ip

    match args.endpoint:
        case "k8s"|"remotehost":
            for endpoint_idx,endpoint in enumerate(run_file["endpoints"]):
                if "controller-ip" in endpoint:
                    log.info("Updating existing %s controller-ip for endpoint %d" % (args.endpoint, endpoint_idx))
                    endpoint["controller-ip"] = args.controller_ip
                else:
                    log.debug("No controller-ip to update for %s endpoint %d" % (args.endpoint, endpoint_idx))

    return

def update_host():
    """
    Update (or add) the host

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_file (dict): the target run-file

    Returns:
        None
    """
    if "host" in run_file["tags"]:
        log.info("Updating existing host tag")
    else:
        log.info("Creating host tag")

    run_file["tags"]["host"] = args.host

    match args.endpoint:
        case "k8s"|"remotehost":
            for endpoint_idx,endpoint in enumerate(run_file["endpoints"]):
                if "host" in endpoint:
                    log.info("Updating existing %s host for endpoint %d" % (args.endpoint, endpoint_idx))
                    endpoint["host"] = args.host
                else:
                    log.debug("No host to update for %s endpoint %d" % (args.endpoint, endpoint_idx))

    return

def update_user():
    """
    Update (or add) the user

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_file (dict): the target run-file

    Returns:
        None
    """
    if "user" in run_file["tags"]:
        log.info("Updating existing user tag")
    else:
        log.info("Creating user tag")

    run_file["tags"]["user"] = args.user

    match args.endpoint:
        case "k8s"|"remotehost":
            for endpoint_idx,endpoint in enumerate(run_file["endpoints"]):
                if "user" in endpoint:
                    log.info("Updating existing %s user for endpoint %d" % (args.endpoint, endpoint_idx))
                    endpoint["user"] = args.user
                else:
                    log.debug("No user to update for %s endpoint %d" % (args.endpoint, endpoint_idx))

    return

def update_samples():
    """
    Update (or add) the number of samples

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_file (dict): the target run-file

    Returns:
        None
    """
    if "num-samples" in run_file["tags"]:
        log.info("Updating existing num-samples tag")
    else:
        log.info("Creating num-samples tag")

    run_file["tags"]["num-samples"] = str(args.samples)

    if "num-samples" in run_file["run-params"]:
        log.info("Updating existing num-samples")
    else:
        log.info("Creating num-samples")

    run_file["run-params"]["num-samples"] = args.samples

    return

def update_test_order():
    """
    Update (or add) the test-order

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_file (dict): the target run-file

    Returns:
        None
    """
    if "test-order" in run_file["run-params"]:
        log.info("Updating existing test-order")
    else:
        log.info("Creating test-order")

    run_file["run-params"]["test-order"] = args.test_order

    return

def update_run_number():
    """
    Update (or add) the run number tag

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_file (dict): the target run-file

    Returns:
        None
    """
    if "run" in run_file["tags"]:
        log.info("Updating existing run-number tag")
    else:
        log.info("Creating run-number tag")

    run_file["tags"]["run-number"] = str(args.run_number)

    return

def update_repeat_runs():
    """
    Update (or add) the repeat-runs tag

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_file (dict): the target run-file

    Returns:
        None
    """
    if "repeat" in run_file["tags"]:
        log.info("Updating existing repeat tag")
    else:
        log.info("Creating repeat tag")

    run_file["tags"]["repeat"] = args.repeat_runs

    return

def main():
    """
    Main control block

    Args:
        None

    Globals:
        args (namespace): the script's CLI parameters
        run_files (dict): the base run-files to build/modify

    Returns:
        0
    """
    global args
    global log
    global run_files
    global run_file

    log_format = "[%(asctime)s %(levelname)s %(module)s %(funcName)s:%(lineno)d] %(message)s"
    logging.basicConfig(level = logging.DEBUG, format = log_format, stream = sys.stdout)
    log = logging.getLogger(__file__)

    log.info("Attempting to create run-file for benchmark %s and endpoint %s" % (args.benchmark, args.endpoint))

    run_file_filename = "%s/crucible-ci/%s.%s.json" % (os.path.dirname(os.path.abspath(__file__)),
                                                       args.benchmark,
                                                       args.endpoint)

    run_file,err_msg = load_json_file(run_file_filename)
    if run_file is None:
        log.error("Failed to load base run-file with error '%s'" % (err_msg))
        return 1

    update_endpoint_sub_type()
    update_userenvs()
    update_controller_ip()
    update_host()
    update_user()
    update_samples()
    update_test_order()
    update_run_number()
    update_repeat_runs()

    log.info("Writing resulting run-file JSON to %s" % (args.output_file))
    with open(args.output_file, "w", encoding = "ascii") as fp:
        fp.write(dump_json(run_file))
    
    return 0

if __name__ == "__main__":
    args = process_options()
    log = None
    exit(main())
