#!/usr/bin/python3

'''Endpoint to run 1 or more engines on 1 or more remotehost systems'''

import argparse
from fabric import Connection
import jsonschema
import logging
import os
from paramiko import ssh_exception
from pathlib import Path
import sys
import threading

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

defaults = {
    "user": "root"
}

def process_options():
    '''Handle the CLI argument parsing options'''

    parser = argparse.ArgumentParser(description = "Endpoint to run 1 or more engines on 1 or more remotehost systems",
                                     formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--base-run-dir",
                        dest = "base_run_dir",
                        help = "The base directory where the run (and all it's components, data, results, etc.) are stored.",
                        required = True,
                        type = str)

    parser.add_argument("--bench-ids",
                        dest = "bench_ids",
                        help = "Comma separated list of the benchmarks to run on this endpoint and on which engine IDs",
                        required = False,
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
    return print(msg)

def validate_comment(msg):
    return print("#" + msg)

def validate_error(msg):
    return print("ERROR: " + msg)

def validate():
    stream = "params:"
    for i in range(1, len(sys.argv)):
        stream += " %s" % sys.argv[i]
    validate_comment(stream)

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

    valid, err = validate_schema(endpoint_settings, args.rickshaw_dir + "/util/JSON/schema-remotehosts.json")
    if not valid:
        validate_error(err)
        return 1

    engines = dict()
    userenvs = dict()
    default_userenv = None
    if "userenv" in endpoint_settings["settings"]:
        default_userenv = endpoint_settings["settings"]["userenv"]
    else:
        rickshaw_settings_json, err = load_json_file(args.base_run_dir + "/config/rickshaw-settings.json.xz", uselzma = True)
        if rickshaw_settings_json is None:
            validate_error(err)
            return 1
        validate_comment("rickshaw-settings: %s" % rickshaw_settings_json)
        default_userenv = rickshaw_settings_json["userenvs"]["default"]["benchmarks"]
    userenvs[default_userenv] = 0

    for remote in endpoint_settings["remotes"]:
        for engine in remote["engines"]:
            if not engine["role"] in engines:
                engines[engine["role"]] = []
            if isinstance(engine["ids"], list):
                for id in engine["ids"]:
                    engines[engine["role"]].append(id)
            else:
                engines[engine["role"]].append(engine["ids"])

        if "settings" in remote["config"] and "userenv" in remote["config"]["settings"]:
            if not userenvs[remote["config"]["settings"]["userenv"]]:
                userenvs[remote["config"]["settings"]["userenv"]] = 1
            else:
                userenvs[remote["config"]["settings"]["userenv"]] += 1
        else:
            if default_userenv is not None:
                userenvs[default_userenv] += 1

    validate_comment("engines: %s" % (engines))
    for role in engines.keys():
        stream = "%s" % (role)
        for id in engines[role]:
            if isinstance(id, int):
                stream += " %d" % (id)
            elif isinstance(id, str):
                subids = id.split("-")
                if len(subids) == 1:
                    stream += " %s" % (subids[0])
                else:
                    subids = list(map(int, subids))
                    if subids[0] >= 0 and subids[0] < subids[1]:
                        for subid in range(subids[0], subids[1]+1):
                            stream += " %d" % (subid)
                    else:
                        validate_error("Invalid id range: %s" % (id))
                        return 1
        validate_log(stream)

    validate_comment("userenvs: %s" % (userenvs))
    for userenv in userenvs.keys():
        if userenvs[userenv] > 0:
            validate_log("userenv %s" % (userenv))

    for remote in endpoint_settings["remotes"]:
        remote_user = defaults["user"]
        if "settings" in remote["config"] and "user" in remote["config"]["settings"]:
            remote_user = remote["config"]["settings"]["user"]
        elif "settings" in endpoint_settings and "user" in endpoint_settings["settings"]:
            remote_user = endpoint_settings["settings"]["user"]

        remote_can_login = False
        try:
            with Connection(host = remote["config"]["host"], user = remote_user) as c:
                result = c.run("uptime", hide = True)
                validate_comment("remote login verification for %s with user %s: rc=%d and stdout=[%s] annd stderr=[%s]" % (remote["config"]["host"], remote_user, result.exited, result.stdout.rstrip('\n'), result.stderr.rstrip('\n')))
                remote_can_login = True
        except ssh_exception.AuthenticationException as e:
            validate_comment("remote login verification for %s with user %s resulted in an authentication exception" % (remote["config"]["host"], remote_user))
        if not remote_can_login:
            validate_error("Could not verify ability to login to remote %s" % (remote["config"]["host"]))
        else:
            with Connection(host = remote["config"]["host"], user = remote_user) as c:
                result = c.run("podman --version", hide = True)
                validate_comment("remote podman presence check for %s: rc=%d and stdout=[%s] and stderr=[%s]" % (remote["config"]["host"], result.exited, result.stdout.rstrip('\n'), result.stderr.rstrip('\n')))
                if result.exited != 0:
                    result = c.run("yum install -y podman", hide = True)
                    validate_comment("remote podman installation for %s: rc=%d" % (remote["config"]["host"], result.exited))
                    if result.exited != 0:
                        validate_error("Could not install podman to remote %s" % (remote["config"]["host"]))
                        validate_error("stdout:\n%s" % (result.stdout))
                        validate_error("stderr:\n%s" % (result.stderr))

    return 0

def init_settings():
    log.info("Initializing settings based on CLI parameters")

    settings["dirs"] = dict()

    settings["dirs"]["local"] = {
        "base": args.base_run_dir,
        "conf": args.base_run_dir + "/config",
        "run": args.base_run_dir + "/run",
        "engine": args.base_run_dir + "/engine",
        "endpoint": args.base_run_dir + "/endpoint/" + args.endpoint_label
    }
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
    settings["dirs"]["remote"]["tmp"] = settings["dirs"]["remote"]["data"] + "/tmp"

    log_settings()

    return 0

def log_settings():
    return log.info("settings:\n%s" % (dump_json(settings)))

def dump_json(obj):
    return json.dumps(obj, indent = 4, separators=(',', ': '), sort_keys = True)

def my_make_dirs(mydir):
    log.info("Creating directory %s (recurisvely if necessary)" % (mydir))
    return os.makedirs(mydir, exist_ok = True)

def create_local_dirs():
    my_make_dirs(settings["dirs"]["local"]["run"])
    my_make_dirs(settings["dirs"]["local"]["engine-logs"])
    my_make_dirs(settings["dirs"]["local"]["roadblock-msgs"])
    return 0

def load_settings():
    log.info("Loading settings from config files")

    rickshaw_settings_file = settings["dirs"]["local"]["conf"] + "/rickshaw-settings.json.xz"
    settings["rickshaw"],err = load_json_file(rickshaw_settings_file, uselzma = True)
    if settings["rickshaw"] is None:
        log.error("Failed to load rickshaw-settings from %s with error '%s'" % (rickshaw_settings_file, err))
        return 1
    else:
        log.info("Loaded rickshaw-settings from %s" % (rickshaw_settings_file))

    settings["run-file"],err = load_json_file(args.run_file)
    if settings["run-file"] is None:
        log.error("Failed to load run-file from %s with error '%s'" % (args.run_file, err))
        return 1
    else:
        log.info("Loaded run-file from %s" % (args.run_file))

    log_settings()

    return 0

def setup_logger():
    logging.basicConfig(level = logging.INFO, format = '[%(asctime)s %(levelname)s %(module)s %(funcName)s:%(lineno)d] %(message)s', stream = sys.stdout)
        
    return logging.getLogger(__file__)

def main():
    '''Main control block'''

    global args
    global log
    global settings

    if args.validate:
        return(validate())

    log = setup_logger()

    init_settings()
    if load_settings() != 0:
        return 1
    create_local_dirs()

    return 1

if __name__ == "__main__":
    args = process_options()
    log = None
    settings = dict()
    exit(main())
