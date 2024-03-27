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

endpoint_defaults = {
    "cpu-partitioning": False,
    "disable-tools": False,
    "numa-node": None,
    "osruntime": "podman",
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

def cli_stream():
    stream = ""
    for i in range(1, len(sys.argv)):
        stream += " %s" % sys.argv[i]
    return stream

def validate():
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

    rickshaw_settings, err = load_json_file(args.base_run_dir + "/config/rickshaw-settings.json.xz", uselzma = True)
    if rickshaw_settings is None:
        validate_error(err)
        return 1
    validate_comment("rickshaw-settings: %s" % rickshaw_settings)

    endpoint_settings = normalize_endpoint_settings(endpoint_settings, rickshaw_settings, validate = True)
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

    for remote in endpoint_settings["remotes"]:
        remote_can_login = False
        try:
            with Connection(host = remote["config"]["host"], user = remote["config"]["settings"]["remote-user"]) as c:
                result = c.run("uptime", hide = True)
                validate_comment("remote login verification for %s with user %s: rc=%d and stdout=[%s] annd stderr=[%s]" % (remote["config"]["host"], remote["config"]["settings"]["remote-user"], result.exited, result.stdout.rstrip('\n'), result.stderr.rstrip('\n')))
                remote_can_login = True
        except ssh_exception.AuthenticationException as e:
            validate_comment("remote login verification for %s with user %s resulted in an authentication exception" % (remote["config"]["host"], remote["config"]["settings"]["remote-user"]))
        if not remote_can_login:
            validate_error("Could not verify ability to login to remote %s" % (remote["config"]["host"]))
        else:
            with Connection(host = remote["config"]["host"], user = remote["config"]["settings"]["remote-user"]) as c:
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

def log_cli():
    log.info("Logging CLI")

    log.info("CLI parameters:\n%s" % (cli_stream()))

    the_args = dict()
    for arg in args.__dict__:
        the_args[str(arg)] = args.__dict__[arg]
    log.info("argparse:\n %s" % (dump_json(the_args)))

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
    match mode:
        case "benchmark-mapping":
            return log.info("settings[benchmark-mapping]:\n%s" % (dump_json(settings["engines"]["benchmark-mapping"])))
        case "engines":
            return log.info("settings[engines]:\n%s" % (dump_json(settings["engines"])))
        case "misc":
            return log.info("settings[misc]:\n%s" % (dump_json(settings["misc"])))
        case "dirs":
            return log.info("settings[dirs]:\n%s" % (dump_json(settings["dirs"])))
        case "endpoint":
            return log.info("settings[endpoint]:\n%s" % (dump_json(settings["run-file"]["endpoints"][args.endpoint_index])))
        case "rickshaw":
            return log.info("settings[rickshaw]:\n%s" % (dump_json(settings["rickshaw"])))
        case "run-file":
            return log.info("settings[run-file]:\n%s" % (dump_json(settings["run-file"])))
        case "all" | _:
            return log.info("settings:\n%s" % (dump_json(settings)))

def dump_json(obj):
    return json.dumps(obj, indent = 4, separators=(',', ': '), sort_keys = True)

def my_make_dirs(mydir):
    log.info("Creating directory %s (recurisvely if necessary)" % (mydir))
    return os.makedirs(mydir, exist_ok = True)

def create_local_dirs():
    log.info("Creating local directories")
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
                settings["misc"]["ssh-private-key"] += line
    except IOError as e:
        log.error("Failed to load the SSH private key [%s]" % (e))
        return 1

    log_settings(mode = "misc")

    return 0

def expand_id_range(id_range):
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
                ids.append(id)
            elif isinstance(id, str):
                new_ids.extend(expand_id_range(id))

    new_ids.sort()

    return new_ids

def normalize_endpoint_settings(endpoint, rickshaw, validate = False):
    defaults = {
        "cpu-partitioning": endpoint_defaults["cpu-partitioning"],
        "disable-tools": endpoint_defaults["disable-tools"],
        "numa-node": endpoint_defaults["numa-node"],
        "osruntime": endpoint_defaults["osruntime"],
        "remote-user": endpoint_defaults["user"],
        "userenv": rickshaw["userenvs"]["default"]["benchmarks"]
    }

    if "settings" in endpoint:
        for key in defaults.keys():
            if key in endpoint["settings"]:
                defaults[key] = endpoint["settings"][key]

    for remote in endpoint["remotes"]:
        if not "settings" in remote["config"]:
            remote["config"]["settings"] = dict()

        for key in defaults.keys():
            if not key in remote["config"]["settings"]:
                remote["config"]["settings"][key] = defaults[key]

        for engine in remote["engines"]:
            if engine["role"] == "profiler":
                continue
            try:
                engine["ids"] = expand_ids(engine["ids"])
            except ValueError as e:
                if validate:
                    validate_error("While expanding '%s' encountered exception '%s'" % (engine["ids"], str(e)))
                    return None

    return endpoint

def check_base_requirements():
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

def build_profiler_config():
    log.info("Building profiler engine configs")

    if not "engines" in settings:
        settings["engines"] = dict()
    settings["engines"]["remotes"] = dict()
    settings["engines"]["profiler-mapping"] = dict()

    for remote_idx,remote in enumerate(settings["run-file"]["endpoints"][args.endpoint_index]["remotes"]):
        if remote["config"]["settings"]["disable-tools"]:
            continue

        if not remote["config"]["host"] in settings["engines"]["remotes"]:
            settings["engines"]["remotes"][remote["config"]["host"]] = {
                "first-engine": None,
                "roles": dict(),
                "run-file-idx": []
            }

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

            if not tool in settings["engines"]["profiler-mapping"]:
                settings["engines"]["profiler-mapping"][tool] = {
                    "name": tool,
                    "ids": []
                }
            settings["engines"]["profiler-mapping"][tool]["ids"].append(profiler_id)

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
    mapping = dict()

    for benchmark_idx,benchmark in enumerate(benchmarks):
        benchmark_id = benchmark["name"] + "-" + str(benchmark_idx)
        mapping[benchmark_id] = {
            "name": benchmark["name"],
            "ids": expand_ids(benchmark["ids"])
        }

    return mapping

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

    log_cli()
    init_settings()
    if load_settings() != 0:
        return 1
    if check_base_requirements() != 0:
        return 1
    if build_profiler_config() != 0:
        return 1
    create_local_dirs()

    return 1

if __name__ == "__main__":
    args = process_options()
    log = None
    settings = dict()
    exit(main())
