import argparse
import calendar
from fabric import Connection
from invoke import run
import json
import logging
import os
from pathlib import Path
import re
import sys
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

ROADBLOCK_HOME = os.environ.get('ROADBLOCK_HOME')
if ROADBLOCK_HOME is None:
    print("This script requires libraries that are provided by the roadblock project.")
    print("Roadblock can be acquired from https://github.com/perftool-incubator/roadblock and")
    print("then use 'export ROADBLOCK_HOME=/path/to/roadblock so that it can be located.")
    exit(1)
else:
    p = Path(ROADBLOCK_HOME) / 'roadblock.py'
    if not p.exists() or not p.is_file():
        print("ERROR: <ROADBLOCK_HOME>/roadblock.py ('%s') does not exist!" % (p))
        exit(2)
    sys.path.append(str(Path(ROADBLOCK_HOME)))
from roadblock import roadblock

roadblock_exits = {
    "success": 0,
    "timeout": 3,
    "abort": 4,
    "input": 2
}

log = logging.getLogger(__file__)

def run_remote(connection, command, validate = False, debug = False):
    """
    Run a command on a remote server using an existing Fabric connection

    Args:
        connection (Fabric Connection): The connection to use to run the command remotely
        command (str): The command to run
        validate (bool): Is the function being called from validation mode (which means that logging cannot be used)
        debug (bool): Is debug output enabled during validation mode

    Globals:
        None

    Returns:
        Fabric run result (obj)
    """
    debug_msg = "on remote '%s' as '%s' running command '%s'" % (connection.host, connection.user, command)
    if validate:
        if debug:
            validate_debug(debug_msg)
    else:
        log.debug(debug_msg, stacklevel = 2)

    return connection.run(command, hide = True, warn = True)

def run_local(command, validate = False, debug = False):
    """
    Run a command on the local machine using Invoke

    Args:
        command (str): The command to run on the local system
        validate (bool): Is the function being called from validation mode (which means that logging cannot be used)
        debug (bool): Is debug output enabled during validation mode

    Globals:
        None

    Returns:
        an Invoke run result
    """
    debug_msg = "running local command '%s'" % (command)
    if validate:
        if debug:
            validate_debug(debug_msg)
    else:
        log.debug(debug_msg, stacklevel = 2)

    return run(command, hide = True, warn = True)

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

def remote_connection(host, user, validate = False):
    """
    Create a Fabric connection and open it

    Args:
       host (str): The IP address or hostname to connect to using Fabric
       user (str): The username to connect as
       validate (bool): Is the function being called from validation mode (which means that logging cannot be used)

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
                if validate:
                    validate_comment(msg)
                else:
                    log.info(msg)
            break
        except (ssh_exception.AuthenticationException, ssh_exception.NoValidConnectionsError) as e:
            msg = "Failed to connect to remote '%s' as user '%s' on attempt %d due to '%s'" % (host, user, attempt, str(e))
            if validate:
                validate_comment(msg)
            else:
                log.warning(msg)

            if attempt == attempts:
                msg = "Failed to connect to remote '%s' as user '%s' and maximum number of attempts (%d) has been exceeded.  Reraising exception '%s'" % (host, user, attempts, str(e))
                if validate:
                    validate_error(msg)
                else:
                    log.error(msg)
                raise e
            else:
                time.sleep(1)
    return connection

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

def log_cli(args):
    """
    Log the script invocation details in a readable form

    Args:
        args (namespace): the script's CLI parameters

    Globals:
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
        return obj.to_dictionary()
    except AttributeError:
        return repr(obj)

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
    log.info("Checking to see if '%s' is an IP address" % (ip_address))

    # check for IPv4
    m = re.search(r"[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}\.[1-9][0-9]{0,2}", ip_address)
    if m:
        return True

    # check for IPv6
    m = re.search(r"[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}:[0-9a-fA-F]", ip_address)
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

def create_roadblock_msg(recipient_type, recipient_id, payload_type, payload):
    """
    Create a user built roadblock message

    Args:
        recipient_type (str): What type of roadblock participant ("leader" or "follower" or "all") is the message for
        recipient_id (str): What is the specific name/ID of the intended message recipient

    Globals:
        None

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

def do_roadblock(roadblock_id = None, label = None, timeout = None, messages = None, wait_for = None, abort = None, max_attempts = 1, follower_id = None, redis_password = None, msgs_dir = None):
    """
    Run a roadblock

    Args:
        roadblock_id (str): The base ID to use as part of the roadblock's name
        label (str): The name of the roadblock to participate in
        timeout (int): An optional timeout value to use for the roadblock
        messages (str): An optional messages file to send
        wait_for (str): An optional command to wait on to complete the roadblock
        abort (bool): An optional parameter specifying that a abort should be sent
        max_attempts (int): The maximum number of attempts for the roadblock
        follower_id (str): The follower ID to use to communicate with the leader
        redis_password (str): The password used to connect to the redis server
        msgs_dir (str): The directory where the message log should be saved

    Globals:
        None

    Returns:
        rc (int): The return code for the roadblock
    """
    if label is None:
        log.error("No roadblock label specified", stacklevel = 2)
        raise ValueError("No roadblock label specified")

    log.info("Processing roadblock '%s'" % (label), stacklevel = 2)
    uuid = "%s:%s" % (roadblock_id, label)
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

    msgs_log_file = msgs_dir + "/" + label + ".json"
    log.info("[%s] Logging messages to: %s" % (label, msgs_log_file))

    redis_server = "localhost"
    leader = "controller"

    result = run_local("ping -w 10 -c 4 " + redis_server)
    ping_log_msg = "[%s] Pinged redis server '%s' with return code %d:\nstdout:\n%sstderr:\n%s" % (label, redis_server, result.exited, result.stdout, result.stderr)
    if result.exited != 0:
        log.error(ping_log_mesg)
    else:
        log.info(ping_log_msg)

    attempts = 0
    rc = -1
    while attempts < max_attempts and rc != roadblock_exits["success"] and rc != roadblock_exits["abort"]:
        attempts += 1
        log.info("[%s] Attempt number: %d" % (label, attempts))

        my_roadblock = roadblock(log, False)
        my_roadblock.set_uuid("%d:%s" % (attempts, uuid))
        my_roadblock.set_role("follower")
        my_roadblock.set_follower_id(follower_id)
        my_roadblock.set_leader_id(leader)
        my_roadblock.set_timeout(timeout)
        my_roadblock.set_redis_server(redis_server)
        my_roadblock.set_redis_password(redis_password)
        my_roadblock.set_abort(abort)
        my_roadblock.set_message_log(msgs_log_file)
        my_roadblock.set_user_messages(messages)
        if not wait_for is None:
            my_roadblock.set_wait_for_cmd(wait_for)
            my_roadblock.set_wait_for_log(wait_for_log)

        rc = my_roadblock.run_it()
        result_log_msg = "[%s] Roadblock resulted in return code %d" % (label, rc)
        if rc != 0:
            log.error(result_log_msg)
        else:
            log.info(result_log_msg)

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

def prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, roadblock_name):
    """
    Prepare queued messages for distribution via roadblock

    Args:
        iteration_sample_dir (str): The directory where the iteration sample's files are stored
        engine_tx_msgs_dir (str): Where to write messages to send
        roadblock_name (str): The name of the roadblock that the messages should be sent for

    Globals:
        None

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

def evaluate_roadblock(quit, abort, roadblock_name, roadblock_rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures):
    """
    Evaluate the status of a completed roadblock and it's affect on the test

    Args:
        quit (bool): Should the entire test be quit
        abort (bool): Should the iteration be aborted
        roadblock_name (str): The name of the roadblock being evaluated
        iteration_sample (dict): The data structure representing the specific iteration sample being evaluated
        engine_rx_msgs_dir (str): Where to look for received messages
        max_sample_failures (int): The maximum number of sample failures that an iteration can have before it fails

    Globals:
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

            if iteration_sample["failures"] >= max_sample_failures:
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

def setup_logger(log_level):
    """
    Setup the logging infrastructure that is used for everything except validation

    Args:
        log_level (str): the logging level to use

    Globals:
        None

    Returns:
        a logging instance
    """
    log_format = '[LOG %(asctime)s %(levelname)s %(module)s %(funcName)s:%(lineno)d] %(message)s'
    match log_level:
        case "debug":
            logging.basicConfig(level = logging.DEBUG, format = log_format, stream = sys.stdout)
        case "normal" | _:
            logging.basicConfig(level = logging.INFO, format = log_format, stream = sys.stdout)

    return logging.getLogger(__file__)

def process_roadblocks(callbacks = None, roadblock_id = None, endpoint_label = None, endpoint_deploy_timeout = None, max_roadblock_attempts = None, roadblock_password = None, new_followers = None, roadblock_messages_dir = None, roadblock_timeouts = None, max_sample_failures = None, engine_commands_dir = None, endpoint_dir = None):
    """
    Process the beginning and ending roadblocks associated with synchronizing a test

    Args:
        callbacks (dict): A dictionary of callbacks to endpoint specific actions.  It can define:
                          engine-init
                          collect-sysinfo
                          test-start
                          test-stop
                          remote-cleanup
        roadblock_id (str): The base ID to use as part of a roadblock's name
        endpoint_label (str): The name of the calling endpoint
        endpoint_deploy_timeout (int): The computed timeout value for endpoint engine deployment
        max_roadblock_attempts (int): The maximum number of attempts that should be made for each roadblock
        roadblock_password (str): The password that roadblock uses to connect to it's server
        new_followers (list): A list of the new followers to inform the roadblock leader about
        roadblock_messages_dir (str): The directory where roadblock messages should be stored
        roadblock_timeouts (dict): The roadblock timeout values from rickshaw-settings
        max_sample_failures (int): The maximum number of times a sample can fail before the iteration is considered a failure
        engine_commands_dir (str): Directory where the engine commands can be found
        endpoint_dir (str): The base endpoint directory for storing endpoint specific information

    Globals:
        None

    Returns:
        0
    """
    log.info("Starting to process roadblocks")

    new_followers_msg_payload = {
        "new-followers": new_followers
    }
    new_followers_msg = create_roadblock_msg("all", "all", "user-object", new_followers_msg_payload)

    new_followers_msg_file = roadblock_messages_dir + "/new-followers.json"
    log.info("Writing new followers message to %s" % (new_followers_msg_file))
    with open(new_followers_msg_file, "w", encoding = "ascii") as new_followers_msg_file_fp:
        new_followers_msg_file_fp.write(dump_json(new_followers_msg))

    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "endpoint-deploy-begin",
                      timeout = endpoint_deploy_timeout,
                      messages = new_followers_msg_file,
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc
    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "endpoint-deploy-end",
                      timeout = endpoint_deploy_timeout,
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc

    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "engine-init-begin",
                      timeout = roadblock_timeouts["engine-start"],
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc
    callback = "engine-init"
    if callback in callbacks:
        log.info("Calling endpoint specified callback for '%s'" % (callback))
        engine_init_msgs = callbacks[callback]()
    else:
        log.info("Calling endpoint did not specify a callback for '%s'" % (callback))
    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "engine-init-end",
                      timeout = roadblock_timeouts["engine-start"],
                      messages = engine_init_msgs,
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc

    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "get-data-begin",
                      timeout = roadblock_timeouts["default"],
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc
    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "get-data-end",
                      timeout = roadblock_timeouts["default"],
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc

    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "collect-sysinfo-begin",
                      timeout = roadblock_timeouts["collect-sysinfo"],
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc
    callback = "collect-sysinfo"
    if callback in callbacks:
        log.info("Calling endpoint specified callback for '%s'" % (callback))
        engine_init_msgs = callbacks[callback]()
    else:
        log.info("Calling endpoint did not specify a callback for '%s'" % (callback))
    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "collect-sysinfo-end",
                      timeout = roadblock_timeouts["collect-sysinfo"],
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc

    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "start-tools-begin",
                      timeout = roadblock_timeouts["default"],
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc
    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "start-tools-end",
                      timeout = roadblock_timeouts["default"],
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc

    rc = process_bench_roadblocks(callbacks = callbacks,
                                  roadblock_id = roadblock_id,
                                  endpoint_label = endpoint_label,
                                  max_roadblock_attempts = max_roadblock_attempts,
                                  roadblock_password = roadblock_password,
                                  max_sample_failures = max_sample_failures,
                                  roadblock_messages_dir = roadblock_messages_dir,
                                  roadblock_timeouts = roadblock_timeouts,
                                  engine_commands_dir = engine_commands_dir,
                                  endpoint_dir = endpoint_dir)
    if rc != 0:
        return rc

    do_roadblock(roadblock_id = roadblock_id,
                 follower_id = endpoint_label,
                 label = "stop-tools-begin",
                 timeout = roadblock_timeouts["default"],
                 max_attempts = max_roadblock_attempts,
                 redis_password = roadblock_password,
                 msgs_dir = roadblock_messages_dir)
    do_roadblock(roadblock_id = roadblock_id,
                 follower_id = endpoint_label,
                 label = "stop-tools-end",
                 timeout = roadblock_timeouts["default"],
                 max_attempts = max_roadblock_attempts,
                 redis_password = roadblock_password,
                 msgs_dir = roadblock_messages_dir)

    do_roadblock(roadblock_id = roadblock_id,
                 follower_id = endpoint_label,
                 label = "send-data-begin",
                 timeout = roadblock_timeouts["default"],
                 max_attempts = max_roadblock_attempts,
                 redis_password = roadblock_password,
                 msgs_dir = roadblock_messages_dir)
    do_roadblock(roadblock_id = roadblock_id,
                 follower_id = endpoint_label,
                 label = "send-data-end",
                 timeout = roadblock_timeouts["default"],
                 max_attempts = max_roadblock_attempts,
                 redis_password = roadblock_password,
                 msgs_dir = roadblock_messages_dir)

    do_roadblock(roadblock_id = roadblock_id,
                 follower_id = endpoint_label,
                 label = "endpoint-cleanup-begin",
                 timeout = roadblock_timeouts["default"],
                 max_attempts = max_roadblock_attempts,
                 redis_password = roadblock_password,
                 msgs_dir = roadblock_messages_dir)
    callback = "remote-cleanup"
    if callback in callbacks:
        log.info("Calling endpoint specified callback for '%s'" % (callback))
        engine_init_msgs = callbacks[callback]()
    else:
        log.info("Calling endpoint did not specify a callback for '%s'" % (callback))
    do_roadblock(roadblock_id = roadblock_id,
                 follower_id = endpoint_label,
                 label = "endpoint-cleanup-end",
                 timeout = roadblock_timeouts["default"],
                 max_attempts = max_roadblock_attempts,
                 redis_password = roadblock_password,
                 msgs_dir = roadblock_messages_dir)

    return 0

def process_bench_roadblocks(callbacks = None, roadblock_id = None, endpoint_label = None, max_roadblock_attempts = None, roadblock_password = None, max_sample_failures = None, roadblock_messages_dir = None, roadblock_timeouts = None, engine_commands_dir = None, endpoint_dir = None):
    """
    Handle the running and evaluation of roadblocks while looping through the iterations and samples

    Args:
        callbacks (dict): A dictionary of callbacks to endpoint specific actions.  It can define:
                          engine-init
                          collect-sysinfo
                          test-start
                          test-stop
                          remote-cleanup
        roadblock_id (str): The base ID to use as part of a roadblock's name
        endpoint_label (str): The name of the calling endpoint
        max_roadblock_attempts (int): The maximum number of attempts that should be made for each roadblock
        roadblock_password (str): The password that roadblock uses to connect to it's server
        max_sample_failures (int): The maximum number of times a sample can fail before the iteration is considered a failure
        roadblock_messages_dir (str): The directory where roadblock messages should be stored
        roadblock_timeouts (dict): The roadblock timeout values from rickshaw-settings
        engine_commands_dir (str): Directory where the engine commands can be found
        endpoint_dir (str): The base endpoint directory for storing endpoint specific information

    Globals:
        log: a logger instance

    Returns:
        0
    """
    log.info("Starting to process benchmark roadblocks")

    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "setup-bench-begin",
                      timeout = roadblock_timeouts["default"],
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc

    iteration_sample_data = []

    log.info("Initializing data structures")
    with open(engine_commands_dir + "/client/1/start") as bench_cmds_fp:
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

    rc = do_roadblock(roadblock_id = roadblock_id,
                      follower_id = endpoint_label,
                      label = "setup-bench-end",
                      timeout = roadblock_timeouts["default"],
                      max_attempts = max_roadblock_attempts,
                      redis_password = roadblock_password,
                      msgs_dir = roadblock_messages_dir)
    if rc != 0:
        return rc

    quit = False
    abort = False
    timeout = roadblock_timeouts["default"]
    current_test = 0

    for iteration_sample_idx,iteration_sample in enumerate(iteration_sample_data):
        if quit:
            break

        current_test += 1

        iteration_sample_dir = "%s/iteration-%d/sample-%d" % (endpoint_dir, iteration_sample["iteration-id"], iteration_sample["sample-id"])
        engine_msgs_dir = "%s/msgs" % (iteration_sample_dir)
        engine_tx_msgs_dir = "%s/tx" % (engine_msgs_dir)
        engine_rx_msgs_dir = "%s/rx" % (engine_msgs_dir)
        log.info("Creating iteration+sample directories")
        for current_dir in [ iteration_sample_dir, engine_msgs_dir, engine_tx_msgs_dir, engine_rx_msgs_dir ]:
            my_make_dirs(current_dir)

        abort = False
        while not quit and not abort and not iteration_sample["complete"] and iteration_sample["failures"] < max_sample_failures:
            iteration_sample["attempt-fail"] = 0
            iteration_sample["attempt-num"] += 1

            log.info("Starting iteration %d sample %d (test %d of %d) attempt number %d of %d" %
                     (
                         iteration_sample["iteration-id"],
                         iteration_sample["sample-id"],
                         current_test,
                         len(iteration_sample_data),
                         iteration_sample["attempt-num"],
                         max_sample_failures
                     ))

            rb_name = None
            test_id = "%d-%d-%d" % (iteration_sample["iteration-id"], iteration_sample["sample-id"], iteration_sample["attempt-num"])
            rb_prefix = "%s:" % (test_id)

            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "infra-start-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)

            rb_name = "%s%s" % (rb_prefix, "infra-start-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "server-start-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)

            rb_name = "%s%s" % (rb_prefix, "server-start-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "endpoint-start-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)

            callback = "test-start"
            if callback in callbacks:
                log.info("Calling endpoint specified callback for '%s'" % (callback))
                engine_init_msgs = callbacks[callback](roadblock_messages_dir, test_id, engine_tx_msgs_dir)
            else:
                log.info("Calling endpoint did not specify a callback for '%s'" % (callback))

            rb_name = "%s%s" % (rb_prefix, "endpoint-start-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "client-start-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)

            msgs_log_file = roadblock_messages_dir + "/" + rb_name + ".json"
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
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)
            ####################################################################
            if timeout != roadblock_timeouts["default"]:
                timeout = roadblock_timeouts["default"]
                log.info("Resetting timeout value: %s" % (timeout))

            rb_name = "%s%s" % (rb_prefix, "client-stop-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)

            rb_name = "%s%s" % (rb_prefix, "client-stop-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "endpoint-stop-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)

            rb_name = "%s%s" % (rb_prefix, "endpoint-stop-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "server-stop-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)

            callback = "test-stop"
            if callback in callbacks:
                log.info("Calling endpoint specified callback for '%s'" % (callback))
                engine_init_msgs = callbacks[callback]()
            else:
                log.info("Calling endpoint did not specify a callback for '%s'" % (callback))

            rb_name = "%s%s" % (rb_prefix, "server-stop-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)
            ####################################################################
            rb_name = "%s%s" % (rb_prefix, "infra-stop-begin")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)

            rb_name = "%s%s" % (rb_prefix, "infra-stop-end")
            user_msgs_file = prepare_roadblock_user_msgs_file(iteration_sample_dir, engine_tx_msgs_dir, rb_name)
            rc = do_roadblock(roadblock_id = roadblock_id,
                              follower_id = endpoint_label,
                              label = rb_name,
                              timeout = timeout,
                              messages = user_msgs_file,
                              max_attempts = max_roadblock_attempts,
                              redis_password = roadblock_password,
                              msgs_dir = roadblock_messages_dir)
            quit,abort = evaluate_roadblock(quit, abort, rb_name, rc, iteration_sample, engine_rx_msgs_dir, max_sample_failures)
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
                         max_sample_failures,
                         sample_result
                     ))

    log.info("Final summary of iteration sample data:\n%s" % (dump_json(iteration_sample_data)))

    return 0

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
