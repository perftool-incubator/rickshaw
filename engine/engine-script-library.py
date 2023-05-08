#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
import sys
import tempfile

# global variables
roadblock_bin = "/usr/local/bin/roadblocker.py"
use_roadblock = 1
rb_exit_success = 0
rb_exit_timeout = 3
rb_exit_abort = 4
rb_exit_input = 2
runtime_padding = 180
abort = 0

# Exits without sending any roadblock message
def exit_error(msg, sync, leader):
    print(f"[ERROR]engine-script-library: {msg}\n", file=sys.stderr)
    print("Exiting", file=sys.stderr)
    sys.exit(1)

# Sends abort message on roadblock but does not exit
def abort_error(msg, sync):
    print(f"[ERROR]engine-script-library: {msg}\n", file=sys.stderr)
    msg_file = tempfile.mkstemp()[1]
    with open(msg_file, "w") as f:
        f.write(f'[{{"recipient":{{"type":"all","id":"all"}},"user-object":{{"error":"{msg}"}}}}]')
    
    do_roadblock(sync, 300, "abort", "messages", msg_file)
    global abort
    abort = 1

def do_roadblock(label, timeout, messages=None, wait_for=None, abort=False):
    """
    Executes a roadblock with the specified label, timeout, and optional parameters for sending a user message file,
    waiting for a particular condition, and aborting the roadblock.

    :param label: A label for the roadblock
    :param timeout: The timeout value for the roadblock
    :param messages: (Optional) A path to a file containing user messages to send
    :param wait_for: (Optional) A command to wait for before proceeding with the roadblock
    :param abort: (Optional) If True, sends an abort message to the roadblock
    :return: The exit code of the roadblock command
    """

    # Set the leader for the roadblock to be the "controller"
    leader = "controller"
    
    wait_for_log = None

    # If messages is not None, print a message indicating that the user message file is going to be sent
    if messages is not None:
        print("Going to send this user message file:", messages)

    # If wait_for is not None, print a message indicating that the wait-for command is going to be run
    # and set wait_for_log to a temporary file path
    if wait_for is not None:
        print("Going to run this wait-for command:", wait_for)
        wait_for_log = tempfile.mktemp()

    # Raise a ValueError if the label or timeout are not provided
    if label is None:
        raise ValueError("[ERROR]do_roadblock() label not provided")

    if timeout is None:
        raise ValueError("[ERROR]do_roadblock() timeout not provided")

    # Set the path for the user messages log file
    msgs_log_file = os.path.join(roadblock_msgs_dir, f"{label}.json")

    # Initialize the command string and set the role to "follower"
    cmd = ""
    role = "follower"

    # Use subprocess to run a "ping" command to the rickshaw host
    subprocess.run(["ping", "-w", "10", "-c", "4", rickshaw_host])

    # Append the necessary arguments to the cmd string
    cmd += f" {roadblock_bin} --role={role} --redis-server={rickshaw_host}"
    cmd += f" --leader-id={leader} --timeout={timeout} --redis-password={roadblock_passwd}"
    cmd += f" --follower-id={cs_label} --message-log={msgs_log_file}"

    # If messages is not None, append the user message argument to the cmd string
    if messages is not None:
        cmd += f" --user-message {messages}"

    # If wait_for is not None, append the wait-for argument and wait_for_log argument to the cmd string
    if wait_for is not None:
        cmd += f" --wait-for \"{wait_for}\""
        cmd += f" --wait-for-log {wait_for_log}"

    # If abort is True, append the abort argument to the cmd string
    if abort:
        cmd += " --abort"

    # Set the uuid to be the concatenation of the roadblock_id and the label
    uuid = f"{roadblock_id}:{label}"

    # Print info about the roadblock
    print("\n\n")
    print(f"Starting roadblock [{datetime.datetime.now()}]")
    print(f"server: {rickshaw_host}")
    print(f"role: {role}")
    print(f"uuid (without attempt ID embedded): {uuid}")
    print(f"timeout: {timeout}")

    # Initialize the number of attempts and the exit code
    attempts = 0
    rc = 99

    # Loop until either the maximum number of attempts is reached or the roadblock is successful or aborted
    while attempts < max_rb_attempts and rc != rb_exit_success and rc != rb_exit_abort:
        attempts += 1
        print(f"attempt number: {attempts}")
        print(f"uuid: {attempts}:{uuid}")
        rb_cmd = f"{cmd} --uuid={attempts}:{uuid}"
        print("going to run this roadblock command:", rb_cmd)
        print("roadblock output BEGIN")

        # Use subprocess to run the roadblock command and capture the exit code
        # Set shell=True to interpret the command as a string instead of a list
        rc = subprocess.run(rb_cmd, shell=True).returncode
        print("roadblock output END")
        print(f"roadblock exit code: {rc}")

        # If the user messages log file exists, print its contents
        if os.path.isfile(msgs_log_file):
            print("# start messages from roadblock ################################################")
            with open(msgs_log_file, "r") as msgs_file:
                print(msgs_file.read())
            print("# end messages from roadblock ##################################################")
        else:
            print("# no messages from roadblock ###################################################")

        # If the wait-for log file exists, print its contents and delete the file
        if wait_for_log is not None and os.path.isfile(wait_for_log):
            print("# start wait-for log ###########################################################")
            with open(wait_for_log, "r") as wait_for_file:
                print(wait_for_file.read())
            print("# end wait-for log #############################################################")
            os.remove(wait_for_log)
        else:
            print("# no wait for log ##############################################################")

    # Print info about the completed roadblock
    print(f"total attempts: {attempts}")
    print(f"Completed roadblock [{datetime.datetime.now()}]")
    print("\n\n")

    # Return the exit code of the roadblock command
    print(f"returning {rc}")
    return rc

# TODO: function roadblock_exit_on_error()

# TODO: function archive_to_controller()

# TODO: function process_opts()

# TODO: function validate_core_env()

# TODO: function setup_core_env()

# TODO: function get_data()

# TODO: function collect_sysinfo()

# TODO: function start_tools()

# TODO: function prepare_roadblock_user_msgs_file()

# TODO: function evaluate_test_roadblock

# TODO: function run_bench_cmd()

# TODO: function process_bench_roadblocks()

# TODO: function stop_tools()

# TODO: function send_data()

# TODO: function load_json_settings()

# TODO: function direct_script_function_call()

# TODO: convert: direct_script_function_call "$@"
