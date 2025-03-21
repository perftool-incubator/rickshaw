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

def do_roadblock(label, timeout, *args):
    print(f"do_roadblock() ARGC: {len(args)}")
    print(f"do_roadblock() ARGS: {args}")

    leader = "controller"
    message = ""
    wait_for = ""
    wait_for_log = ""
    do_abort = 0

    arg_iter = iter(args)
    for arg in arg_iter:
        if arg == "messages":
            message = next(arg_iter)
            print(f"Going to send this user message file: {message}")
        elif arg == "wait-for":
            wait_for = next(arg_iter)
            wait_for_log = tempfile.mkstemp()[1]
            print(f"Going to run this wait-for command: {wait_for}")
            print(f"Going to log wait-for to this file: {wait_for_log}")
        elif arg == "abort":
            do_abort = 1
            print("Going to send an abort")
        else:
            exit_error(f"[ERROR]do_roadblock() encountered invalid optional parameter(s) {args}")

    if not message:
        print("Not going to send a user message file")
    if not wait_for:
        print("Not going to use wait-for")
    if not label:
        exit_error("[ERROR]do_roadblock() label not provided")
    if not timeout:
        exit_error("[ERROR]do_roadblock() timeout not provided")

    msgs_log_file = f"{roadblock_msgs_dir}/{label}.json"
    cmd = []
    role = "follower"

    # Replace rickshaw_host with the appropriate hostname or IP address
    rickshaw_host = "localhost"
    subprocess.run(["ping", "-w", "10", "-c", "4", rickshaw_host])

    cmd += [roadblock_bin, f"--role={role}", f"--redis-server={rickshaw_host}"]
    cmd += [f"--leader-id={leader}", f"--timeout={timeout}", f"--redis-password={roadblock_passwd}"]
    cmd += [f"--follower-id={cs_label}", f"--message-log={msgs_log_file}"]

    if message:
        cmd += ["--user-message", message]
    if wait_for:
        cmd += ["--wait-for", wait_for]
        cmd += ["--wait-for-log", wait_for_log]
    if do_abort:
        cmd += ["--abort"]

    uuid = f"{roadblock_id}:{label}"
    print("\n\n")
    print(f"Starting roadblock [`date`]")
    print(f"server: {rickshaw_host}")
    print(f"role: {role}")
    print(f"uuid: {uuid}")
    print(f"timeout: {timeout}")

    rc = 99
    rb_cmd = cmd + [f"--uuid={uuid}"]
    print(f"going to run this roadblock command: {' '.join(rb_cmd)}")
    print("roadblock output BEGIN")
    result = subprocess.run(rb_cmd)
    rc = result.returncode
    print("roadblock output END")
    print(f"roadblock exit code: {rc}")

    if os.path.isfile(msgs_log_file):
        print("# start messages from roadblock ################################################")
        with open(msgs_log_file, "r") as f:
            print(f.read())
        print("# end messages from roadblock ##################################################")
    else:
        print("# no messages from roadblock ###################################################")

    if wait_for_log and os.path.isfile(wait_for_log):
        print("# start wait-for log ###########################################################")
        with open(wait_for_log, "r") as f:
            print(f.read())
        print("# end wait-for log #############################################################")
        os.remove(wait_for_log)
    else:
        print("# no wait-for log ##############################################################")

    print("Completed roadblock [`date`]")
    print("\n\n")

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
