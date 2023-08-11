#!/usr/bin/python3

import argparse
import datetime
import json
import os
import lzma
import platform
import re
import subprocess
import sys
import tempfile

def collect_sysinfo(RUNTIME_PADDING):
    """Collects sysinfo using the packrat tool."""
    packrat_bin = subprocess.run(["command", "-v", "packrat"], capture_output=True, text=True).stdout.strip()
    sysinfo_dir = "/path/to/sysinfo/dir"

    print()
    print("Collecting sysinfo")

    if packrat_bin:
        print("Running packrat...")
        packrat_process = subprocess.run([packrat_bin, sysinfo_dir], timeout=RUNTIME_PADDING)
        packrat_rc = packrat_process.returncode
        print("Packrat is finished")

        print(f"Contents of {sysinfo_dir}:")
        subprocess.run(["ls", "-l", sysinfo_dir])

        if packrat_rc != 0:
            exit_error("Critical error encountered by packrat during sysinfo collection", "collect-sysinfo-end")
    else:
        print("Packrat is not available")

    print()

def do_roadblock(options, label, timeout, roadblock_msgs_dir, roadblock_bin, rb_exit_success, rb_exit_abort, max_rb_attempts, messages=None, wait_for=None, abort=False):
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
    subprocess.run(["ping", "-w", "10", "-c", "4", options.rickshaw_host])

    # Append the necessary arguments to the cmd string
    cmd += f" {roadblock_bin} --role={role} --redis-server={options.rickshaw_host}"
    cmd += f" --leader-id={leader} --timeout={timeout} --redis-password={options.roadblock_passwd}"
    cmd += f" --follower-id={options.cs_label} --message-log={msgs_log_file}"

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
    uuid = f"{options.roadblock_id}:{label}"

    # Print info about the roadblock
    print("\n\n")
    print(f"Starting roadblock [{datetime.datetime.now()}]")
    print(f"server: {options.rickshaw_host}")
    print(f"role: {role}")
    print(f"uuid (without attempt ID embedded): {uuid}")
    print(f"timeout: {timeout}")

    # Initialize the number of attempts and the exit code
    attempts = 0
    rc = 99

    # Loop until either the maximum number of attempts is reached or the roadblock is successful or aborted
    while attempts < int(max_rb_attempts) and rc != rb_exit_success and rc != rb_exit_abort:
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

def exit_error(msg):
    """
    Prints an error message and exits the script with an error code of 1.
    Exits without sending any roadblock message.
    """
    print(f"[ERROR]engine-script-library: {msg}\n")
    print("Exiting")
    sys.exit(1)

def get_data(cs_type, cs_id, ssh_id_file, engine_config_dir, tool_cmds_dir):
    """Retrieves files required to run a benchmark and tools for a distributed system from a controller node to worker nodes.

    Args:
        cs_type (str): The type of node to retrieve files for, e.g., client, server, profiler, worker, master.
        cs_id (str): The ID of the node to retrieve files for.
        ssh_id_file (str): The path to the SSH identity file to use for authentication.
        engine_config_dir (str): The path to the directory containing the engine configuration files.
        tool_cmds_dir (str): The path to the directory containing the tool commands.

    Returns:
        None.
    """
    # Construct the file name
    if cs_type == "client" or cs_type == "server" or cs_type == "profiler":
        cs_files_list = f"{cs_type}-{cs_id}-files-list"
    else:
        cs_files_list = f"{cs_type}-files-list"

    # Retrieve the files
    cmd = f"scp -i {ssh_id_file} controller:{engine_config_dir}/{cs_files_list} {cs_files_list}"
    subprocess.run(cmd, check=True, shell=True)

    with open(cs_files_list) as f:
        for line in f:
            if "src=" in line:
                src_file = line.strip().split("=")[1]
            elif "dest=" in line:
                dest_file = line.strip().split("=")[1]
                cmd = f"scp -i {ssh_id_file} controller:{src_file} {dest_file}"
                subprocess.run(cmd, check=True, shell=True)

    # Retrieve the benchmark and tool commands
    if cs_type == "client" or cs_type == "server":
        cmd = f"scp -i {ssh_id_file} controller:{engine_config_dir}/bench-cmds/{cs_type}/{cs_id}/start bench-start-cmds"
        subprocess.run(cmd, check=True, shell=True)
        if cs_type == "client":
            cmd = f"scp -i {ssh_id_file} controller:{engine_config_dir}/bench-cmds/{cs_type}/{cs_id}/infra bench-infra-cmds"
            subprocess.run(cmd, check=True, shell=True)
            if cs_id == "1":
                cmd = f"scp -i {ssh_id_file} controller:{engine_config_dir}/bench-cmds/{cs_type}/{cs_id}/runtime bench-runtime-cmds"
                subprocess.run(cmd, check=True, shell=True)
        else:
            cmd = f"scp -i {ssh_id_file} controller:{engine_config_dir}/bench-cmds/{cs_type}/{cs_id}/stop bench-stop-cmds"
            subprocess.run(cmd, check=True, shell=True)
    else:
        cmd = f"scp -i {ssh_id_file} controller:{engine_config_dir}/bench-cmds/client/1/start bench-start-cmds"
        subprocess.run(cmd, check=True, shell=True)

    cmd = f"scp -i {ssh_id_file} controller:{tool_cmds_dir}/{cs_id}/start tool-start-cmds"
    subprocess.run(cmd, check=True, shell=True)
    cmd = f"scp -i {ssh_id_file} controller:{tool_cmds_dir}/{cs_id}/stop tool-stop-cmds"
    subprocess.run(cmd, check=True, shell=True)

def process_bench_roadblocks(cs_type, test_config, total_tests):
    """Process the benchmark roadblocks for the given test.

    Args:
        cs_type (str): The type of the benchmark test, which can be either "single_client" or "multi_client".
        test_config (dict): A dictionary that contains the configuration settings for the benchmark test.
        total_tests (int): The total number of benchmark tests to run.

    Raises:
        ValueError: If the value of "cs_type" is not "single_client" or "multi_client".
        Exception: If any of the shell commands executed during the benchmark test returns a non-zero exit code.

    """
    # Define some constants used in the function
    timeout_padding = 10
    default_timeout = test_config["timeout"]
    runtime_padding = test_config["runtime_padding"]

    # Verify that the value of cs_type is valid
    if cs_type not in ["single_client", "multi_client"]:
        raise ValueError("cs_type must be either 'single_client' or 'multi_client'")

    # Loop through each test iteration and sample in the test configuration
    for iter_data in test_config["iterations"]:
        iter_id = iter_data["iteration_id"]
        iter_samp_dir = os.path.join(test_config["test_dir"], f"iteration_{iter_id}")
        os.makedirs(iter_samp_dir, exist_ok=True)

        for samp_data in iter_data["samples"]:
            samp_id = samp_data["sample_id"]
            iter_array_idx = (iter_id - 1) * len(iter_data["samples"]) + (samp_id - 1)
            sample_data_complete = test_config["sample_data_complete"]
            sample_data_attempt_num = test_config["sample_data_attempt_num"]
            sample_data_attempt_fail = test_config["sample_data_attempt_fail"]
            sample_data_attempt_stop = test_config["sample_data_attempt_stop"]
            sample_data_iteration_id = test_config["sample_data_iteration_id"]
            sample_data_sample_id = test_config["sample_data_sample_id"]
            sample_data_cs_id = test_config["sample_data_cs_id"]
            sample_data_endpoint_id = test_config["sample_data_endpoint_id"]
            sample_data_attempt_start_time = test_config["sample_data_attempt_start_time"]

            # Check if this sample has already been completed
            if sample_data_complete[iter_array_idx]:
                print(f"Sample {iter_id}-{samp_id} has already been completed")
                continue

            print(f"Starting iteration {iter_id} sample {samp_id} (test {total_tests} of {total_tests}) attempt number {sample_data_attempt_num[iter_array_idx]+1} of {test_config['max_sample_failures']}")

            # Prepare the user messages file for the roadblocks
            user_msgs_file = os.path.join(iter_samp_dir, "user_msgs.json")

            # Set the roadblock name prefix
            test_id = f"{sample_data_iteration_id[iter_array_idx]}-{sample_data_sample_id[iter_array_idx]}-{sample_data_attempt_num[iter_array_idx]}"
            rb_prefix = f"{test_id}:"

            force_server_stop = 0
            abort = 0
            quit = 0
            unbounded_roadblock = "no"
            tmp_timeout = None

            # Process the infra-start-begin roadblock
            rb_name = f"{rb_prefix}infra-start-begin"
            prepare_roadblock_user_msgs_file(iter_samp_dir, rb_name)
            do_roadblock(rb_name, default_timeout, "messages", user_msgs)

def process_opts():
    """
    Parses command line arguments.
    """
    parser = argparse.ArgumentParser(description='Crucible run arguments')
    parser.add_argument('--base-run-dir', dest='base_run_dir', help='Description of base-run-dir argument')
    parser.add_argument('--cs-label', dest='cs_label', help='Description of cs-label argument')

    parser.add_argument('--cpu-partitioning', dest='cpu-partitioning', help='TODO')
    parser.add_argument('--cpu-partitions', dest='cpu-partitions', help='TODO')
    parser.add_argument('--cpu-partition-index', dest='cpu-partition-index', help='TODO')

    parser.add_argument('--disable-tools', dest='disable_tools', help='Description of disable-tools argument')


    parser.add_argument('--endpoint', dest='endpoint', help='Description of endpoint argument')
    parser.add_argument('--endpoint-run-dir', dest='endpoint_run_dir', help='Description of endpoint-run-dir argument')

    parser.add_argument('--engine-script-start-timeout', dest='engine_script_start_timeout', help='Description of engine-script-start-timeout argument')

    parser.add_argument('--max-sample-failures', dest='max_sample_failures', help='Description of max-sample-failures argument')
    parser.add_argument('--max-rb-attempts', dest='max_rb_attempts', help='Description of max-rb-attempts argument')

    parser.add_argument('--osruntime', dest='osruntime', help='Description of osruntime argument')

    parser.add_argument('--rickshaw-host', dest='rickshaw_host', help='Description of rickshaw-host argument')

    parser.add_argument('--roadblock-passwd', dest='roadblock_passwd', help='Description of roadblock-passwd argument')
    parser.add_argument('--roadblock-id', dest='roadblock_id', help='Description of roadblock-id argument')

    parser.add_argument('--', dest='unknown', help='An empty param generated by bootstrap')

    return parser.parse_args()

def remove_quotes(s):
    if s.startswith("'"):
        s = s[1:]
    if s.endswith("'"):
        s = s[:-1]
    return s

def setup_core_env(cs_label, base_run_dir):
    """
    Sets several environment variables, creates several directories on the client/server and the controller.

    Args:
        cs_label (str): Label for the client/server
        base_run_dir (str): Base run directory for the controller

    Returns:
        cs_dir
    """
    os.environ["RS_CS_LABEL"] = cs_label
    cs_type = cs_label.split("-")[0]
    cs_id = cs_label.split("-")[1]

    # Directories on the client/server
    cs_dir = tempfile.mkdtemp()
    print(f"cs_dir: {cs_dir}")
    tool_start_cmds = os.path.join(cs_dir, "tool-start")
    tool_stop_cmds = os.path.join(cs_dir, "tool-stop")
    roadblock_msgs_dir = os.path.join(cs_dir, "roadblock-msgs")
    os.makedirs(roadblock_msgs_dir)
    sysinfo_dir = os.path.join(cs_dir, "sysinfo")
    os.makedirs(sysinfo_dir)

    # Directories on the controller
    config_dir = os.path.join(base_run_dir, "config")
    engine_config_dir = os.path.join(config_dir, "engine")
    tool_cmds_dir = os.path.join(config_dir, f"tool-cmds/{cs_type}")
    run_dir = os.path.join(base_run_dir, "run")
    archives_dir = os.path.join(run_dir, "engine/archives")
    sync_prefix = "engine"
    sync = f"{sync_prefix}-script-start"

    return cs_dir, tool_start_cmds, tool_stop_cmds, roadblock_msgs_dir, engine_config_dir, tool_cmds_dir

def start_tools():
    """
    Starts a set of tools specified in a configuration file.

    The function checks if the `disable_tools` flag is set to 1, in which case
    it skips starting the tools. Otherwise, it checks if the tool start and stop
    command files exist and then reads the tool names and corresponding commands
    from the start command file.

    For each tool, it creates a directory with the tool name and executes the tool
    command within that directory. The function captures the return code from the
    command and logs the output.

    Finally, it prints a message indicating whether any tools were started or not.
    """
    print("running start_tools()")

    # Make the tool-data directory if it doesn't exist
    os.makedirs("tool-data", exist_ok=True)

    # Check if tools are disabled
    if os.environ.get("disable_tools") == "1":
        print("Not starting tools because --disable-tools=1 was used")
        return

    # Check if tool start and stop command files exist
    tool_start_cmds = os.environ.get("tool_start_cmds")
    tool_stop_cmds = os.environ.get("tool_stop_cmds")
    if not os.path.exists(tool_start_cmds) or not os.path.exists(tool_stop_cmds):
        exit_error(f"Tool cmd file {tool_start_cmds} was not found", "start-tools-end")
        return

    # Start the tools
    total_tools = 0
    with open(tool_start_cmds, "r") as f:
        for line in f:
            # Get the tool name and command
            tool_name, tool_cmd = line.strip().split(":", 1)
            total_tools += 1

            # Create the tool directory
            os.makedirs(tool_name, exist_ok=True)

            # Change to the tool directory and execute the command
            try:
                os.chdir(tool_name)
                print(f"Starting tool '{tool_name}' with command '{tool_cmd}'")
                os.system(tool_cmd)
                tool_cmd_rc = 0
                print(f"Starting tool '{tool_name}' with command '{tool_cmd}' returned {tool_cmd_rc}")
            except Exception as e:
                exit_error(f"start_tools: Failed to start tool '{tool_name}' with error: {e}", "start-tools-end")

    if total_tools == 0:
        print("No tools configured for this engine")
    else:
        print(f"Started {total_tools} tool(s)") 

def validate_core_env(options, ROADBLOCK_BIN):
    """
    Check that all necessary environment variables have been set, and log various
    configuration settings to the console. If any required variables are missing, the
    script will print an error message and exit with an error code.
    """
    print()
    print("core environment:")

    # Check that rickshaw_host environment variable is set
    rickshaw_host = options.rickshaw_host
    if rickshaw_host is None:
        exit_error("Exiting due to rickshaw host not being set")
    else:
        print(f"rickshaw_host={rickshaw_host}")

    # Check if roadblock_id and roadblock_passwd environment variables are set, and if so, check if the roadblock binary exists
    roadblock_id = options.roadblock_id
    roadblock_passwd = options.roadblock_passwd
    if roadblock_id is None or roadblock_passwd is None:
        print("Cannot use roadblock for synchronization because an ID or password was not provided")
        USE_ROADBLOCK = False
    else:
        USE_ROADBLOCK = True
        if not os.path.exists(ROADBLOCK_BIN):
            exit_error(f"Could not find roadblock binary: {ROADBLOCK_BIN}")

    # Check that cs_label environment variable is set and matches a certain regular expression
    cs_label = options.cs_label
    cs_label = cs_label.replace("'", "") # remove single quotes from cs_label
    if cs_label is None:
        exit_error("The client/server label (--cs-label) was not defined")
    else:
        match = re.match(r'^(\w+)-\d+$', cs_label)
        if match is None:
            exit_error(f'cs_label "{cs_label}" does not adhere to regex /^(\w+)-\d+$/')
        else:
            print(f'engine-label "{cs_label}" is valid')

    # Check that max_rb_attempts environment variable is set, and if not, set it to a default value
    max_rb_attempts = options.max_rb_attempts
    max_rb_attempts = max_rb_attempts.replace("'", "")
    if max_rb_attempts is None:
        max_rb_attempts = 1
        print(f'[WARNING] --max-rb-attempts was not used, so setting to default of {max_rb_attempts}')
    else:
        print(f"max_rb_attempts={max_rb_attempts}")

    # Check that max_sample_failures environment variable is set, and if not, set it to a default value
    max_sample_failures = options.max_sample_failures
    if max_sample_failures is None:
        max_sample_failures = 3
        print(f'[WARNING] --max-sample-failures was not used, so setting to default of {max_sample_failures}')
    else:
        print(f"max_sample_failures={max_sample_failures}")

    # Check that endpoint_run_dir is set
    endpoint_run_dir = options.endpoint_run_dir
    if endpoint_run_dir is None:
        exit_error("The endpoint run directory (--endpoint-run-dir) was not defined")
    else:
        print(f"endpoint_run_dir={endpoint_run_dir}")

    engine_script_start_timeout = options.engine_script_start_timeout
    print(f"engine_script_start_timeout: {engine_script_start_timeout}")

    return max_rb_attempts

def main(*args):
    options = process_opts()
    print('engine-script options:')
    print(options)

    # Configuration settings
    ROADBLOCK_BIN = "/usr/local/bin/roadblock.py"
    USE_ROADBLOCK = True
    RB_EXIT_SUCCESS = 0
    RB_EXIT_TIMEOUT = 3
    RB_EXIT_ABORT = 4
    RB_EXIT_INPUT = 2
    RUNTIME_PADDING = 180
    ABORT = 0

    # knocking out for now
    """
    # source /etc/profile if exists
    if os.path.exists('/etc/profile'):
        exec(open('/etc/profile').read())
    """

    # modify PATH variable
    # Depending on how engine-script is started, "/usr/local/bin" is not
    # always in the $PATH
    os.environ['PATH'] = f"/usr/local/bin:{os.getenv('PATH')}"

    print("engine-script env:")
    print(os.environ)

    print("engine-script params:")
    print(sys.argv[1:])

    print("\nos-release:")
    with open("/etc/os-release") as file:
        print(file.read())

    print("\nuname:")
    print(platform.uname())

    version = "20200509"
    print(f"version: {version}\n")

    leader = "controller"
    #default_timeout = load_default_timeout()

    # load Rickshaw settings
    #with open('/tmp/rickshaw-settings.json', 'r') as f:
    #    rickshaw_opts = json.load(f)
    with lzma.open('/tmp/rickshaw-settings.json.xz', 'rt') as f:
        rickshaw_opts = json.load(f)

    # process Rickshaw options
    default_rickshaw_timeout = rickshaw_opts['roadblock']['timeouts']['default']

    # validate core environment
    max_rb_attempts = validate_core_env(options, ROADBLOCK_BIN)

    # setup core environment
    base_run_dir = options.base_run_dir
    cs_label = options.cs_label
    cs_dir, tool_start_cmds, tool_stop_cmds, roadblock_msgs_dir, engine_config_dir, tool_cmds_dir = setup_core_env(cs_label, base_run_dir)

    # cs_type and cs_id can have leading or trailing single-quotes
    cs_type = remove_quotes(cs_type)
    cs_id = remove_quotes(cs_id)
    # roadblocks may be used after this

    # cs_dir is created by setup_core_env
    if not os.path.exists(cs_dir):
        exit_error(f"Could not chdir to {cs_dir}", "engine-init-begin")

    if os.getenv('cpu_partitioning') == "1":
        if not os.getenv('HK_CPUS'):
            exit_error("cpu-partitioning is enabled but HK_CPUS is empty", "engine-init-begin")

        if not os.getenv('WORKLOAD_CPUS'):
            exit_error("cpu-partitioning is enabled but WORKLOAD_CPUS is empty", "engine-init-begin")

    engine_script_start_timeout = options.engine_script_start_timeout

    do_roadblock(options, 'engine-init-begin', engine_script_start_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)
    do_roadblock(options, 'engine-init-end', engine_script_start_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)

    # Get data
    do_roadblock(options, 'get-data-begin', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)

    # get data
    os.environ["RS_CS_LABEL"] = cs_label
    cs_type = cs_label.split("-")[0]
    cs_id = cs_label.split("-")[1]
    ssh_id_file='/tmp/rickshaw_id.rsa'
    # engine_config_dir=
    # tool_cmds_dir=


    get_data(cs_type, cs_id, ssh_id_file, engine_config_dir, tool_cmds_dir)

    do_roadblock(options, 'get-data-end', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)


    # Collect sysinfo
    do_roadblock(options, 'collect-sysinfo-begin', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)

    collect_sysinfo(RUNTIME_PADDING)

    do_roadblock(options, 'collect-sysinfo-end', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)


    # Start tools
    do_roadblock(options, 'start-tools-begin', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)

    start_tools()

    do_roadblock(options, 'start-tools-end', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)


    # Process bench roadblocks
    test_config = {} # TODO: fix placeholder
    total_tests = 1 # TODO: fix placeholder
    process_bench_roadblocks(cs_type, test_config, total_tests)


    # Stop tools
    do_roadblock(options, 'stop-tools-begin', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)
    do_roadblock(options, 'stop-tools-end', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts, wait_for="/usr/local/bin/engine-script-library stop_tools '$(pwd)' '${tool_stop_cmds}' '${disable_tools}'")
    # wait-for "/usr/local/bin/engine-script-library stop_tools '$(pwd)' '${tool_stop_cmds}' '${disable_tools}'"


    # Send data
    do_roadblock(options, 'send-data-begin', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts)
    do_roadblock(options, 'send-data-end', default_rickshaw_timeout, roadblock_msgs_dir, ROADBLOCK_BIN, RB_EXIT_SUCCESS, RB_EXIT_ABORT, max_rb_attempts, wait_for="/usr/local/bin/engine-script-library send_data '${ssh_id_file}' '${cs_dir}' '${rickshaw_host}' '${archives_dir}/${cs_label}-data.tgz'")


    print('All client/server scripts are finished')

    # TODO: /bin/rm -rf $cs_dir

if __name__ == '__main__':
    main()
