#!/usr/bin/python3

import argparse
import datetime
import json
import lzma
import os
import re
import subprocess
import sys
import tempfile

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

    return parser.parse_args()

def exit_error(msg):
    """
    Prints an error message and exits the script with an error code of 1.
    Exits without sending any roadblock message.
    """
    print(f"[ERROR]engine-script-library: {msg}\n")
    print("Exiting")
    sys.exit(1)

def remove_quotes(s):
    if s.startswith("'"):
        s = s[1:]
    if s.endswith("'"):
        s = s[:-1]
    return s

def remove_single_quotes(s):
    return s.replace("'", "")


def collect_sysinfo(options):
    """Collects sysinfo using the packrat tool."""
    packrat_bin = subprocess.run(["command", "-v", "packrat"], capture_output=True, text=True).stdout.strip()
    sysinfo_dir = "/path/to/sysinfo/dir"

    print()
    print("Collecting sysinfo")
    
    if packrat_bin:
        print("Running packrat...")
        packrat_process = subprocess.run([packrat_bin, sysinfo_dir], timeout=options['runtime_padding'])
        packrat_rc = packrat_process.returncode
        print("Packrat is finished")

        print(f"Contents of {sysinfo_dir}:")
        subprocess.run(["ls", "-l", sysinfo_dir])

        if packrat_rc != 0:
            exit_error("Critical error encountered by packrat during sysinfo collection", "collect-sysinfo-end")
    else:
        print("Packrat is not available")

def do_roadblock(options, label):
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

    # remove the single quotes from all used options
    engine_script_start_timeout = remove_single_quotes(options['engine_script_start_timeout'])
    if options['messages'] is not None:
        messages = remove_single_quotes(options['messages'])
    else:
        messages = None
    
    # vars = messages, wait_for, msgs_log_file, roadblock_msgs_dir, rickshaw_host, roadblock_bin, roadblock_passwd, cs_label, abort, roadblock_id, rb_exit_success, rb_exit_abort

    # for var in vars:
    #     if options['{var}'] is not None:
    #         var = remove_single_quotes(options{'var'})
    #     else:
    #         var = None

    # wait_for = remove_single_quotes(options['wait_for'])
    # msgs_log_file = remove_single_quotes(options['msgs_log_file'])
    # roadblock_msgs_dir = remove_single_quotes(options['roadblock_msgs_dir'])
    # rickshaw_host = remove_single_quotes(options['rickshaw_host'])
    # roadblock_bin = remove_single_quotes(options['roadblock_bin'])
    # roadblock_passwd = remove_single_quotes(options['roadblock_passwd'])
    # cs_label = remove_single_quotes(options['cs_label'])
    # abort = remove_single_quotes(options['abort'])
    # roadblock_id = remove_single_quotes(options['roadblock_id'])
    # rb_exit_success = remove_single_quotes(options['rb_exit_success'])
    # rb_exit_abort = remove_single_quotes(options['rb_exit_abort'])

    if options['wait_for'] is not None:
        wait_for = remove_single_quotes(options['wait_for'])
    else:
        wait_for = None
    
    """  if options['msgs_log_file'] is not None:
        msgs_log_file = remove_single_quotes(options['msgs_log_file'])
    else:
        msgs_log_file = None """

    if options['roadblock_msgs_dir'] is not None:
        roadblock_msgs_dir = remove_single_quotes(options['roadblock_msgs_dir'])
    else:
        roadblock_msgs_dir = None

    if options['rickshaw_host'] is not None:
        rickshaw_host = remove_single_quotes(options['rickshaw_host'])
    else:
        rickshaw_host = None

    if options['roadblock_bin'] is not None:
        roadblock_bin = remove_single_quotes(options['roadblock_bin'])
    else:
        roadblock_bin = None

    if options['roadblock_passwd'] is not None:
        roadblock_passwd = remove_single_quotes(options['roadblock_passwd'])
    else:
        roadblock_passwd = None

    if options['cs_label'] is not None:
        cs_label = remove_single_quotes(options['cs_label'])
    else:
        cs_label = None

    # abort is an int, does not have quotes to remove
    """  if options['abort'] is not None:
        abort = remove_single_quotes(options['abort'])
    else:
        abort = None """
    abort = options['abort']

    if options['roadblock_id'] is not None:
        roadblock_id = remove_single_quotes(options['roadblock_id'])
    else:
        roadblock_id = None

    # rb_exit_success if an int, does not have quotes to remove
    """ if options['rb_exit_success'] is not None:
        rb_exit_success = remove_single_quotes(options['rb_exit_success'])
    else:
        rb_exit_success = None """
    rb_exit_success = options['rb_exit_success']
    
    # rb_exit_abort is an int, does not have quotes to remove
    """ if options['rb_exit_abort'] is not None:
        rb_exit_abort = remove_single_quotes(options['rb_exit_abort'])
    else:
        rb_exit_abort = None """
    rb_exit_abort = options['rb_exit_abort']

    


    # Set the leader for the roadblock to be the "controller"
    leader = "controller"

    #timeout = options['engine_script_start_timeout']
    timeout = engine_script_start_timeout

    wait_for_log = None

    # If messages is not None, print a message indicating that the user message file is going to be sent
    #if options['messages'] is not None:
        #print("Going to send this user message file:", options['messages'])
    if messages is not None:
        print("Going to send this user message file:", messages)

    # If wait_for is not None, print a message indicating that the wait-for command is going to be run
    # and set wait_for_log to a temporary file path
    # if options['wait_for'] is not None:
    #     print("Going to run this wait-for command:", options['wait_for'])
    #     wait_for_log = tempfile.mktemp()
    if wait_for is not None:
        print("Going to run this wait-for command:", wait_for)
        wait_for_log = tempfile.mktemp()
    
    # Raise a ValueError if the label or timeout are not provided
    if label is None:
        raise ValueError("[ERROR]do_roadblock() label not provided")

    if timeout is None:
        raise ValueError("[ERROR]do_roadblock() timeout not provided")
    
    # Set the path for the user messages log file
    # options['msgs_log_file'] = os.path.join(options['roadblock_msgs_dir'], f"{label}.json")
    msgs_log_file = os.path.join(roadblock_msgs_dir, f"{label}.json")

    # Initialize the command string and set the role to "follower"
    cmd = ""
    role = "follower"

    # Use subprocess to run a "ping" command to the rickshaw host
    # subprocess.run(["ping", "-w", "10", "-c", "4", options['rickshaw_host']])
    subprocess.run(["ping", "-w", "10", "-c", "4", rickshaw_host])

    # Append the necessary arguments to the cmd string
    # cmd += f" {options['roadblock_bin']} --role={role} --redis-server={options['rickshaw_host']}"
    cmd += f" {roadblock_bin} --role={role} --redis-server={rickshaw_host}"
    # cmd += f" --leader-id={leader} --timeout={timeout} --redis-password={options['roadblock_passwd']}"
    cmd += f" --leader-id={leader} --timeout={timeout} --redis-password={roadblock_passwd}"
    # cmd += f" --follower-id={options['cs_label']} --message-log={options['msgs_log_file']}"
    cmd += f" --follower-id={cs_label} --message-log={msgs_log_file}"

    # If messages is not None, append the user message argument to the cmd string
    # if options['messages'] is not None:
    #     cmd += f" --user-message {options['messages']}"
    if messages is not None:
        cmd += f" --user-message {messages}"

    # If wait_for is not None, append the wait-for argument and wait_for_log argument to the cmd string
    # if options['wait_for'] is not None:
    #     cmd += f" --wait-for \"{options['wait_for']}\""
    #     cmd += f" --wait-for-log {wait_for_log}"
    if wait_for is not None:
        cmd += f" --wait-for \"{wait_for}\""
        cmd += f" --wait-for-log {wait_for_log}"
    
    # If abort is True, append the abort argument to the cmd string
    # if options['abort']:
    #     cmd += " --abort"
    if abort:
        cmd += " --abort"
    
    # Set the uuid to be the concatenation of the roadblock_id and the label
    uuid = f"{roadblock_id}:{label}"

    # Print info about the roadblock
    print("\n\n")
    print(f"Starting roadblock [{datetime.datetime.now()}]")
    # print(f"server: {options['rickshaw_host']}")
    print(f"server: {rickshaw_host}")
    print(f"role: {role}")
    print(f"uuid (without attempt ID embedded): {uuid}")
    print(f"timeout: {timeout}")

    # Initialize the number of attempts and the exit code
    attempts = 0
    rc = 99

    # Loop until either the maximum number of attempts is reached or the roadblock is successful or aborted
    max_rb_attempts = options['max_rb_attempts']
    max_rb_attempts = remove_quotes(max_rb_attempts)
    # while attempts < int(max_rb_attempts) and rc != options['rb_exit_success'] and rc != options['rb_exit_abort']:
    while attempts < int(max_rb_attempts) and rc != rb_exit_success and rc != rb_exit_abort:
        attempts += 1
        print(f"attempt number: {attempts}")
        print(f"uuid: {attempts}:{uuid}")
        rb_cmd = f"{cmd} --uuid={attempts}:{uuid}" # DEBUG uncomment when done
        # Error
        # going to run this roadblock command: /bin/sh: /usr/local/bin/roadblock.py: Permission denied

        # DEBUG
        #rb_cmd = f"sudo {cmd} --uuid={attempts}:{uuid}" # DEBUG
        # Error
        # sudo: command not 
        
        # original rb command containing single quotes
        #print("going to run this roadblock command:", rb_cmd)

        # testing: remove single quotes from rb command
        rb_cmd = remove_single_quotes(rb_cmd)

        # what if the command isn't calling a python script?
        # could add a conditional to check if '.py' is in command string
        rb_cmd = 'python3' + rb_cmd

        #print('DEBUG: attempt to remove single quotes from command')
        print("going to run this roadblock command:", rb_cmd)
        print("roadblock output BEGIN")
        print()

        # Debug - problem: subprocess is failing. modifying the function to provide insight into the source of the error.
        # TODO: uncomment when fixed
        ## Use subprocess to run the roadblock command and capture the exit code
        ## Set shell=True to interpret the command as a string instead of a list
        #rc = subprocess.run(rb_cmd, shell=True).returncode
        
        try:
            rc = subprocess.run(rb_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            print(rc.stdout.decode())
        except subprocess.CalledProcessError as e:
            print(f"Command '{rb_cmd}' returned non-zero exit status {e.returncode}.")
            print(e.stderr.decode())  # Display the error message
        
        print("roadblock output END")
        print(f"roadblock exit code: {rc}")

        # If the user messages log file exists, print its contents
        # if os.path.isfile(options['msgs_log_file']):
        if os.path.isfile(msgs_log_file):
            print("# start messages from roadblock ################################################")
            # with open(options['msgs_log_file'], "r") as msgs_file:
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
    return options,rc


def get_data(options):
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
    cs_id = options['cs_id']
    cs_type = options['cs_type']
    engine_config_dir = options['engine_config_dir']
    rickshaw_host = options['rickshaw_host']
    ssh_id_file = options['ssh_id_file']
    tool_cmds_dir = options['tool_cmds_dir']

    # Construct the file name
    if cs_type == "client" or cs_type == "server" or cs_type == "profiler":
        cs_files_list = f"{cs_type}-{cs_id}-files-list"
    else:
        cs_files_list = f"{cs_type}-files-list"
    
    # Retrieve the files
    cmd = f"scp -i {ssh_id_file} {rickshaw_host}:{engine_config_dir}/{cs_files_list} {cs_files_list}"
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
        cmd = f"scp -i {ssh_id_file} {rickshaw_host}:{engine_config_dir}/bench-cmds/{cs_type}/{cs_id}/start bench-start-cmds"
        subprocess.run(cmd, check=True, shell=True)
        if cs_type == "client":
            cmd = f"scp -i {ssh_id_file} {rickshaw_host}:{engine_config_dir}/bench-cmds/{cs_type}/{cs_id}/infra bench-infra-cmds"
            subprocess.run(cmd, check=True, shell=True)
            if cs_id == "1":
                cmd = f"scp -i {ssh_id_file} {rickshaw_host}:{engine_config_dir}/bench-cmds/{cs_type}/{cs_id}/runtime bench-runtime-cmds"
                subprocess.run(cmd, check=True, shell=True)
        else:
            cmd = f"scp -i {ssh_id_file} {rickshaw_host}:{engine_config_dir}/bench-cmds/{cs_type}/{cs_id}/stop bench-stop-cmds"
            subprocess.run(cmd, check=True, shell=True)
    else:
        cmd = f"scp -i {ssh_id_file} {rickshaw_host}:{engine_config_dir}/bench-cmds/client/1/start bench-start-cmds"
        subprocess.run(cmd, check=True, shell=True)

    cmd = f"scp -i {ssh_id_file} {rickshaw_host}:{tool_cmds_dir}/{cs_id}/start tool-start-cmds"
    subprocess.run(cmd, check=True, shell=True)
    cmd = f"scp -i {ssh_id_file} {rickshaw_host}:{tool_cmds_dir}/{cs_id}/stop tool-stop-cmds"
    subprocess.run(cmd, check=True, shell=True)

def setup_core_env(options):
    """
    Sets several environment variables, creates several directories on the client/server and the controller.

    """
    os.environ["RS_CS_LABEL"] = options['cs_label']
    cs_label = options['cs_label']
    options['cs_type'] = cs_label.split("-")[0]
    options['cs_id'] = cs_label.split("-")[1]

    # Directories on the client/server
    options['cs_dir'] = tempfile.mkdtemp()
    print(f"cs_dir: {options['cs_dir']}")
    options['tool_start_cmds'] = os.path.join(options['cs_dir'], "tool-start")
    options['tool_stop_cmds'] = os.path.join(options['cs_dir'], "tool-stop")
    options['roadblock_msgs_dir'] = os.path.join(options['cs_dir'], "roadblock-msgs")
    os.makedirs(options['roadblock_msgs_dir'])
    sysinfo_dir = os.path.join(options['cs_dir'], "sysinfo")
    os.makedirs(sysinfo_dir)

    # Directories on the controller
    options['config_dir'] = os.path.join(options['base_run_dir'], "config")
    options['engine_config_dir'] = os.path.join(options['config_dir'], "engine")
    options['tool_cmds_dir'] = os.path.join(options['config_dir'], f"tool-cmds/{options['cs_type']}")
    options['run_dir'] = os.path.join(options['base_run_dir'], "run")
    options['archives_dir'] = os.path.join(options['run_dir'], "engine/archives")
    options['sync_prefix'] = "engine"
    options['sync'] = f"{options['sync_prefix']}-script-start"

    return options

def validate_core_env(options):
    """
    Check that all necessary environment variables have been set, and log various
    configuration settings to the console. If any required variables are missing, the
    script will print an error message and exit with an error code.
    """

    print()
    print("core environment:")
    
    # Check that rickshaw_host environment variable is set
    rickshaw_host = options['rickshaw_host']
    if rickshaw_host is None:
        exit_error("Exiting due to rickshaw host not being set")
    else:
        print(f"rickshaw_host={rickshaw_host}")

    # Check if roadblock_id and roadblock_passwd environment variables are set, and if so, check if the roadblock binary exists
    roadblock_id = options['roadblock_id']
    roadblock_passwd = options['roadblock_passwd']
    if roadblock_id is None or roadblock_passwd is None:
        print("Cannot use roadblock for synchronization because an ID or password was not provided")
        USE_ROADBLOCK = False
    else:
        print("Using roadblock for synchronization because both ID and password were provided")
        
        USE_ROADBLOCK = True
        if not os.path.exists(options['roadblock_bin']):
            exit_error(f"Could not find roadblock binary: {options['roadblock_bin']}")
        else:
            print("Found roadblock binary")
    
    # Check that cs_label environment variable is set and matches a certain regular expression
    cs_label = options['cs_label']
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
    max_rb_attempts = options['max_rb_attempts']
    max_rb_attempts = max_rb_attempts.replace("'", "")
    if max_rb_attempts is None:
        max_rb_attempts = 1
        print(f'[WARNING] --max-rb-attempts was not used, so setting to default of {max_rb_attempts}')
    else:
        print(f"max_rb_attempts={max_rb_attempts}")

    # Check that endpoint_run_dir is set
    endpoint_run_dir = options['endpoint_run_dir']
    if endpoint_run_dir is None:
        exit_error("The endpoint run directory (--endpoint-run-dir) was not defined")
    else:
        print(f"endpoint_run_dir={endpoint_run_dir}")
    
    engine_script_start_timeout = options['engine_script_start_timeout']
    print(f"engine_script_start_timeout: {engine_script_start_timeout}")

    return


def main(*args):
    args = process_opts()
    options = vars(args)

    # debug print: options parsed from command line
    #print()
    #print(options)

    # Add configuration settings
    options['roadblock_bin'] = "/usr/local/bin/roadblock.py"
    options['use_roadblock'] = True
    options['rb_exit_success'] = 0
    options['rb_exit_timeout'] = 3
    options['rb_exit_abort'] = 4
    options['rb_exit_input'] = 2
    options['runtime_padding'] = 180
    options['abort'] = 0
    
    # debug print: adding additional options
    #print()
    #print(options)

    # load Rickshaw settings from json
    with lzma.open('/tmp/rickshaw-settings.json.xz', 'rt') as f:
        rickshaw_opts = json.load(f)

    # debug print: show rickshaw settings
    #print()
    #print('Rickshaw settings from json file:')
    #print(rickshaw_opts)

    options['leader'] = 'controller'

    # print options before entering modifying functions
    print()
    print('options:')
    print(options)

    validate_core_env(options)
    print()
    print('options post validate_core_env:')
    print(options)

    setup_core_env(options)
    print()
    print('options post setup_core_env:')
    print(options)

    # cs_dir is created by setup_core_env
    if not os.path.exists(options['cs_dir']):
        exit_error(f"Could not chdir to {options['cs_dir']}", "engine-init-begin")

    """
    TODO: handle cpu partitioning
    """

    options['messages'] = None
    options['wait_for'] = None


    # Initialize engine
    do_roadblock(options, 'engine-init-begin')
    do_roadblock(options, 'engine-init-end')

    # Get data
    do_roadblock(options, 'get-data-begin')
    os.environ["RS_CS_LABEL"] = options['cs_label']
    do_roadblock(options, 'get-data-end')

    # Collect sysinfo
    do_roadblock(options, 'collect-sysinfo-begin')
    


if __name__ == '__main__':
    main()
