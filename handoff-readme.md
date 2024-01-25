# Overview
The project goal is to rewrite the Rickshaw engine from bash to Python 3. Expected benefits include improved readability and maintainability of the codebase. 

# Current bash implementation
Most key files to the engine script can be found in the directory `/rickshaw/engine/`:
* `bootstrap`: bash script used for setting up the environment for Rickshaw.
* `engine-script`: driver script for the Rickshaw engine.
* `engine-script-library`: defines functions used in engine-script.

Rickshaw engine-script workflow:
* Displays information about the environment to console, including:
    * Bash version;
    * Any parameters the engine script was run with;
    * Release version of the operating system via `cat /etc/os-release`;
    * System info via `uname -a`;
    * The Rickshaw engine script version number;
* Completes initial setup
    * Sets leader as controller;
    * Calls function load_json_setting (a Pearl script) to set the default timeout value;
    * Calls function process_opts
    * Calls function validate_core_env
    * Calls function setup_core_env
* The main body of the script runs roadblocks in stages via the do_roadblock function. In general, this is done by running a ‘begin’ stage and an ‘end’ stage in do_roadblock. Each stage ends with error catching by the roadblock_exit_on_error function. For some roadblocks, additional functions are called between the ‘begin’ and ‘end’ stages:
    * Roadblock: engine-init
        * do_roadblock engine-init-begin
        * do_roadblock engine-init-end
    * Roadblock: get-data
        * do_roadblock get-data-begin
        * Calls function get_data
        * do_roadblock get-data-end
    * Roadblock: collect-sysinfo
        * do_roadblock: collect-sysinfo-begin
        * Calls function collect_sysinfo
        * do_roadblock: collect-sysinfo-end
    * Roadblock: start-tools
        * do_roadblock: start-tools-begin 
        * Calls function start_tools
        * do_roadblock start-tools-end
        * Calls function: process_bench_roadblocks
    * Roadblock: stop-tools
        * do_roadblock: stop-tools-begin
        * do_roadblock: stop-tools-end
    * Roadblock: send-data 
        * do_roablock: send-data-begin
        * do_roadblock: send-data-end

     
# Python implementation: engine.py
The functionality of `engine-script` and `engine-script-library` are combined into the single `engine.py` file. The Python 3 script has two basic components:
* A driver program that processes parameters supplied by the `bootstrap` script, then uses those paramters to initiate a test run.
* The various helper functions, which largely mirror those defined in `engine-script-library`.


# Other files that changed
In addition to `engine.py`, the following other changes were required:
* `/engine/bootstrap`: updated references of `engine-script` and `engine-script-library` with `scp_from_controller`, `exec`, and `exit_error` to point to the new Python file. Please refer to this branch's version of the [bootstrap script](https://github.com/perftool-incubator/rickshaw/blob/rickshaw-engine-port/engine/bootstrap) for more information.
* `/rickshaw-run`: updated several references of `engine-script` and `engine-script-library` to the new Python script within `sub make_run_dirs()` and `sub prepare_bench_tool_engines`. Please refer to this branch's version of the [rickshaw-run script](https://github.com/perftool-incubator/rickshaw/blob/rickshaw-engine-port/rickshaw-run) for more information.


# Current status and issues
When executed on a Crucible controller, `engine.py` will successfully parse run parameters from `bootstrap`, copy files from the Crucible controller to the SUT, and attempt to run the engine-init roadblock. The engine-init roadblock times out, report that no followers came online, and the run exits in failure.  
