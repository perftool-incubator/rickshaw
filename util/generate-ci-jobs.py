#!/usr/bin/python3

import argparse
import copy
import logging
from invoke import run

import sys
import os
from pathlib import Path
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
    Process the user input parameters

    Args:
        None

    Globals:
        None

    Returns:
        args: The generated arguments
    """
    parser = argparse.ArgumentParser(description="Read the CI config file and generate a list of jobs based on the input parameters")

    parser.add_argument("--benchmark",
                        dest = "benchmark",
                        help = "Which benchmark to generate jobs for",
                        default = "all",
                        type = str)

    parser.add_argument("--log-level",
                        dest = "log_level",
                        help = "Control how much logging output should be generated",
                        default = "normal",
                        choices = [ "normal", "debug" ])

    parser.add_argument("--runner-tags",
                        dest = "runner_tags",
                        help = "What tags are required for self-hosted runners",
                        default = "",
                        type = str)

    parser.add_argument("--runner-pool",
                        dest = "runner_pool",
                        help = "Which pool of self-hosted runners to use (e.g., 'kmr-cloud-1', 'aws-cloud-1')",
                        default = "",
                        type = str)

    parser.add_argument("--runner-type",
                        dest = "runner_type",
                        help = "Which runner type to generate jobs for",
                        default = "github",
                        choices = [ "github", "self-hosted" ])

    parser.add_argument("--runtime-env",
                        dest = "runtime_env",
                        help = "Where is this being run",
                        default = "local",
                        choices = [ "local", "github" ])

    parser.add_argument("--userenv-filter",
                        dest = "userenv_filter",
                        help = "A filter to determine which userenvs should be included",
                        default = "all",
                        choices = [ "all", "minimal", "unique" ])

    the_args = parser.parse_args()

    # convert from comma separated string list to a Python list
    the_args.runner_tags = the_args.runner_tags.split(",")

    return the_args

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
    Convert a variable into a human readable formatted JSON string

    Args:
        obj: A variable of potentially many types to convert into a JSON string

    Globals:
        None

    Returns:
        str: A formatted string containing the JSON representation of obj
    """
    return json.dumps(obj, indent = 4, separators=(',', ': '), sort_keys = True, default = not_json_serializable)

def dump_raw_json(obj):
    """
    Convert a variable into a raw formatted JSON string suitable for ingest into another program

    Args:
        obj: A variable of potentially many types to convert into a JSON string

    Globals:
        None

    Returns:
        str: A string containing the JSON representation of obj
    """
    return json.dumps(obj, separators=(',', ':'), sort_keys = True, default = not_json_serializable)

def build_runner_labels(logger, runner_type, runner_tags, runner_pool):
    """
    Build the list of runner labels for the runs-on directive

    Args:
        logger: a logger instance used to output status
        runner_type: The type of runner (github or self-hosted)
        runner_tags: List of tags for self-hosted runners
        runner_pool: Optional pool identifier for self-hosted runners

    Globals:
        None

    Returns:
        list: A list of runner labels to be used in the runs-on directive
    """
    labels = []

    if runner_type == "self-hosted":
        labels.append("self-hosted")

        # Add pool identifier if specified (e.g., 'kmr-cloud-1', 'aws-cloud-1')
        # This is optional for backward compatibility
        if runner_pool:
            labels.append(runner_pool)
            logger.debug("Adding runner pool label '%s'" % (runner_pool))

        # Add other runner tags (e.g., 'cpu-partitioning', 'remotehosts')
        # Filter out empty strings that might come from split
        for tag in runner_tags:
            if tag:  # only add non-empty tags
                labels.append(tag)
                logger.debug("Adding runner tag '%s'" % (tag))
    else:
        # For GitHub-hosted runners, use ubuntu-latest
        labels.append("ubuntu-latest")

    logger.debug("Built runner labels: %s" % (str(labels)))
    return labels

def get_jobs(logger):
    """
    Load the CI config file and generate raw jobs from it based on the user input

    Args:
        logger: a logger instance used to output status

    Globals:
        args: user input parameters

    Returns:
        obj: The list of generated jobs
    """
    logger.info("Getting raw jobs")

    ci_input_file = rickshaw_dir + "/util/crucible-ci.json"
    input_json, load_err = load_json_file(ci_input_file)
    if input_json is None:
        logger.error("Failed to load input file: %s" % (ci_input_file))
        logger.error("Load error: %s" % (load_err))
        return None
    else:
        logger.info("Loaded input file: %s" % (ci_input_file))

    ci_schema_file = rickshaw_dir + "/schema/crucible-ci.json"
    valid, valid_err = validate_schema(input_json, ci_schema_file)
    if not valid:
        logger.error("Failed to validate input file against schema file: %s" % (ci_schema_file))
        logger.error("Validation error: %s" % (valid_err))
        return None
    else:
        logger.info("Validated input file against schema file: %s" % (ci_schema_file))

    raw_jobs = list()

    if input_json["config"]["enabled"]:
        logger.debug("Global enabled is True")

        for benchmark in input_json["benchmarks"]:
            if benchmark["enabled"]:
                logger.debug("Benchmark '%s' enabled is True" % (benchmark["name"]))

                if args.benchmark == "all" or args.benchmark == benchmark["name"]:
                    logger.info("Processing benchmark '%s'" % (benchmark["name"]))

                    for scenario in benchmark["scenarios"]:
                        if scenario["enabled"]:
                            logger.debug("Scenario '%s' enabled is True" % (str(scenario)))

                            if scenario["runners"]["type"] == args.runner_type:
                                add_scenario = True
                                if args.runner_type == "self-hosted":
                                    for tag in scenario["runners"]["tags"]:
                                        if tag in args.runner_tags:
                                            logger.debug("Found scenario tag '%s' in requested" % (tag))
                                        else:
                                            logger.debug("Scenario tag '%s' not found in requested" % (tag))
                                            add_scenario = False

                                if add_scenario:
                                    logger.info("Adding jobs for benchmark '%s' and runner type '%s'" % (benchmark["name"], scenario["runners"]["type"]))
                                    # Build runner labels for this scenario
                                    runner_labels = build_runner_labels(
                                        logger,
                                        scenario["runners"]["type"],
                                        scenario["runners"].get("tags", []) if args.runner_type == "self-hosted" else [],
                                        args.runner_pool
                                    )
                                    for endpoint in scenario["endpoints"]:
                                        job = {
                                            "benchmark": benchmark["name"],
                                            "enabled": True,
                                            "endpoint": endpoint,
                                            "runner_labels": runner_labels
                                        }
                                        logger.info("Adding job for endpoint '%s'" % (endpoint))
                                        raw_jobs.append(job)
                                else:
                                    logger.debug("Not adding jobs for benchmark '%s' and runner type '%s' due to missing tag" % (benchmark["name"], scenario["runners"]["type"]))
                            else:
                                logger.debug("Runner type '%s' does not match '%s'" % (scenario["runners"]["type"], args.runner_type))
                        else:
                            logger.debug("Scenario '%s' enabled is False" % (str(scenario)))
                else:
                    logger.debug("Benchmark '%s' is not included" % (benchmark["name"]))
            else:
                logger.debug("Benchmark '%s' enabled is False" % (benchmark["name"]))
    else:
        logger.debug("Global enabled is False")

    if len(raw_jobs) == 0:
        # we need at least one job otherwise the github action workflow
        # for this runnter-type will fail -- this should only happen if
        # a specific benchmark is requested that does not require this
        # runner type so lets hard code that benchmark

        # the job will be disabled so that it does not actually run the
        # integration test, it's just a "filler" to have something in
        # the job matrix

        # specifying the remotehosts endpoint since that should work on
        # all current runner types (not that it really matters since it
        # won't actually run)
        runner_labels = build_runner_labels(logger, args.runner_type, args.runner_tags, args.runner_pool)
        job = {
            "benchmark": args.benchmark,
            "enabled": False,
            "endpoint": "remotehosts",
            "runner_labels": runner_labels
        }
        raw_jobs.append(job)

    return raw_jobs

def get_userenvs(logger):
    """
    Inspect the rickshaw directory and generate a list of userenvs based on user input

    Args:
        logger: a logger instance used to output status

    Globals:
        args: user input parameters

    Returns:
        obj: The list of generated userenvs
    """
    logger.info("Getting userenvs")

    userenvs = list()
    final_userenvs = list()
    userenv_excludes = list()
    diff_cmd_validate="git log HEAD^1"
    diff_cmd="git diff --name-only HEAD^1 HEAD"

    try:
        userenv_excludes_file = rickshaw_dir + "/userenvs/ci-excludes.txt"
        logger.debug("Loading userenv excludes from: %s" % (userenv_excludes_file))
        with open(userenv_excludes_file, "r") as fh:
            for line in fh:
                userenv_excludes.append(line.strip())
    except FileNotFoundError:
        logger.debug("Could not find %s, failling back on historical excludes list" % (userenv_excludes_file))
        userenv_excludes.extend([ "stream8-flexran", "rhel-ai" ])
    logger.debug("List of %d userenv excludes:\n%s" % (len(userenv_excludes), "\n".join(userenv_excludes)))

    include_all_testable_userenvs = False

    result = run(diff_cmd_validate, hide = True, warn = True)
    changed_files = None
    if result.exited == 0:
        # history is available -- this must be a rickshaw repository PR
        logger.info("Rickshaw history is available")

        result = run(diff_cmd, hide = True, warn = True)
        changed_files = result.stdout.split("\n")
        changed_files = list(filter(None, changed_files)) # remove the empty lines
        logger.info("%d rickshaw files changed:\n%s" % (len(changed_files), "\n".join(changed_files)))
        non_userenv_files_changed = list(filter(lambda x: not x.startswith("userenvs/"), changed_files)) # get the files that do not start with 'userenvs/'
        logger.info("%d rickshaw non-userenv files changed:\n%s" % (len(non_userenv_files_changed), "\n".join(non_userenv_files_changed)))

        if len(non_userenv_files_changed) > 0:
            include_all_testable_userenvs = True
            logger.info("Non userenv changes are present so reverting to normal behavior")
    else:
        # no history available -- this is not a rickshaw repository PR
        logger.info("Rickshaw history is not available")
        include_all_testable_userenvs = True

    start_path = Path(rickshaw_dir + "/userenvs")
    testable_userenv_paths = sorted(start_path.glob("*.json"))
    logger.debug("testable userenv paths:\n%s" % (testable_userenv_paths))
    testable_userenvs = list()
    for userenv in testable_userenv_paths:
        if not userenv.is_symlink():
            userenv_path = str(userenv)
            userenv_path = userenv_path.removeprefix(rickshaw_dir + "/")
            testable_userenvs.append(userenv_path)
    logger.debug("%d testable userenvs:\n%s" % (len(testable_userenvs), "\n".join(testable_userenvs)))
    if not include_all_testable_userenvs:
        for file in changed_files:
            if file in testable_userenvs:
                logger.info("Found userenv '%s' in the testable list" % (file))
                userenvs.append(file)
    else:
        for file in testable_userenvs:
            logger.info("Found userenv '%s' in the testable list" % (file))
            userenvs.append(file)

    # remove 'userenvs/' from the beginning of the names and '.json' from the end
    userenvs = list(map(lambda x: x.removeprefix("userenvs/").removesuffix(".json"), userenvs))

    logger.info("Initial list of %d userenvs (pre-exclusion):\n%s" % (len(userenvs), "\n".join(userenvs)))

    logger.info("Applying userenv exclusion list")
    tmp_userenvs = list()
    for userenv in userenvs:
        if userenv in userenv_excludes:
            logger.info("Discarding userenv '%s' as it is in the exclusion list" % (userenv))
        else:
            logger.debug("Userenv '%s' is being kept since it is not in the exclusion list" % (userenv))
            tmp_userenvs.append(userenv)
    userenvs = copy.deepcopy(tmp_userenvs)

    logger.info("List of %d userenvs (post-exclusion):\n%s" % (len(userenvs), "\n".join(userenvs)))

    if include_all_testable_userenvs:
        # this is the normal behavior
        logger.info("Creating final userenvs through normal filtering process")

        if args.userenv_filter == "all" or args.userenv_filter == "minimal":
            logger.info("Adding 'default' userenv")
            final_userenvs.append("default")

        if args.userenv_filter == "all" or args.userenv_filter == "unique":
            for userenv in userenvs:
                logger.info("Adding '%s' userenv" % (userenv))
                final_userenvs.append(userenv)

        if args.userenv_filter == "all":
            # this userenv comes from the crucible-ci-userenvs repo
            # and is used to test externally provided userenvs --
            # ie. userenvs that do not come from Crucible/rickshaw
            logger.info("Adding 'external' userenv")
            final_userenvs.append("external")
    else:
        # this is the rickshaw PR behavior where only modified /
        # created userenvs are being tested when no other rickshaw
        # changes are present
        logger.info("Creating final userenvs through rickshaw userenv PR process")

        for userenv in userenvs:
            logger.info("Adding '%s' userenv")
            final_userenvs.append(userenv)

        logger.info("Total userenvs added: %d" % (len(final_userenvs)))

        if len(final_userenvs) == 0:
            # if we have reached this point then it is likely that a
            # situation has occurred where there no userenvs to test
            # -- for example the current rickshaw PR is only removing
            # a userenv -- so generate a single default userenv to
            # satisify testing requirements
            logger.info("Adding 'default' userenv since no other userenvs were added")
            final_userenvs.append("default")

    return final_userenvs

def main():
    """
    The main processing function

    Args:
        None

    Globals:
        args: user input parameters

    Returns:
        int: The return code for the program
    """

    log_debug_format =  '[%(asctime)s %(levelname)s %(module)s %(funcName)s:%(lineno)d] %(message)s'
    log_normal_format = '[%(asctime)s %(levelname)s] %(message)s'

    if args.log_level == 'debug':
        logging.basicConfig(level = logging.DEBUG, format = log_debug_format, stream = sys.stdout)
    elif args.log_level == 'normal':
        logging.basicConfig(level = logging.INFO, format = log_normal_format, stream = sys.stdout)

    logger = logging.getLogger(__file__)

    logger.info("Parameters: " + str(args))
    os.chdir(rickshaw_dir)

    jobs = list()

    raw_jobs = get_jobs(logger)
    if raw_jobs is None:
        return 1
    logger.info("Generated %d jobs:\n%s" % (len(raw_jobs), dump_json(raw_jobs)))

    userenvs = get_userenvs(logger)
    if userenvs is None:
        return 1
    logger.info("Generated %d userenvs\n%s" % (len(userenvs), "\n".join(userenvs)))

    # multiply the jobs * userenvs to get the final job list
    logger.info("Performing job multiplication (jobs * userenvs)")
    for raw_job in raw_jobs:
        for userenv in userenvs:
            job = copy.deepcopy(raw_job)
            job["userenv"] = userenv
            jobs.append(job)
            logger.info("Adding job \"%s\"" % (job))

    logger.info("Generated %d final jobs:\n%s" % (len(jobs), dump_json(jobs)))

    if args.runtime_env == "github":
        github_output = os.environ.get("GITHUB_OUTPUT")
        if github_output is not None:
            with open(github_output, "a") as fh:
                fh.write("jobs=" + dump_raw_json(jobs) + "\n")
            logger.info("Wrote GitHub output to %s" % (github_output))
        else:
            logger.error("The runtime-env is defined as GitHub but there is no GITHUB_OUTPUT environment variable")
            return 1

    return 0

if __name__ == "__main__":
    args = process_options()
    logger = None
    rickshaw_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    exit(main())
