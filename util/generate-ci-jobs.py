#!/usr/bin/python3

import argparse
import copy
import json
import logging
import re
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


CI_CONFIG_PATH = "util/crucible-ci.json"
CI_SCHEMA_PATH = "schema/crucible-ci.json"
SCENARIO_FILE_RE = re.compile(r"^util/crucible-ci/([^./]+)\.([^./]+)\.json$")

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

    parser.add_argument("--endpoint",
                        dest = "endpoint",
                        help = "Filter jobs to a specific endpoint (e.g., 'kube' or 'remotehosts')",
                        default = "all",
                        type = str)

    parser.add_argument("--job-size",
                        dest = "job_size",
                        help = "What size of job should be constructed",
                        default = "small",
                        choices = [ "small", "big" ])

    parser.add_argument("--log-level",
                        dest = "log_level",
                        help = "Control how much logging output should be generated",
                        default = "normal",
                        choices = [ "normal", "debug" ])

    parser.add_argument("--runner-pool",
                        dest = "runner_pool",
                        help = "Which runner pool to use (e.g., 'aws-cloud-1')",
                        default = "",
                        type = str)

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

def _validate_capabilities(logger, input_json):
    """
    Validate that all capability references in endpoints, runner-pools, and
    scenarios are defined in the capabilities dictionary.

    Returns:
        list: error messages, empty if valid
    """
    defined = set(input_json.get("capabilities", {}).keys())
    if not defined:
        return []

    errors = []

    for ep_name, ep_config in input_json.get("endpoints", {}).items():
        for req in ep_config.get("requirements", []):
            if req not in defined:
                errors.append("Endpoint '%s' references undefined capability '%s'" % (ep_name, req))

    for pool_name, pool_config in input_json.get("runner-pools", {}).items():
        for cap in pool_config.get("provides", []):
            if cap not in defined:
                errors.append("Runner pool '%s' references undefined capability '%s'" % (pool_name, cap))

    for benchmark in input_json.get("benchmarks", []):
        for scenario in benchmark.get("scenarios", []):
            for req in scenario.get("requirements", []):
                if req not in defined:
                    errors.append("Benchmark '%s' scenario references undefined capability '%s'" % (benchmark.get("name", "?"), req))

    return errors


def _normalize_json(value):
    """Return a JSON value with object and array ordering removed."""
    if isinstance(value, dict):
        return {key: _normalize_json(item) for key, item in value.items()}
    if isinstance(value, list):
        normalized = [_normalize_json(item) for item in value]
        return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True))
    return value


def _load_git_json(logger, path):
    """Load a JSON file from the first parent, or return None if unavailable."""
    result = run("git show HEAD^1:%s" % (path), hide=True, warn=True)
    if result.exited != 0:
        logger.info("Could not load %s from HEAD^1; using the full scenario matrix", path)
        return None

    try:
        return json.loads(result.stdout)
    except (TypeError, ValueError) as e:
        logger.info("Could not parse %s from HEAD^1 (%s); using the full scenario matrix", path, str(e))
        return None


def _load_current_json(logger, path):
    """Load a JSON file from the checked-out head, or return None on error."""
    try:
        with open(os.path.join(rickshaw_dir, path), "r") as fh:
            return json.load(fh)
    except (OSError, TypeError, ValueError) as e:
        logger.info("Could not load current %s (%s); using the full scenario matrix", path, str(e))
        return None


def _benchmark_entries(input_json):
    """Index benchmark entries by name, rejecting ambiguous configurations."""
    benchmarks = input_json.get("benchmarks")
    if not isinstance(benchmarks, list):
        return None

    entries = {}
    for benchmark in benchmarks:
        if not isinstance(benchmark, dict) or not isinstance(benchmark.get("name"), str):
            return None
        name = benchmark["name"]
        if name in entries:
            return None
        entries[name] = benchmark
    return entries


def _benchmark_enum_location(schema):
    """Return the schema node containing the benchmark name enum."""
    try:
        return schema["properties"]["benchmarks"]["items"]["properties"]["name"]
    except (KeyError, TypeError):
        return None


def _isolated_benchmark_enum_additions(base_schema, head_schema, new_benchmark_names):
    """Return newly configured benchmark names added only to the schema enum."""
    base_name_schema = _benchmark_enum_location(base_schema)
    head_name_schema = _benchmark_enum_location(head_schema)
    if base_name_schema is None or head_name_schema is None:
        return None

    base_names = base_name_schema.get("enum")
    head_names = head_name_schema.get("enum")
    if not isinstance(base_names, list) or not isinstance(head_names, list):
        return None
    if not all(isinstance(name, str) for name in base_names + head_names):
        return None
    if len(set(base_names)) != len(base_names) or len(set(head_names)) != len(head_names):
        return None

    additions = set(head_names) - set(base_names)
    removals = set(base_names) - set(head_names)
    if not additions or removals:
        return None
    if not additions.issubset(new_benchmark_names):
        return None

    # The enum's order has no meaning. Ignore its contents after checking that
    # only configured benchmark names were added, then compare the rest.
    base_copy = copy.deepcopy(base_schema)
    head_copy = copy.deepcopy(head_schema)
    _benchmark_enum_location(base_copy)["enum"] = ["<benchmark-name-enum>"]
    _benchmark_enum_location(head_copy)["enum"] = ["<benchmark-name-enum>"]
    if _normalize_json(base_copy) != _normalize_json(head_copy):
        return None

    return additions


def _current_job_pairs(input_json, pool):
    """Return benchmark/endpoint pairs that can produce enabled current jobs."""
    pairs = set()
    if not input_json.get("config", {}).get("enabled", False):
        return pairs

    pool_provides = set(pool.get("provides", []))
    for benchmark in input_json.get("benchmarks", []):
        if not benchmark.get("enabled", False):
            continue
        for scenario in benchmark.get("scenarios", []):
            if not scenario.get("enabled", False):
                continue
            for endpoint in scenario.get("endpoints", []):
                endpoint_config = input_json.get("endpoints", {}).get(endpoint, {})
                if not endpoint_config.get("enabled", True):
                    continue
                requirements = set(scenario.get("requirements", []))
                requirements.update(endpoint_config.get("requirements", []))
                if requirements.issubset(pool_provides):
                    pairs.add((benchmark["name"], endpoint))
    return pairs


def _classify_changed_scenarios(logger, changed_files, input_json, current_pairs):
    """Return selected benchmark/endpoint pairs, or None for the full matrix."""
    if changed_files is None:
        logger.info("Changed-file history is unavailable; using the full scenario matrix")
        return None

    if not changed_files:
        logger.info("No changed paths were available; using the full scenario matrix")
        return None

    selected_pairs = set()
    config_changed = False
    schema_changed = False

    for path in changed_files:
        match = SCENARIO_FILE_RE.fullmatch(path)
        if match is not None:
            pair = (match.group(1), match.group(2))
            if not os.path.isfile(os.path.join(rickshaw_dir, path)):
                logger.info("Changed scenario file %s is not present at HEAD; using the full scenario matrix", path)
                return None
            if pair not in current_pairs:
                logger.info("Changed scenario file %s has no current enabled job; using the full scenario matrix", path)
                return None
            selected_pairs.add(pair)
        elif path == CI_CONFIG_PATH:
            config_changed = True
        elif path == CI_SCHEMA_PATH:
            schema_changed = True
        else:
            logger.info("Changed path %s is outside the scenario classifier; using the full scenario matrix", path)
            return None

    changed_benchmark_names = set()
    new_benchmark_names = set()
    if config_changed:
        base_config = _load_git_json(logger, CI_CONFIG_PATH)
        if not isinstance(base_config, dict):
            logger.info("Base CI config is unavailable or invalid; using the full scenario matrix")
            return None

        base_shared = {key: value for key, value in base_config.items() if key != "benchmarks"}
        head_shared = {key: value for key, value in input_json.items() if key != "benchmarks"}
        if _normalize_json(base_shared) != _normalize_json(head_shared):
            logger.info("Shared CI settings changed; using the full scenario matrix")
            return None

        base_benchmarks = _benchmark_entries(base_config)
        head_benchmarks = _benchmark_entries(input_json)
        if base_benchmarks is None or head_benchmarks is None:
            logger.info("Benchmark entries cannot be compared safely; using the full scenario matrix")
            return None

        changed_benchmark_names = {
            name for name in set(base_benchmarks) | set(head_benchmarks)
            if name not in base_benchmarks or name not in head_benchmarks or
            _normalize_json(base_benchmarks.get(name)) != _normalize_json(head_benchmarks.get(name))
        }
        new_benchmark_names = set(head_benchmarks) - set(base_benchmarks)

        if not changed_benchmark_names:
            logger.info("CI config has no structural benchmark changes")

        for name in changed_benchmark_names:
            if name not in head_benchmarks:
                logger.info("Benchmark %s was removed; using the full scenario matrix", name)
                return None
            benchmark_pairs = {pair for pair in current_pairs if pair[0] == name}
            if not benchmark_pairs:
                logger.info("Changed benchmark %s has no current enabled job; using the full scenario matrix", name)
                return None
            selected_pairs.update(benchmark_pairs)

    if schema_changed:
        if not config_changed:
            logger.info("CI schema changed without a matching benchmark config change; using the full scenario matrix")
            return None
        base_schema = _load_git_json(logger, CI_SCHEMA_PATH)
        head_schema = _load_current_json(logger, CI_SCHEMA_PATH)
        if base_schema is None or head_schema is None:
            return None
        added_names = _isolated_benchmark_enum_additions(base_schema, head_schema, new_benchmark_names)
        if added_names is None:
            logger.info("CI schema changes extend beyond a new benchmark name; using the full scenario matrix")
            return None
        for added_name in added_names:
            added_pairs = {pair for pair in current_pairs if pair[0] == added_name}
            if not added_pairs:
                logger.info("New benchmark %s has no current enabled job; using the full scenario matrix", added_name)
                return None
            selected_pairs.update(added_pairs)

    if not selected_pairs:
        logger.info("Changed paths did not identify current scenarios; using the full scenario matrix")
        return None

    logger.info("Restricting CI scenarios to: %s", sorted(selected_pairs))
    return selected_pairs


def _apply_scenario_filter(logger, raw_jobs, selected_pairs):
    """Apply classified scenarios before userenvs are expanded into jobs."""
    if selected_pairs is None:
        return raw_jobs
    filtered_jobs = [
        job for job in raw_jobs
        if (job["benchmark"], job["endpoint"]) in selected_pairs
    ]
    if raw_jobs and not filtered_jobs:
        logger.info("No changed scenarios overlap the requested jobs; using the requested job matrix")
        return raw_jobs
    logger.info("Scenario filtering kept %d of %d raw jobs", len(filtered_jobs), len(raw_jobs))
    return filtered_jobs


def _get_runner_pool(logger, input_json):
    """
    Get the runner pool configuration from the CI config or fall back to legacy behavior.

    Args:
        logger: a logger instance used to output status
        input_json: the parsed CI config

    Returns:
        dict: pool config with 'provides' and 'labels' keys, or None if no pool matches
    """
    runner_pools = input_json.get("runner-pools", {})

    if args.runner_pool and args.runner_pool in runner_pools:
        pool = runner_pools[args.runner_pool]
        if not pool.get("enabled", True):
            logger.error("Requested runner pool '%s' is disabled" % (args.runner_pool))
            return None
        logger.info("Using runner pool '%s' with capabilities: %s" % (args.runner_pool, pool["provides"]))
        return pool

    if args.runner_pool and runner_pools:
        logger.error("Requested runner pool '%s' not found in config. Available pools: %s" % (args.runner_pool, list(runner_pools.keys())))
        return None

    if args.runner_pool:
        logger.error("Runner pool '%s' specified but no runner-pools config found" % (args.runner_pool))
        return None

    logger.error("No runner pool specified and no runner-pools config found")
    return None


def _get_effective_requirements(logger, scenario, endpoint_name, input_json):
    """
    Compute the effective requirements for a scenario + endpoint combination.

    Args:
        logger: a logger instance
        scenario: the scenario dict from the config
        endpoint_name: the endpoint being evaluated
        input_json: the full CI config

    Returns:
        set: the union of scenario requirements and endpoint requirements
    """
    scenario_reqs = set(scenario.get("requirements", []))
    endpoint_config = input_json.get("endpoints", {}).get(endpoint_name, {})
    endpoint_reqs = set(endpoint_config.get("requirements", []))
    effective = scenario_reqs | endpoint_reqs
    logger.debug("Effective requirements for endpoint '%s': scenario=%s + endpoint=%s = %s" % (
        endpoint_name, scenario_reqs, endpoint_reqs, effective))
    return effective


def get_jobs(logger, changed_files):
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

    capability_errors = _validate_capabilities(logger, input_json)
    if capability_errors:
        for err in capability_errors:
            logger.error(err)
        return None

    pool = _get_runner_pool(logger, input_json)
    if pool is None:
        return None

    current_pairs = _current_job_pairs(input_json, pool)
    selected_pairs = _classify_changed_scenarios(logger, changed_files, input_json, current_pairs)

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

                            _process_scenario(logger, scenario, benchmark, pool, input_json, raw_jobs)
                        else:
                            logger.debug("Scenario '%s' enabled is False" % (str(scenario)))
                else:
                    logger.debug("Benchmark '%s' is not included" % (benchmark["name"]))
            else:
                logger.debug("Benchmark '%s' enabled is False" % (benchmark["name"]))
    else:
        logger.debug("Global enabled is False")

    raw_jobs = _apply_scenario_filter(logger, raw_jobs, selected_pairs)

    if len(raw_jobs) == 0:
        runner_labels = pool["labels"]
        job = {
            "benchmark": args.benchmark,
            "enabled": False,
            "endpoint": "remotehosts",
            "runner_labels": runner_labels
        }
        raw_jobs.append(job)

    return raw_jobs


def _process_scenario(logger, scenario, benchmark, pool, input_json, raw_jobs):
    """Process a scenario using the capability-based requirements format."""
    for endpoint in scenario["endpoints"]:
        if args.endpoint != "all" and args.endpoint != endpoint:
            logger.debug("Skipping endpoint '%s' because it does not match requested endpoint '%s'" % (endpoint, args.endpoint))
            continue

        endpoint_config = input_json.get("endpoints", {}).get(endpoint, {})
        if not endpoint_config.get("enabled", True):
            logger.info("Skipping endpoint '%s' because it is disabled" % (endpoint))
            continue

        effective_reqs = _get_effective_requirements(logger, scenario, endpoint, input_json)
        pool_provides = set(pool.get("provides", []))

        if not effective_reqs.issubset(pool_provides):
            missing = effective_reqs - pool_provides
            logger.warning("Runner pool does not satisfy requirements for benchmark '%s' endpoint '%s': missing %s" % (benchmark["name"], endpoint, missing))
            continue

        logger.info("Adding job for benchmark '%s' endpoint '%s' (requirements satisfied: %s)" % (benchmark["name"], endpoint, effective_reqs))
        job = {
            "benchmark": benchmark["name"],
            "enabled": True,
            "endpoint": endpoint,
            "runner_labels": pool["labels"]
        }
        if "userenvs" in benchmark:
            # Retain benchmark-level image constraints until the matrix is
            # expanded, then remove this internal field from the output job.
            job["_userenvs"] = benchmark["userenvs"]
        raw_jobs.append(job)


def references_file(obj, target_path):
    """
    Recursively search a JSON object for any 'src' field whose value
    contains the target path (e.g., 'userenvs/requirement-sources/alma10.repo')

    Args:
        obj: A parsed JSON object (dict, list, or scalar)
        target_path: The relative path to search for in 'src' fields

    Returns:
        bool: True if any 'src' field references the target path
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "src" and isinstance(value, str) and target_path in value:
                return True
            if references_file(value, target_path):
                return True
    elif isinstance(obj, list):
        for item in obj:
            if references_file(item, target_path):
                return True
    return False

def get_changed_files(logger):
    """Read the PR changed paths once for both scenario and userenv selection."""
    result = run("git log HEAD^1", hide=True, warn=True)
    if result.exited != 0:
        logger.info("Rickshaw history is not available")
        return None

    logger.info("Rickshaw history is available")
    result = run("git diff --name-only HEAD^1 HEAD", hide=True, warn=True)
    if result.exited != 0:
        logger.warning("Could not read changed paths from HEAD^1 to HEAD; using the full CI matrix")
        return None

    changed_files = list(filter(None, result.stdout.split("\n")))
    logger.info("%d rickshaw files changed:\n%s" % (len(changed_files), "\n".join(changed_files)))
    return changed_files


def get_userenvs(logger, changed_files):
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

    include_all_testable_userenvs = changed_files is None
    if changed_files is not None:
        non_userenv_files_changed = list(filter(lambda x: not x.startswith("userenvs/"), changed_files)) # get the files that do not start with 'userenvs/'
        logger.info("%d rickshaw non-userenv files changed:\n%s" % (len(non_userenv_files_changed), "\n".join(non_userenv_files_changed)))

        if len(non_userenv_files_changed) > 0:
            include_all_testable_userenvs = True
            logger.info("Non userenv changes are present so reverting to normal behavior")
    else:
        # No usable history is available, so preserve the full userenv matrix.
        logger.info("Using all testable userenvs because changed paths are unavailable")

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
            elif file.startswith("userenvs/requirement-sources/"):
                logger.info("Found requirement-source change '%s', searching for referencing userenvs" % (file))
                for testable_userenv in testable_userenvs:
                    testable_userenv_path = rickshaw_dir + "/" + testable_userenv
                    try:
                        userenv_json, err = load_json_file(testable_userenv_path)
                        if userenv_json is not None and references_file(userenv_json, file):
                            if testable_userenv not in userenvs:
                                logger.info("Userenv '%s' references '%s', adding to test list" % (testable_userenv, file))
                                userenvs.append(testable_userenv)
                    except Exception as e:
                        logger.warning("Could not process '%s': %s" % (testable_userenv, str(e)))
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
            logger.info("Adding '%s' userenv", userenv)
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


def _get_job_userenvs(logger, raw_job, selected_userenvs):
    """Apply an optional benchmark allowlist or denylist to selected userenvs."""
    userenv_config = raw_job.get("_userenvs", {})
    allowlist = userenv_config.get("allowlist")
    denylist = userenv_config.get("denylist")

    if allowlist is not None:
        allowed = set(allowlist)
        job_userenvs = [userenv for userenv in selected_userenvs if userenv in allowed]
        logger.info(
            "Benchmark '%s' userenv allowlist selected %s from %s",
            raw_job["benchmark"], job_userenvs, selected_userenvs)
    elif denylist is not None:
        denied = set(denylist)
        job_userenvs = [userenv for userenv in selected_userenvs if userenv not in denied]
        logger.info(
            "Benchmark '%s' userenv denylist selected %s from %s",
            raw_job["benchmark"], job_userenvs, selected_userenvs)
    else:
        job_userenvs = list(selected_userenvs)

    return job_userenvs


def _expand_jobs(logger, raw_jobs, selected_userenvs, job_size):
    """Apply benchmark userenv restrictions and expand the final job matrix."""
    jobs = list()
    for raw_job in raw_jobs:
        job_userenvs = _get_job_userenvs(logger, raw_job, selected_userenvs)
        if job_size == "big" and job_userenvs:
            job_userenvs = [ ",".join(job_userenvs) ]

        for userenv in job_userenvs:
            job = copy.deepcopy(raw_job)
            job.pop("_userenvs", None)
            job["userenv"] = userenv
            jobs.append(job)
            logger.info("Adding job \"%s\"" % (job))

    return jobs

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

    changed_files = get_changed_files(logger)

    jobs = list()

    raw_jobs = get_jobs(logger, changed_files)
    if raw_jobs is None:
        return 1
    logger.info("Generated %d jobs:\n%s" % (len(raw_jobs), dump_json(raw_jobs)))

    userenvs = get_userenvs(logger, changed_files)
    if userenvs is None:
        return 1
    logger.info("Generated %d userenvs\n%s" % (len(userenvs), "\n".join(userenvs)))

    logger.info("Applying benchmark userenv restrictions and expanding jobs")
    jobs = _expand_jobs(logger, raw_jobs, userenvs, args.job_size)

    if len(jobs) == 0:
        # Keep a non-empty GitHub matrix when restrictions exclude every
        # globally selected userenv; the disabled row is skipped by CI.
        runner_labels = raw_jobs[0].get("runner_labels", []) if raw_jobs else []
        jobs.append({
            "benchmark": args.benchmark,
            "enabled": False,
            "endpoint": "remotehosts",
            "runner_labels": runner_labels,
            "userenv": "default"
        })

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
