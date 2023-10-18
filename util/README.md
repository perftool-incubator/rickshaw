
# Crucible run-file

The Crucible run-file, a.k.a. "all-in-one" JSON, is the configuration file
used for running benchmarks. It consolidates all kinds of the settings from
the test run without the need of creating separated files for different
aspects of the test.

When a run-file JSON is used by Crucible command line, it enables test
configuration by using a single JSON file. Users can specify multi-value
parameters, tools, tags, endpoints, and run parameters, all using an unified
JSON format, in one single place.

For examples, refer to the [crucible-examples](https://github.com/perftool-incubator/crucible-examples/tree/main/runfile)
repository.

## Format
The config runfile structure is the following:
```
{
    "benchmarks": [
        {
            (...)
            "mv-params": {
                (...)
            }
        }
    ]
    "tool-params": [
        (...)
    ],
    "tags": {
        (...)
    },
    "endpoints": [
        (...)
    ],
    "run-params": {
        (...)
    }
}
```
where:
 * `benchmarks`: benchmark definitions
     * `mv-params`: benchmark specific params
 * `tool-params`: performance tool params, e.g. perf, sysstat, procstat, etc.
 * `tags`: tags to identify the test run in the form of "key":"value"
 * `endpoints`: endpoint definitions and configuration settings
 * `run-params`: addtional (optional) command line params, e.g. number of samples

For more details on the supported format, refer to the JSON [schema](JSON/schema.json).

## Usage

To validate a run-file, run:
```
python3 validate_run_file.py --json <run-file-json> 
```

To use a run-file JSON in Crucible, run:
```
crucible run --from-file <run-file-json>
```

### blockbreaker.py utility

The "blockbreaker.py" utility extracts a configuration block from the
run-file JSON and transforms into a JSON block or stream output.

Usage:
```
python3 blockbreaker.py --json <run-file-json> --config <config>
```
Example:
```
python3 blockbreaker.py --json run-file.json --config mv-params --benchmark iperf
```
For more options and usage run:
```
python3 blockbreaker.py --help
```
