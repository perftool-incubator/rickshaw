
# Crucible run-file

The Crucible run-file, a.k.a. "all-in-one" JSON, is the configuration file
used for running benchmarks. It consolidates all kinds of the settings from
the test run without the need of creating separated files for different
aspects of the test.

When a run-file JSON is used by Crucible command line, it enables test
configuration by using a single JSON file. Users can specify multi-value
parameters, tools, tags, endpoints, and run parameters, all using an unified
JSON format, in one single place.

For more details on the supported format, refer to the JSON [schema](JSON/schema.json).

To validate a run-file, run:
```
python3 validate_run_file.py --json <run-file-json> 
```

To use a run-file JSON in Crucible, run:
```
crucible run --from-file <run-file-json>
```

## blockbreaker.py utility

The "blockbreaker.py" utility extracts a configuration block from the
run-file JSON and transforms into a JSON block or stream output.

Usage:
```
python3 blockbreaker.py --json <run-file-json> --config <config>
```
Example:
```
python3 blockbreaker.py --json run-file.json --config mv-params
```
For more options and usage run:
```
python3 blockbreaker.py --help
```
