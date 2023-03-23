
### JSON Run File

The "all-in-one" JSON run file enables test configuration by using a single
JSON file. Users can specify multi-value parameters, tools, tags, endpoint,
and passthru arguments in JSON format, everything in the same file.

For more details on the supported format, refer to the JSON [schema](JSON/schema.json).

## blockbreaker.py utility

The "blockbreaker.py" utility extracts a configuration block from the
"all-in-one" JSON run file and transforms into a single JSON block or stream.

## Usage
```
# python3 blockbreaker.py --json <json-run-file> --config <config>
```
Example:
```
# python3 blockbreaker.py --json run-file.json --config mv-params
```

For more options and usage run:
```
# python3 blockbreaker.py --help
```


