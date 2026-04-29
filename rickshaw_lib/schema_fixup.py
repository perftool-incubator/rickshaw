# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import json
import logging
import os
import sys

from pathlib import Path

TOOLBOX_HOME = os.environ.get("TOOLBOX_HOME")
if TOOLBOX_HOME:
    sys.path.append(str(Path(TOOLBOX_HOME) / "python"))

from toolbox.json import load_json_file, validate_schema

logger = logging.getLogger(__name__)


def rickshaw_run_schema_fixup(result_file, result_schema_file):
    """Migrate old rickshaw-run JSON data to match the current schema.

    Port of rickshaw::fixup Perl module. Handles backwards-incompatible
    schema changes by adjusting the data in-place.

    Args:
        result_file: path to the rickshaw-run.json file
        result_schema_file: path to the rickshaw-run JSON schema

    Returns:
        0 on success, non-zero on failure
    """
    result_data, err = load_json_file(result_file)
    if result_data is None:
        # Try .xz variant
        xz_file = result_file + ".xz"
        if os.path.exists(xz_file):
            result_data, err = load_json_file(xz_file, uselzma=True)

    if result_data is None:
        logger.error("Could not open rickshaw-run data for schema fixup: %s", err)
        return 1

    logger.info("Checking if the rickshaw-run data matches the current JSON schema")

    schema_data, schema_err = load_json_file(result_schema_file)
    if schema_data is None:
        logger.error("Could not load schema: %s", schema_err)
        return 1

    try:
        validate_schema(schema_data, result_data)
        return 0
    except Exception:
        pass

    logger.info("Attempting to update the rickshaw-run data to match the current JSON schema")

    # Force numeric fields to be integers
    for field in ("max-rb-attempts", "max-sample-failures", "num-samples"):
        if field in result_data and isinstance(result_data[field], str):
            try:
                result_data[field] = int(result_data[field])
            except (ValueError, TypeError):
                pass

    try:
        validate_schema(schema_data, result_data)
    except Exception as e:
        logger.error(
            "Unable to validate rickshaw-run data after schema fixup: %s", e
        )
        return 1

    # Write the fixed data back
    try:
        with open(result_file, "w") as f:
            json.dump(result_data, f, indent=2, sort_keys=True)
            f.write("\n")
        logger.info("Updated rickshaw-run file to match current JSON schema")
    except OSError as e:
        logger.error("Unable to write updated rickshaw-run file: %s", e)
        return 1

    return 0
