#!/usr/bin/python3

'''Utility to validate crucible run file JSON'''

from blockbreaker import load_json_file,validate_schema
import argparse

def process_options():
    """Handle the CLI argument parsing options"""

    parser = argparse.ArgumentParser(description = "Get a config block from crucible run file")

    parser.add_argument("--json",
                        dest = "json_file",
                        help = "Crucible run-file JSON",
                        required = True,
                        type = str)

    args = parser.parse_args()
    return args

def main():
    """Main function of the validate.py utility"""

    global args
    err_msg=None

    input_json = load_json_file(args.json_file)
    if input_json is None:
        err_msg=f"{ err }: Failed to load run-file JSON { args.json_file }"
        rc=1

    if not validate_schema(input_json):
        err_msg=(
            f"{ err }: Failed to validate run-file JSON "
            f"{ args.json_file } against schema."
        )
        rc=2

    for json_blk in input_json["endpoints"]:
        endpoint_type = json_blk["type"]
        if not validate_schema(json_blk, "schema-" + endpoint_type + ".json"):
            err_msg=(
                f"{ err }: Failed to validate the 'endpoints' block from "
                f" the JSON run-file { args.json_file } against the "
                f"{ endpoint_type }'s schema."
            )
            rc=3

    if err_msg is not None:
        print(f"ERROR: { err_msg }")
        return rc

    print(f"[ OK ] run-file JSON { args.json_file } validated!")

if __name__ == "__main__":
    args = process_options()
    exit(main())
