#!/usr/bin/python3

'''Utility to get a config block from the crucible run file'''

import os
import argparse
import sys
import json
import tempfile
from jsonschema import validate
from jsonschema import exceptions

def process_options():
    """Handle the CLI argument parsing options"""

    parser = argparse.ArgumentParser(description = "Get a config block from crucible run file")

    parser.add_argument("--json",
                        dest = "json_file",
                        help = "Crucible config settings json file (e.g. crucible-run.json)",
                        type = str)

    parser.add_argument("--config",
                        dest = "config",
                        required = True,
                        help = "Configuration type to get from the json file",
                        choices = [ "mv-params", "tool-params", "passthru-args", "tags", "endpoint" ])

    parser.add_argument("--index",
                        dest = "index",
                        help = "Crucible config index (e.g. 2 => endpoint[2]",
                        default = 0,
                        type = int)

    args = parser.parse_args()
    return args

def dump_json(obj, key, format = 'readable'):
    """Dump json in readable or parseable format"""
    # Parseable format has no indentation
    indentation = None
    sep = ':'
    if format == 'readable':
        indentation = 4
        sep += ' '

    try:
        json_str = json.dumps(obj[key], indent = indentation, separators = (',', sep),
                              sort_keys = False)
        return json_str
    except KeyError:
        return None

def json_blk_to_file(endpoint_setting, json_filename):
    """Generate json file from endpoint setting block"""
    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            f.write(endpoint_setting)
            f.close()
    except Exception as err:
         print("Unexpected error writing JSON file %s: %s" % (json_filename, err))
         return None
    return True

def load_json_file(json_file):
    """Load JSON file and return a json object"""
    try:
         input_fp = open(json_file, 'r')
         input_json = json.load(input_fp)
         input_fp.close()
    except FileNotFoundError as err:
         print("Could not find JSON file %s: %s" % (json_file, err))
         return None
    except IOError as err:
         print("Could not open/read JSON file %s: %s" % (json_file, err))
         return None
    except Exception as err:
         print("Unexpected error opening JSON file %s: %s" % (json_file, err))
         return None
    except JSONDecodeError as err:
         print("Decoding JSON file %s has failed: %s" % (json_file, err))
         return None
    except TypeError as err:
         print("JSON object type error: %s" % (err))
         return None
    return input_json

def json_to_stream(json_obj, cfg, idx):
    """Parse key:value from a JSON object/block and transform into a stream"""
    stream = ""
    err_msg = None
    if json_obj is None:
        return None

    try:
        # arrays w/ multiple key:value objects e.g. "endpoint" block
        if isinstance(json_obj[cfg], list):
            json_blk = json_obj[cfg][idx]
        else:
            # single object w/ key:value pairs e.g. "tags" block
            json_blk = json_obj[cfg]
    except IndexError as err:
        err_msg="{} => Invalid index: {} block has {} element(s).".format(
                        err, cfg, len(json_obj[cfg]))
    except Exception as err:
        err_msg="{} => Failed to convert block {} into stream.".format(
                        err, cfg)

    if err_msg is not None:
        print("ERROR: %s" % (err_msg))
        return None

    for key in json_blk:
        if cfg == 'endpoint' and key == 'config':
            for ecfg in range(0, len(json_blk[key])):
                # process targets e.g. 'client-1' for each config section
                tg_list = []
                targets = json_blk[key][ecfg]['targets']
                if isinstance(targets, str):
                    # alias for default: 'all' --> applies config to all
                    # target engines, but handle it as 'default'
                    if targets == 'all':
                        targets = 'default'
                    tg_list = [ targets ]
                elif isinstance(targets, list):
                    for tg in targets:
                        tg_name = tg['role'] + '-' + str(tg['ids'])
                        tg_list.append(tg_name)

                # get the individual sections e.g. 'securityContext'
                for st in json_blk[key][ecfg]['settings']:
                    st_val = json_blk[key][ecfg]['settings'][st]
                    if isinstance(st_val, dict):
                        # auto-detect json config blocks from settings and
                        # create inidividual json files e.g. securityContext
                        st_val_str = json.dumps(st_val)
                        st_blk = '\"' + st + '": ' + st_val_str
                        # create json file for each setting block
                        tf = tempfile.NamedTemporaryFile(prefix='__'+st+'__',
                                      suffix='.tmp.json', delete=False, dir=os.getcwd())
                        json_blk_to_file(st_blk, tf.name)
                        st_val = tf.name
                    for tg_name in tg_list:
                        stream += st + ':' + tg_name + ':' + str(st_val) + ','
        else:
            val = json_blk[key]
            if isinstance(val, list):
                for idx in range(len(val)):
                    item_val = val[idx]
                    stream += key + ':' + item_val + ','
            else:
                try:
                    val_str = str(val)
                    # TODO: Handle endpoint type in rickshaw like the other args
                    if key != 'type':
                        stream += key + ':'
                    stream += val_str + ','
                except:
                    raise Exception("Error: Unexpected object type %s" % (type(val)))
                    return None

    # remove last ","
    if len(stream)>0:
        stream = stream[:-1]

    return stream

def validate_schema(input_json, schema_file = None):
    """Validate json with schema file"""
    # schema_file defaults to general schema.json for the full run-file
    schema_path = "%s/JSON/" % (os.path.dirname(os.path.abspath(__file__)))
    schema_default = "schema.json"

    try:
        # use block sub-schema if schema_file is specified
        if (schema_file is None):
            schema_json = schema_path + schema_default
        else:
            schema_json = schema_path + schema_file
        schema_obj = load_json_file(schema_json)
        if schema_obj is None:
            return False
        validate(instance = input_json, schema = schema_obj)
    except Exception as err:
        print("JSON schema validation error: %s" % (err))
        return False
    return True

def main():
    """Main function of get-json-config.py tool"""

    global args

    input_json = load_json_file(args.json_file)
    if input_json is None:
        return 1

    if not validate_schema(input_json):
        return 1

    if args.config == "endpoint" or args.config == "tags":
        if args.config == "endpoint":
            try:
                endp = input_json[args.config][args.index]["type"]
            except:
                endp = "null"
                return 1
            finally:
                if not validate_schema(input_json[args.config][args.index],
                                        "schema-" + endp + ".json"):
                    return 1
        output = json_to_stream(input_json, args.config, args.index)
    else:
        output = dump_json(input_json, args.config)
    print(output)

if __name__ == "__main__":
    args = process_options()
    exit(main())

