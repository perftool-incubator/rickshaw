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
                        choices = [ "benchmarks", "mv-params", "tool-params", "passthru-args", "tags", "endpoint", "endpoints" ])

    parser.add_argument("--index",
                        dest = "index",
                        help = "Crucible config index (e.g. 2 => endpoints[2]",
                        default = 0,
                        type = int)

    parser.add_argument("--benchmark",
                        dest = "benchmark",
                        help = "Crucible benchmark block to extract data from  (e.g. 'uperf')",
                        default = None,
                        type = str)

    args = parser.parse_args()
    return args

def dump_json(obj, key, idx, format = 'readable'):
    """Dump json in readable or parseable format"""
    # Parseable format has no indentation
    indentation = None
    sep = ':'
    if format == 'readable':
        indentation = 4
        sep += ' '

    try:
        blk = obj[key]
        if isinstance(obj[key], list):
            if key != 'tool-params':
                blk = blk[idx]
        
        json_str = json.dumps(blk, indent = indentation, separators = (',', sep),
                              sort_keys = False)
        return json_str
    except IndexError as err:
        err_msg = f"{ err }: Index { idx } does not exist in the config block { key }."
    except KeyError as err:
        err_msg = f"{ err }: Config block { key } does not exist."
    except Exception as err:
        err_msg = f"{ err }: An unexpected error occurred while processing JSON { obj }."
        
    print(err_msg)
    return None

def json_blk_to_file(endpoint_setting, json_filename):
    """Generate json file from endpoints setting block"""
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
    if cfg == 'endpoint':
        cfg = 'endpoints'

    try:
        # arrays w/ multiple key:value objects e.g. "endpoints" block
        if cfg == 'endpoints' and isinstance(json_obj[cfg], list):
            json_blk = json_obj[cfg][idx]
            endpoint_type = json_blk["type"]
            if not validate_schema(json_blk, "schema-" + endpoint_type + ".json"):
                return None
        else:
            # single object w/ key:value pairs e.g. "tags" block
            json_blk = json_obj[cfg]
    except IndexError as err:
        err_msg=(
            f"{ err }: Invalid index { idx }: Config block { cfg } has "
            f"{ len(json_obj[cfg]) } element(s)."
        )
    except KeyError as err:
        err_msg=f"{ err }: There is no 'type' key in { json_blk }"
    except Exception as err:
        err_msg=f"{ err }: Failed to parse config { cfg }, index { idx }."

    if err_msg is not None:
        print(f"ERROR: { err_msg }")
        return None

    for key in json_blk:
        if cfg == 'endpoints' and key == 'config':
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
                    # TODO: Handle endpoint type as key:val in rickshaw-run like the
                    # other args. Instead of: k8s,foo:bar ==> type:k8s,foo:bar
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

def get_bench_ids(json_obj, cfg):
    """Get benchmark names and ids from the benchmarks block"""
    stream=""
    if json_obj is None:
        return ""
    try:
        bench_count = len(json_obj[cfg])
        if bench_count > 0:
            for idx in range(0, bench_count):
                json_blk = json_obj[cfg][idx]
                stream+=json_blk["name"]+":"+json_blk["ids"]+","
            # remove last ","
            if len(stream)>0:
                stream = stream[:-1]
    except:
        pass

    return stream

def get_mv_params(input_json, name):
    """Extract mv-params from the benchmarks block"""
    try:
        benchmark_blk = next(d for d in input_json['benchmarks'] if d.get('name', None) == name)
    except:
        return None
    return benchmark_blk

def main():
    """Main function of get-json-config.py tool"""

    global args

    input_json = load_json_file(args.json_file)
    if input_json is None:
        return 1

    if not validate_schema(input_json):
        return 1

    match args.config:
        case "benchmarks":
            output = get_bench_ids(input_json, args.config)
        case "endpoints" | "endpoint" | "tags":
            # output is a stream of the endpoint or tags
            output = json_to_stream(input_json, args.config, args.index)
        case default:
            if args.config == "mv-params":
                input_json = get_mv_params(input_json, args.benchmark)
            # mv-params, tool-params, passthru-args
            output = dump_json(input_json, args.config, args.index)

    print(output)

if __name__ == "__main__":
    args = process_options()
    exit(main())

