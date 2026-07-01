#!/usr/bin/python3

import argparse
import copy
import logging
from pathlib import Path

import sys
import os
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
from toolbox.system_cpu_topology import *

class t_global(object):
    system_cpus = None
    args = None
    log_debug_format =    '[%(asctime)s %(levelname)s %(module)s %(funcName)s:%(lineno)d] %(message)s'
    log_verbose_format =  '[%(asctime)s %(levelname)s] %(message)s'
    log_normal_format =   '%(message)s'
    log = None

def process_options():
    parser = argparse.ArgumentParser(description="Extract a partition from a supplied list of CPUs")

    parser.add_argument("--log-level",
                        dest = "log_level",
                        help = "Control how much logging output should be generated",
                        default = "normal",
                        choices = [ "normal", "verbose", "debug" ])

    parser.add_argument("--partitions",
                        dest = "partitions",
                        help = "How many partitions should be created?",
                        default = 1,
                        type = int)

    parser.add_argument("--partition-index",
                        dest = "partition_index",
                        help = "Which partition is being discovered?",
                        default = 1,
                        type = int)

    parser.add_argument("--cpu",
                        dest = "cpu_list",
                        help = "One or more CPUs to order",
                        default = [],
                        action = 'append',
                        type = int)

    t_global.args = parser.parse_args()

    if t_global.args.log_level == 'debug':
        logging.basicConfig(level = logging.DEBUG, format = t_global.log_debug_format, stream = sys.stdout)
    elif t_global.args.log_level == 'verbose':
        logging.basicConfig(level = logging.INFO, format = t_global.log_verbose_format, stream = sys.stdout)
    elif t_global.args.log_level == 'normal':
        logging.basicConfig(level = logging.INFO, format = t_global.log_normal_format, stream = sys.stdout)

    t_global.log = logging.getLogger(__file__)

    return(0)

def output_cpu_info(label, cpu_list):
    t_global.log.debug("%s cpus: %d" % (label, len(cpu_list)))
    short_cpu_list = system_cpu_topology.formatted_cpu_list(cpu_list)
    formatted_short_cpu_list = ','.join(short_cpu_list)
    t_global.log.debug("%s cpus: %s" % (label, formatted_short_cpu_list))

    cpu_list = ','.join(map(str, cpu_list))
    t_global.log.info("%s cpus: %s" % (label, cpu_list))

    t_global.log.info("")

    return(0)

def main():
    process_options()

    t_global.system_cpus = system_cpu_topology(log = t_global.log)

    partition = []

    if t_global.args.partitions == 1:
        partition = copy.deepcopy(t_global.args.cpu_list)
    else:
        isolated_cpus = copy.deepcopy(t_global.args.cpu_list)

        cores = []

        while len(isolated_cpus):
            core = []
            cpu_thread = isolated_cpus.pop()
            cpu_siblings = t_global.system_cpus.get_thread_siblings(cpu_thread)

            core.append(cpu_thread)
            for sibling in cpu_siblings:
                try:
                    isolated_cpus.remove(sibling)

                    # if we get here without an exception then the
                    # sibling needs to processed
                    core.append(sibling)
                except ValueError as e:
                    pass

            cores.append(core)

        while len(cores) and len(cores) % t_global.args.partitions != 0:
            core = cores.pop()
            t_global.log.debug("dropping core with cpu threads %s from the cpus considered for partitions to equalize partition sizes" % (core))

        partition_size = int(len(cores) / t_global.args.partitions)
        t_global.log.debug("partition_size=%d" % (partition_size))

        partition_start_index = partition_size * (t_global.args.partition_index - 1)
        t_global.log.debug("partition_start_index=%d" % (partition_start_index))
        partition_stop_index = partition_start_index + partition_size - 1
        t_global.log.debug("partition_stop_index=%d" % (partition_stop_index))

        for core_idx in range(partition_start_index, partition_stop_index + 1):
            t_global.log.debug("adding core with cpu threads %s at index %d to partition" % (cores[core_idx], core_idx))
            for cpu in cores[core_idx]:
                partition.append(cpu)

    output_cpu_info("partition", partition)

    return(0)

if __name__ == "__main__":
    exit(main())
