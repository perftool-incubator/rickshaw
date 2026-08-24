# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import logging
import re

logger = logging.getLogger(__name__)


def expand_id_ranges(ids_str):
    """Expand an ids string (e.g. "1", "1-2", "1+3", "1-2+5-7") into a
    sorted list of individual id strings (e.g. ["1", "2", "3"]).
    Shared by rickshaw-run.py's assign_bench_ids()/load_bench_params()
    and rickshaw-post-process-bench.py's ids_to_benchmark mapping, so
    all of them agree on what an ids string means."""
    expanded = set()
    for segment in ids_str.split(","):
        for sub in segment.split("+"):
            m = re.match(r'^(\d+)-(\d+)$', sub)
            if m:
                for i in range(int(m.group(1)), int(m.group(2)) + 1):
                    expanded.add(str(i))
            elif re.match(r'^\d+$', sub):
                expanded.add(sub)
            else:
                logger.warning("ID range or number not recognized: %s", sub)
    return sorted(expanded, key=int)
