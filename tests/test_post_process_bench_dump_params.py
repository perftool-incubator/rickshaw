#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

"""Unit tests for rickshaw-post-process-bench.py's dump_params().

This is a second, independently-maintained "port of the Perl
dump_params() function" (rickshaw-run.py has its own), and it was not
covered by rickshaw-run.py's id-scoping fix for duplicate benchmark
names (see test_load_bench_params_duplicate_names.py): a param whose
"id" is auto-scoped to a "+"-joined set (e.g. "1+2" for a duplicate
instance covering ids 1 and 2) was being compared with
`str(param_id) != str(cs_id)`, which is always True for a joined set,
so every such param was silently dropped during post-processing. This
covers the fix (splitting on "+", matching rickshaw-run.py's
dump_params()) and confirms it via the shared rickshaw_lib.id_ranges
helper both scripts now use to build "id" values consistently.

toolbox is mocked out rather than required, since rickshaw-post-process-
bench.py imports from it at module scope and CI does not check toolbox
out for this job.
"""

import importlib.machinery
import importlib.util
import logging
import os
import sys
import types
import unittest


def import_post_process_bench():
    """Load rickshaw-post-process-bench.py as a module with toolbox mocked out."""
    mock_json = types.ModuleType("toolbox.json")
    mock_json.load_json_file = lambda *a, **k: (None, "not used in this test")
    mock_json.validate_schema = lambda *a, **k: (True, None)

    mock_logging_mod = types.ModuleType("toolbox.logging")
    mock_logging_mod.setup_logging = lambda *a, **k: None

    mock_run = types.ModuleType("toolbox.run")
    mock_run.run_cmd = lambda *a, **k: ("cmd", "", 0)

    mock_parallel = types.ModuleType("toolbox.parallel")
    mock_parallel.run_parallel_jobs = lambda *a, **k: None
    mock_parallel.get_max_workers = lambda *a, **k: 1

    mock_toolbox = types.ModuleType("toolbox")
    mock_toolbox.json = mock_json
    mock_toolbox.logging = mock_logging_mod
    mock_toolbox.run = mock_run
    mock_toolbox.parallel = mock_parallel

    mod_name = "post_process_bench_under_test"
    sys.modules.pop(mod_name, None)

    mocks = {
        "toolbox": mock_toolbox,
        "toolbox.json": mock_json,
        "toolbox.logging": mock_logging_mod,
        "toolbox.run": mock_run,
        "toolbox.parallel": mock_parallel,
    }
    saved = {key: sys.modules.get(key) for key in mocks}
    sys.modules.update(mocks)

    try:
        script_path = os.path.join(os.path.dirname(__file__), "..", "rickshaw-post-process-bench.py")
        loader = importlib.machinery.SourceFileLoader(mod_name, script_path)
        spec = importlib.util.spec_from_loader(mod_name, loader)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)
    finally:
        for key, val in saved.items():
            if val is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = val

    return mod


class TestDumpParams(unittest.TestCase):
    def setUp(self):
        self.mod = import_post_process_bench()

    def test_single_id_matches_only_that_engine_id(self):
        ids_to_benchmark = {"1": "oslat", "2": "oslat"}
        params = [{"arg": "duration", "val": "10", "role": "all", "benchmark": "oslat", "id": "1"}]
        result, _ = self.mod.dump_params(params, "1", "client", ids_to_benchmark)
        self.assertEqual(result, "--duration=10")
        result, _ = self.mod.dump_params(params, "2", "client", ids_to_benchmark)
        self.assertEqual(result, "")

    def test_plus_joined_id_set_matches_any_member(self):
        # load_bench_params() auto-scopes a duplicate-name benchmark
        # instance's params to a "+"-joined set of its own ids (e.g.
        # "1+2" for ids="1-2", expanded via rickshaw_lib.id_ranges) --
        # any of those engine ids must match, not just the first.
        ids_to_benchmark = {"1": "oslat", "2": "oslat", "3": "oslat"}
        joined_id = "+".join(self.mod.expand_id_ranges("1-2"))
        params = [{"arg": "duration", "val": "10", "role": "all", "benchmark": "oslat", "id": joined_id}]
        result, _ = self.mod.dump_params(params, "1", "client", ids_to_benchmark)
        self.assertEqual(result, "--duration=10")
        result, _ = self.mod.dump_params(params, "2", "client", ids_to_benchmark)
        self.assertEqual(result, "--duration=10")
        result, _ = self.mod.dump_params(params, "3", "client", ids_to_benchmark)
        self.assertEqual(result, "")

    def test_absent_id_still_applies_to_every_engine_id(self):
        ids_to_benchmark = {"1": "oslat", "2": "oslat"}
        params = [{"arg": "duration", "val": "10", "role": "all", "benchmark": "oslat"}]
        result, _ = self.mod.dump_params(params, "1", "client", ids_to_benchmark)
        self.assertEqual(result, "--duration=10")
        result, _ = self.mod.dump_params(params, "2", "client", ids_to_benchmark)
        self.assertEqual(result, "--duration=10")

    def test_benchmark_filtering_excludes_non_matching_benchmark(self):
        params = [
            {"arg": "fio-only", "val": "1", "role": "client", "benchmark": "fio"},
            {"arg": "uperf-only", "val": "1", "role": "client", "benchmark": "uperf"},
        ]
        result, _ = self.mod.dump_params(params, "1", "client", {"1": "fio"})
        self.assertEqual(result, "--fio-only=1")


if __name__ == "__main__":
    unittest.main()
