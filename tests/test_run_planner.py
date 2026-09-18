import json
import tempfile
import unittest
from pathlib import Path

from rickshaw_lib.run_planner import PlannerLimits, RunPlanner


class FakeMultiplex:
    def expand_parameters(self, input_json, requirements_json=None, max_results=None):
        sets = input_json["sets"]
        expanded = []
        for parameter_set in sets:
            values = [param["vals"] for param in parameter_set.get("params", [])]
            if not values:
                expanded.append([])
                continue
            for value in values[0]:
                expanded.append([{"arg": "value", "val": value}])
        if max_results is not None:
            returned = expanded[:max_results]
        else:
            returned = expanded
        return {
            "sets": returned,
            "count": len(expanded),
            "returned": len(returned),
            "truncated": len(returned) < len(expanded),
        }


class RunPlannerTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        schema_dir = self.root / "schema"
        schema_dir.mkdir()
        with open(
            Path(__file__).parent.parent / "schema" / "run-file.json",
            encoding="utf-8",
        ) as source:
            (schema_dir / "run-file.json").write_text(source.read(), encoding="utf-8")
        config_dir = self.root / "config"
        config_dir.mkdir()
        (config_dir / "tool-params.json").write_text(
            '[{"tool": "sysstat"}, {"tool": "procstat"}]', encoding="utf-8"
        )
        self.benchmarks = self.root / "benchmarks"
        self.benchmarks.mkdir()
        (self.benchmarks / "fio").mkdir()
        self.planner = RunPlanner(self.root, FakeMultiplex(), self.benchmarks)

    def tearDown(self):
        self.tempdir.cleanup()

    @staticmethod
    def document():
        return {
            "benchmarks": [
                {
                    "name": "fio",
                    "ids": "1-2",
                    "mv-params": {"sets": [{"params": [{"arg": "size", "vals": ["1", "2"]}]}]},
                },
                {
                    "name": "fio",
                    "ids": "3",
                    "mv-params": {"sets": [{"params": [{"arg": "size", "vals": ["1"]}]}]},
                },
            ],
            "endpoints": [{"type": "remotehosts"}],
            "run-params": {"num-samples": 2, "test-order": "sample"},
        }

    def test_plan_preserves_duplicate_occurrences_and_index_aligned_total(self):
        result = self.planner.plan(self.document())

        self.assertTrue(result["validation"]["valid"])
        self.assertEqual(result["benchmarks"][0]["occurrence"], 0)
        self.assertEqual(result["benchmarks"][1]["occurrence"], 1)
        self.assertEqual(result["totals"]["global_iteration_count"], 2)
        self.assertEqual(result["totals"]["sample_execution_count"], 4)
        self.assertEqual(result["topology"]["endpoint_types"], ["remotehosts"])

    def test_plan_reports_bounded_parameter_prefix_and_exact_count(self):
        result = self.planner.plan(
            self.document(), PlannerLimits(max_parameter_sets=1)
        )

        self.assertEqual(result["benchmarks"][0]["parameter_sets"]["count"], 2)
        self.assertEqual(result["benchmarks"][0]["parameter_sets"]["returned"], 1)
        self.assertTrue(result["benchmarks"][0]["parameter_sets"]["truncated"])
        self.assertTrue(result["limits"]["truncated"])

    def test_plan_returns_structured_invalid_result(self):
        result = self.planner.plan({"benchmarks": "invalid"})

        self.assertFalse(result["validation"]["valid"])
        self.assertEqual(result["benchmarks"], [])
        self.assertEqual(result["validation"]["errors"][0]["code"], "invalid_input")

    def test_plan_uses_resolver_for_managed_benchmark_symlinks(self):
        managed = self.root / "managed" / "fio"
        managed.mkdir(parents=True)
        logical = self.benchmarks / "fio"
        logical.rmdir()
        logical.symlink_to(managed, target_is_directory=True)
        planner = RunPlanner(
            self.root,
            FakeMultiplex(),
            self.benchmarks,
            benchmark_resolver=lambda name: logical if name == "fio" else None,
        )

        result = planner.plan(self.document())

        self.assertTrue(result["validation"]["valid"])
        self.assertEqual(result["benchmarks"][0]["name"], "fio")

    def test_plan_reports_missing_installed_benchmark(self):
        document = self.document()
        document["benchmarks"][0]["name"] = "missing"

        result = self.planner.plan(document)

        self.assertFalse(result["validation"]["valid"])
        self.assertEqual(result["validation"]["errors"][0]["code"], "not_found")

    def test_plan_bounds_huge_engine_id_ranges_without_expanding_them(self):
        document = self.document()
        document["benchmarks"][0]["ids"] = "1-1000000000000"

        result = self.planner.plan(document, PlannerLimits(max_engine_ids=3))

        engine_ids = result["benchmarks"][0]["engine_ids"]
        self.assertEqual(engine_ids["count"], 1000000000000)
        self.assertEqual(engine_ids["items"], ["1", "2", "3"])
        self.assertTrue(engine_ids["truncated"])

    def test_plan_reports_unknown_endpoint_topology_without_failing(self):
        document = self.document()
        document["endpoints"] = [{}]

        result = self.planner.plan(document)

        self.assertTrue(result["validation"]["valid"])
        self.assertEqual(result["topology"]["confidence"], "unknown")
        self.assertEqual(
            result["topology"]["warnings"],
            ["one or more endpoint types are unavailable"],
        )

    def test_plan_reports_repository_default_tools_when_omitted(self):
        result = self.planner.plan(self.document())

        self.assertEqual(result["tools"]["mode"], "default")
        self.assertEqual(
            [entry["tool"] for entry in result["tools"]["entries"]],
            ["sysstat", "procstat"],
        )


if __name__ == "__main__":
    unittest.main()
