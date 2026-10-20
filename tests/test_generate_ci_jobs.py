import copy
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


def _load_generator_module():
    """Load the script with small dependency stubs for its CI helpers."""
    toolbox_home = tempfile.TemporaryDirectory()
    (Path(toolbox_home.name) / "python").mkdir()
    old_toolbox_home = os.environ.get("TOOLBOX_HOME")
    os.environ["TOOLBOX_HOME"] = toolbox_home.name

    invoke_module = types.ModuleType("invoke")
    invoke_module.run = lambda *args, **kwargs: SimpleNamespace(exited=1, stdout="")
    toolbox_module = types.ModuleType("toolbox")
    toolbox_module.__path__ = []
    toolbox_json_module = types.ModuleType("toolbox.json")
    toolbox_json_module.__all__ = ["json", "load_json_file", "validate_schema"]
    toolbox_json_module.json = json
    toolbox_json_module.load_json_file = lambda path: (None, "not used by these tests")
    toolbox_json_module.validate_schema = lambda *args: (True, None)

    replaced_modules = {
        name: sys.modules.get(name)
        for name in ("invoke", "toolbox", "toolbox.json")
    }
    sys.modules["invoke"] = invoke_module
    sys.modules["toolbox"] = toolbox_module
    sys.modules["toolbox.json"] = toolbox_json_module

    try:
        script_path = Path(__file__).parents[1] / "util" / "generate-ci-jobs.py"
        spec = importlib.util.spec_from_file_location("generate_ci_jobs_under_test", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module, toolbox_home
    finally:
        for name, old_module in replaced_modules.items():
            if old_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old_module
        if old_toolbox_home is None:
            os.environ.pop("TOOLBOX_HOME", None)
        else:
            os.environ["TOOLBOX_HOME"] = old_toolbox_home


generator, _toolbox_home = _load_generator_module()


class NullLogger:
    def info(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class GenerateCIJobsScenarioFilterTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.logger = NullLogger()
        generator.rickshaw_dir = str(self.root)
        (self.root / "util" / "crucible-ci").mkdir(parents=True)
        (self.root / "schema").mkdir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write_json(self, relative_path, value):
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def _base_run(self, base_files):
        def fake_run(command, **kwargs):
            prefix = "git show HEAD^1:"
            if command.startswith(prefix):
                path = command[len(prefix):]
                if path in base_files:
                    return SimpleNamespace(exited=0, stdout=json.dumps(base_files[path]))
            return SimpleNamespace(exited=1, stdout="")

        return patch.object(generator, "run", side_effect=fake_run)

    @staticmethod
    def _config(benchmarks):
        return {
            "config": {"enabled": True},
            "capabilities": {"k8s-cluster": "Kubernetes", "remotehosts-access": "SSH"},
            "endpoints": {
                "kube": {"enabled": True, "requirements": ["k8s-cluster"]},
                "remotehosts": {"enabled": True, "requirements": ["remotehosts-access"]},
            },
            "runner-pools": {
                "pool": {
                    "enabled": True,
                    "provides": ["k8s-cluster", "remotehosts-access"],
                    "labels": ["self-hosted"],
                }
            },
            "benchmarks": benchmarks,
        }

    @staticmethod
    def _benchmark(name, endpoints=("kube", "remotehosts")):
        return {
            "name": name,
            "enabled": True,
            "scenarios": [{"enabled": True, "endpoints": list(endpoints), "requirements": []}],
        }

    @staticmethod
    def _schema(names):
        return {
            "properties": {
                "benchmarks": {
                    "items": {"properties": {"name": {"enum": list(names)}}}
                }
            },
            "description": "CI schema",
        }

    def test_scenario_files_select_one_pair_and_union_multiple_pairs(self):
        config = self._config([self._benchmark("fio"), self._benchmark("oslat")])
        paths = [
            "util/crucible-ci/fio.kube.json",
            "util/crucible-ci/oslat.remotehosts.json",
        ]
        for path in paths:
            self._write_json(path, {})

        current_pairs = generator._current_job_pairs(config, config["runner-pools"]["pool"])
        selected = generator._classify_changed_scenarios(
            self.logger, paths, config, current_pairs)

        self.assertEqual(selected, {("fio", "kube"), ("oslat", "remotehosts")})

    def test_benchmark_entry_change_selects_only_its_current_jobs(self):
        base_config = self._config([self._benchmark("fio"), self._benchmark("oslat")])
        head_config = copy.deepcopy(base_config)
        head_config["benchmarks"][0]["userenvs"] = {"allowlist": ["alma9"]}
        current_pairs = generator._current_job_pairs(head_config, head_config["runner-pools"]["pool"])

        with self._base_run({"util/crucible-ci.json": base_config}):
            selected = generator._classify_changed_scenarios(
                self.logger, ["util/crucible-ci.json"], head_config, current_pairs)

        self.assertEqual(selected, {("fio", "kube"), ("fio", "remotehosts")})

    def test_config_ordering_is_ignored_when_another_path_selects_scenarios(self):
        base_config = self._config([self._benchmark("fio"), self._benchmark("oslat")])
        head_config = copy.deepcopy(base_config)
        head_config["benchmarks"].reverse()
        scenario_path = "util/crucible-ci/fio.kube.json"
        self._write_json(scenario_path, {})
        current_pairs = generator._current_job_pairs(head_config, head_config["runner-pools"]["pool"])

        with self._base_run({"util/crucible-ci.json": base_config}):
            selected = generator._classify_changed_scenarios(
                self.logger,
                ["util/crucible-ci.json", scenario_path],
                head_config,
                current_pairs,
            )

        self.assertEqual(selected, {("fio", "kube")})

    def test_matching_new_benchmark_and_isolated_schema_enum_addition_are_narrowed(self):
        base_config = self._config([self._benchmark("fio")])
        head_config = copy.deepcopy(base_config)
        head_config["benchmarks"].append(self._benchmark("newbench", ("kube",)))
        base_schema = self._schema(["fio"])
        head_schema = self._schema(["newbench", "fio"])
        self._write_json("schema/crucible-ci.json", head_schema)
        current_pairs = generator._current_job_pairs(head_config, head_config["runner-pools"]["pool"])

        with self._base_run({
            "util/crucible-ci.json": base_config,
            "schema/crucible-ci.json": base_schema,
        }):
            selected = generator._classify_changed_scenarios(
                self.logger,
                ["util/crucible-ci.json", "schema/crucible-ci.json"],
                head_config,
                current_pairs,
            )

        self.assertEqual(selected, {("newbench", "kube")})

    def test_unclassified_mixed_shared_and_unavailable_changes_use_full_matrix(self):
        base_config = self._config([self._benchmark("fio"), self._benchmark("oslat")])
        current_pairs = generator._current_job_pairs(base_config, base_config["runner-pools"]["pool"])
        scenario_path = "util/crucible-ci/fio.kube.json"
        self._write_json(scenario_path, {})

        selected = generator._classify_changed_scenarios(
            self.logger, ["rickshaw-run.py"], base_config, current_pairs)
        self.assertIsNone(selected)

        selected = generator._classify_changed_scenarios(
            self.logger, [scenario_path, "README.md"], base_config, current_pairs)
        self.assertIsNone(selected)

        head_config = copy.deepcopy(base_config)
        head_config["runner-pools"]["pool"]["labels"].append("new-runner")
        with self._base_run({"util/crucible-ci.json": base_config}):
            selected = generator._classify_changed_scenarios(
                self.logger, ["util/crucible-ci.json"], head_config, current_pairs)
        self.assertIsNone(selected)

        self.assertIsNone(generator._classify_changed_scenarios(
            self.logger, None, base_config, current_pairs))

        with patch.object(generator, "run", return_value=SimpleNamespace(exited=1, stdout="")):
            self.assertIsNone(generator.get_changed_files(self.logger))

    def test_schema_changes_beyond_matching_enum_addition_use_full_matrix(self):
        base_config = self._config([self._benchmark("fio")])
        head_config = copy.deepcopy(base_config)
        head_config["benchmarks"].append(self._benchmark("newbench", ("kube",)))
        base_schema = self._schema(["fio"])
        head_schema = self._schema(["fio", "newbench"])
        head_schema["description"] = "Broader schema changes"
        self._write_json("schema/crucible-ci.json", head_schema)
        current_pairs = generator._current_job_pairs(head_config, head_config["runner-pools"]["pool"])

        with self._base_run({
            "util/crucible-ci.json": base_config,
            "schema/crucible-ci.json": base_schema,
        }):
            selected = generator._classify_changed_scenarios(
                self.logger,
                ["util/crucible-ci.json", "schema/crucible-ci.json"],
                head_config,
                current_pairs,
            )

        self.assertIsNone(selected)

    def test_deleted_or_unmapped_scenario_uses_full_matrix(self):
        config = self._config([self._benchmark("fio")])
        current_pairs = generator._current_job_pairs(config, config["runner-pools"]["pool"])

        selected = generator._classify_changed_scenarios(
            self.logger, ["util/crucible-ci/oldbench.kube.json"], config, current_pairs)
        self.assertIsNone(selected)

        self._write_json("util/crucible-ci/fio.kube.json", {})
        selected = generator._classify_changed_scenarios(
            self.logger,
            ["util/crucible-ci/fio.kube.json"],
            config,
            {("fio", "remotehosts")},
        )
        self.assertIsNone(selected)

    def test_scenario_filter_composes_with_userenv_restrictions_and_job_size(self):
        raw_jobs = [
            {
                "benchmark": "fio",
                "endpoint": "kube",
                "enabled": True,
                "_userenvs": {"allowlist": ["alma9", "external"]},
            },
            {
                "benchmark": "fio",
                "endpoint": "remotehosts",
                "enabled": True,
                "_userenvs": {"allowlist": ["alma9", "external"]},
            },
            {
                "benchmark": "oslat",
                "endpoint": "kube",
                "enabled": True,
                "_userenvs": {"denylist": ["external"]},
            },
        ]
        narrowed = generator._apply_scenario_filter(
            self.logger, raw_jobs, {("fio", "kube")})
        selected_userenvs = ["default", "alma9", "external"]

        small_jobs = generator._expand_jobs(
            self.logger, narrowed, selected_userenvs, "small")
        big_jobs = generator._expand_jobs(
            self.logger, narrowed, selected_userenvs, "big")

        self.assertEqual([job["userenv"] for job in small_jobs], ["alma9", "external"])
        self.assertEqual(len(big_jobs), 1)
        self.assertEqual(big_jobs[0]["userenv"], "alma9,external")
        self.assertTrue(all(job["benchmark"] == "fio" and job["endpoint"] == "kube"
                            for job in small_jobs + big_jobs))
        self.assertTrue(all("_userenvs" not in job for job in small_jobs + big_jobs))


if __name__ == "__main__":
    unittest.main()
