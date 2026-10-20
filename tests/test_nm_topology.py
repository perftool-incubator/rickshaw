"""N:M allocation and contact routing, including existing pairing behavior."""

import importlib.util
import json
import logging
from pathlib import Path
import shutil
import subprocess
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from test_endpoints_process_bench_roadblocks import import_endpoints
from test_remotehosts_tool_tags import import_remotehosts
from test_validate_only import import_rickshaw_run


ROOT = Path(__file__).resolve().parents[1]


def state_for(tmp_path, clients, servers, ratio="N:M", instances=None, server_config=True):
    state = import_rickshaw_run().RunState()
    state.bench_configs = {"cluster": {"client": {"client-server-ratio": ratio}}}
    if server_config:
        state.bench_configs["cluster"]["server"] = {"required": True}
    state.clients_servers = {
        "client": {str(eid): {} for eid in clients},
        "server": {str(eid): {} for eid in servers},
    }
    state.bench_instances = instances or [{
        "name": "cluster", "ids": {str(eid) for eid in clients + servers},
    }]
    state.config_dir = str(tmp_path)
    state.engine_config_dir = str(tmp_path)
    return state


@pytest.mark.parametrize("clients,servers", [([1, 2], [1, 2, 3]), ([1, 2, 3], [1, 2]), ([4, 7], [10, 11, 15])])
def test_nm_accepts_independent_counts_and_ids(tmp_path, clients, servers):
    state = state_for(tmp_path, clients, servers)
    state.validate_client_server_ids()
    state.compute_engine_pairing()
    state.write_engine_topology()
    topology = json.loads((tmp_path / "engine-topology.json").read_text())
    assert topology["0"]["client-ids"] == clients
    assert topology["0"]["server-ids"] == servers
    pairing = json.loads((tmp_path / "pairing.json").read_text())
    assert set(pairing) == {f"client-{eid}" for eid in clients} | {f"server-{eid}" for eid in servers}


@pytest.mark.parametrize("clients,servers,server_config", [([], [1], True), ([1], [], True), ([1], [2], False)])
def test_nm_rejects_missing_or_unsupported_roles(tmp_path, clients, servers, server_config):
    state = state_for(tmp_path, clients, servers, server_config=server_config)
    with pytest.raises(SystemExit) as exc:
        state.validate_client_server_ids()
    assert exc.value.code == 1


def test_nm_allows_optional_servers(tmp_path):
    state = state_for(tmp_path, [1, 2], [])
    state.bench_configs["cluster"]["server"]["required"] = False
    state.validate_client_server_ids()


@pytest.mark.parametrize("ratio,clients,servers,expected", [
    ("", [1, 2], [5, 6], {"client-1": "server-5", "server-5": "client-1", "client-2": "server-6", "server-6": "client-2"}),
    ("1:N", [1, 2], [3, 4, 5], {"server-3": "client-1", "server-4": "client-2", "server-5": "client-1", "client-1": "server-3", "client-2": "server-4"}),
])
def test_existing_pairing_is_preserved(tmp_path, ratio, clients, servers, expected):
    state = state_for(tmp_path, clients, servers, ratio)
    state.validate_client_server_ids()
    state.compute_engine_pairing()
    state.write_engine_topology()
    assert json.loads((tmp_path / "pairing.json").read_text()) == expected
    assert json.loads((tmp_path / "engine-topology.json").read_text()) == {}


def test_default_ratio_still_rejects_unequal_counts(tmp_path):
    with pytest.raises(SystemExit):
        state_for(tmp_path, [1, 2], [3], ratio="").validate_client_server_ids()


@pytest.mark.parametrize("endpoint_type", ["kube", "remotehosts", "osp"])
def test_deployment_passes_topology_manifest_to_endpoint(tmp_path, endpoint_type):
    state = state_for(tmp_path, [1, 2], [3, 4, 5])
    state.run = {"base-run-dir": str(tmp_path), "id": "test", "max-sample-failures": 1}
    state.rickshaw_project_dir = str(ROOT)
    state.base_endpoint_run_dir = str(tmp_path / "endpoint")
    state.endpoint_roadblock_opt = " --roadblock-id=test"
    state.endpoints = [{"type": endpoint_type, "label": f"{endpoint_type}-0", "opts": "run-file=run.json"}]
    runner = sys.modules[state.__class__.__module__]
    with patch.object(runner.subprocess, "Popen") as popen:
        state.deploy_endpoints()
    command = popen.call_args.args[0]
    expected = f"{state.config_dir}/engine-topology.json"
    if endpoint_type == "osp":
        assert f"engine-topology-file:{expected}" in command
    else:
        assert f"--engine-topology-file={expected}" in command
    for process, log_file in state.endpoint_processes:
        log_file.close()


def test_repeated_benchmark_instances_have_separate_contact_sets(tmp_path):
    state = state_for(tmp_path, [1, 4], [2, 3, 5, 6], instances=[
        {"name": "cluster", "ids": {"1", "2", "3"}},
        {"name": "cluster", "ids": {"4", "5", "6"}},
    ])
    state.validate_client_server_ids()
    state.write_engine_topology()
    topology = json.loads((tmp_path / "engine-topology.json").read_text())
    endpoints = import_endpoints()
    settings = {"engines": {"topology": topology}}
    assert endpoints.benchmark_topology_for_engine(settings, "server", "server-2")["client-ids"] == [1]
    assert endpoints.benchmark_topology_for_engine(settings, "server", "server-5")["client-ids"] == [4]
    assert endpoints.benchmark_topology_for_engine(settings, "server", "server-99") is None


def contact_message(server):
    return {"payload": {"sender": {"id": f"server-{server}"}, "message": {
        "command": "user-object", "user-object": {"svc": {"ip": f"192.0.2.{server}", "ports": [9042]}},
    }}}


def roadblock_message(recipient_type, recipient, command, payload):
    return [{"recipient": {"type": recipient_type, "id": recipient}, command: payload}]


def configure_routing(module, endpoints, tmp_path, clients, servers):
    topology = {"0": {"client-server-ratio": "N:M", "client-ids": clients, "server-ids": servers}}
    module.logger = logging.getLogger("test_nm_routing")
    module.endpoints = endpoints
    endpoints.create_roadblock_msg = roadblock_message
    endpoints.dump_json = json.dumps
    module.load_json_file = lambda path: (json.loads(Path(path).read_text()), None)
    module.settings = {"engines": {"topology": topology}}
    (tmp_path / "1-1-1:server-start-end.json").write_text(json.dumps({
        "received": [contact_message(eid) for eid in servers] + [contact_message(99)],
    }))
    tx = tmp_path / "tx"
    tx.mkdir()
    return tx


def assert_contacts(tx, clients, servers):
    messages = [json.loads(path.read_text())[0] for path in tx.glob("*.json")]
    assert len(messages) == len(clients) * len(servers)
    for client in clients:
        contacts = [msg["user-object"] for msg in messages if msg["recipient"]["id"] == f"client-{client}"]
        assert {obj["server-engine"] for obj in contacts} == {f"server-{eid}" for eid in servers}
        assert {obj["svc"]["ip"] for obj in contacts} == {f"192.0.2.{eid}" for eid in servers}


@pytest.mark.parametrize("clients,servers", [([1, 2], [3, 4, 5]), ([1, 2, 3], [4, 5])])
def test_remotehosts_relays_every_server_to_every_client(tmp_path, clients, servers):
    module = import_remotehosts()
    endpoints = import_endpoints()
    tx = configure_routing(module, endpoints, tmp_path, clients, servers)
    module.settings["engines"]["remotes"] = {"host": {"engines": [f"server-{eid}" for eid in servers]}}
    # Foreign service messages must be ignored rather than forwarded here.
    module.settings["engines"]["topology"]["1"] = {"client-server-ratio": "N:M", "client-ids": [9], "server-ids": [99]}
    module.test_start(str(tmp_path), "1-1-1", str(tx))
    assert_contacts(tx, clients, servers)


def import_kube(endpoints, tmp_path):
    toolbox = types.ModuleType("toolbox")
    toolbox.__path__ = []
    toolbox_json = types.ModuleType("toolbox.json")
    toolbox_json.json = json
    fabric = types.ModuleType("fabric")
    fabric.Connection = MagicMock()
    paramiko = types.ModuleType("paramiko")
    paramiko.ssh_exception = types.SimpleNamespace()
    (tmp_path / "python").mkdir()
    spec = importlib.util.spec_from_file_location("kube_nm_under_test", ROOT / "endpoints/kube/kube.py")
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"endpoints": endpoints, "fabric": fabric, "paramiko": paramiko, "toolbox": toolbox, "toolbox.json": toolbox_json}), patch.dict("os.environ", {"TOOLBOX_HOME": str(tmp_path)}):
        spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("clients,servers", [([1, 2], [3, 4, 5]), ([1, 2, 3], [4, 5])])
@pytest.mark.parametrize("network", ["other", "service", "nodeport", "mixed", "ingress-lb"])
def test_kube_relays_resolved_service_addresses(tmp_path, clients, servers, network):
    endpoints = import_endpoints()
    module = import_kube(endpoints, tmp_path)
    tx = configure_routing(module, endpoints, tmp_path, clients, servers)
    module.args = types.SimpleNamespace(endpoint_index=0)
    endpoint = {"host": "cluster", "user": "root", "namespace": {"name": "test"}}
    if network == "ingress-lb":
        endpoint["metallb-pool"] = "test-pool"
    module.settings["run-file"] = {"endpoints": [endpoint]}
    pods = {f"server-{eid}": {"pod-ip": f"192.0.2.{eid}" if network != "other" else "198.51.100.1", "node-ip": f"192.0.2.{eid}"} for eid in servers}
    if network == "service":
        pods.update({f"client-{eid}": {} for eid in clients})
    elif network == "mixed":
        pods[f"client-{clients[0]}"] = {}
    module.settings["engines"]["endpoint"] = {"pods": pods}
    module.settings["dirs"] = {"local": {"engine-conf": str(tmp_path), "crds": {"networking": str(tmp_path)}}}
    module.settings["misc"] = {"k8s-bin": "kubectl", "debug-output": False, "remote-env": {}}
    endpoints.remote_connection = MagicMock()
    endpoints.log_result = MagicMock()
    def remote_result(con, cmd, **kwargs):
        # Each mocked Service retains its server's advertised address.
        if " get svc/" in cmd:
            server = int(cmd.split("server-")[1].split()[0].split("-")[0])
            return types.SimpleNamespace(exited=0, stdout=json.dumps({
                "spec": {"clusterIP": f"192.0.2.{server}"},
                "status": {"loadBalancer": {"ingress": [{"ip": f"192.0.2.{server}"}]}},
            }))
        return types.SimpleNamespace(exited=0)
    endpoints.run_remote = remote_result
    def build_crd(kind, obj):
        return {"type": kind, "crd": obj, "validated": False, "created": False}
    module.build_network_crd_obj = build_crd
    module.service_status = MagicMock()
    module.test_start(str(tmp_path), "1-1-1", str(tx))
    assert_contacts(tx, clients, servers)


@pytest.mark.skipif(shutil.which("jq") is None, reason="OSP needs jq")
def test_osp_relays_only_owned_servers(tmp_path):
    topology = tmp_path / "engine-topology.json"
    topology.write_text(json.dumps({"0": {"client-server-ratio": "N:M", "client-ids": [1, 2], "server-ids": [3, 4, 99]}}))
    (tmp_path / "1-1-1:server-start-end.json").write_text(json.dumps({"received": [contact_message(eid) for eid in [3, 4, 99]]}))
    tx = tmp_path / "tx"
    tx.mkdir()
    source = (ROOT / "endpoints/osp/osp").read_text()
    function = source.split("function endpoint_osp_test_start() {", 1)[1].split("\nfunction osp_req_check()", 1)[0]
    script = "function endpoint_osp_test_start() {" + function + '\nmsgs_dir="$1"\nendpoint_run_dir="$1"\nengine_topology_file="$2"\nservers=(server-3 server-4)\nendpoint_osp_test_start "$1" 1-1-1 "$3"\n'
    subprocess.run(["bash", "-c", script, "osp-test", str(tmp_path), str(topology), str(tx)], check=True, capture_output=True, text=True)
    assert_contacts(tx, [1, 2], [3, 4])
