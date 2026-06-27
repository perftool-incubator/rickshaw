#!/usr/bin/env python3
# -*- mode: python; indent-tabs-mode: nil; python-indent-level: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import argparse
import logging
import os
import stat
import subprocess
import sys
import time

from fabric import Connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("bootstrap")


def _connect(ssh_id_file, rickshaw_host):
    """Create a Fabric Connection with pre-configured paramiko client."""
    import paramiko
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=rickshaw_host,
        key_filename=ssh_id_file,
        look_for_keys=False,
        allow_agent=False,
        timeout=30,
    )
    conn = Connection(rickshaw_host)
    conn.client = client
    conn.transport = client.get_transport()
    return conn


def scp_from_controller(ssh_id_file, rickshaw_host, src, dest, max_retries=10):
    resolved_dest = dest
    if os.path.isdir(resolved_dest):
        resolved_dest = os.path.join(resolved_dest, os.path.basename(src))

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "SCP %s:%s -> %s (attempt %d/%d)",
                rickshaw_host, src, resolved_dest, attempt, max_retries,
            )
            conn = _connect(ssh_id_file, rickshaw_host)
            try:
                conn.get(src, local=resolved_dest)
            finally:
                conn.close()
            logger.info("SCP succeeded")
            return
        except Exception as exc:
            logger.warning(
                "SCP attempt %d/%d failed: [%s] %s",
                attempt, max_retries, type(exc).__name__, exc,
            )
            if attempt < max_retries:
                time.sleep(attempt)
    logger.error("Could not copy %s from %s after %d attempts", src, rickshaw_host, max_retries)
    sys.exit(1)


def main():
    sys.stdout = sys.stderr
    logger.info("Python bootstrap starting")
    logger.info("Python %s", sys.version)

    if os.path.exists("/etc/profile"):
        logger.info("Sourcing /etc/profile")
        try:
            import subprocess as _sp
            result = _sp.run(
                ["/bin/bash", "-c", "source /etc/profile && env -0"],
                capture_output=True, text=False,
            )
            if result.returncode == 0:
                for entry in result.stdout.split(b"\x00"):
                    if b"=" in entry:
                        key, _, val = entry.partition(b"=")
                        os.environ[key.decode()] = val.decode()
        except Exception as exc:
            logger.warning("Could not source /etc/profile: %s", exc)

    os.environ["PATH"] = "/usr/local/bin:" + os.environ.get("PATH", "")

    parser = argparse.ArgumentParser(description="Rickshaw engine bootstrap")
    parser.add_argument("--rickshaw-host", default="")
    parser.add_argument("--base-run-dir", default="")
    parser.add_argument("--endpoint-run-dir", default="")
    parser.add_argument("--cs-label", default="")
    parser.add_argument("--roadblock-server", default="")
    parser.add_argument("--roadblock-passwd", default="")
    parser.add_argument("--roadblock-id", default="")
    parser.add_argument("--disable-tools", default="")
    parser.add_argument("--endpoint", default="")
    parser.add_argument("--osruntime", default="")
    parser.add_argument("--max-sample-failures", default="")
    parser.add_argument("--engine-script-start-timeout", default="")
    parser.add_argument("--cpu-partitioning", default="")
    parser.add_argument("--cpu-partitions", default="")
    parser.add_argument("--cpu-partition-index", default="")

    args, _ = parser.parse_known_args()

    def _resolve(arg_val, *env_keys):
        """CLI arg wins if non-empty, otherwise fall back to env vars."""
        if arg_val:
            return arg_val
        for key in env_keys:
            val = os.environ.get(key, "")
            if val:
                return val
        return ""

    rickshaw_host = _resolve(
        args.rickshaw_host or args.roadblock_server,
        "rickshaw_host", "roadblock_server",
    )
    base_run_dir = _resolve(args.base_run_dir, "base_run_dir")
    cs_label = _resolve(args.cs_label, "cs_label")
    endpoint = _resolve(args.endpoint, "endpoint")
    endpoint_run_dir = _resolve(args.endpoint_run_dir, "endpoint_run_dir")
    roadblock_passwd = _resolve(args.roadblock_passwd, "roadblock_passwd")
    roadblock_id = _resolve(args.roadblock_id, "roadblock_id")
    disable_tools = _resolve(args.disable_tools, "disable_tools")
    osruntime = _resolve(args.osruntime, "osruntime")
    max_sample_failures = _resolve(args.max_sample_failures, "max_sample_failures")
    engine_script_start_timeout = _resolve(
        args.engine_script_start_timeout, "engine_script_start_timeout",
    )
    cpu_partitioning = _resolve(args.cpu_partitioning, "cpu_partitioning")
    cpu_partitions = _resolve(args.cpu_partitions, "cpu_partitions")
    cpu_partition_index = _resolve(args.cpu_partition_index, "cpu_partition_index")

    os.environ["rickshaw_host"] = rickshaw_host
    os.environ["base_run_dir"] = base_run_dir
    os.environ["cs_label"] = cs_label
    os.environ["endpoint"] = endpoint
    os.environ["endpoint_run_dir"] = endpoint_run_dir
    os.environ["roadblock_passwd"] = roadblock_passwd
    os.environ["roadblock_id"] = roadblock_id
    os.environ["disable_tools"] = disable_tools
    os.environ["osruntime"] = osruntime
    os.environ["max_sample_failures"] = max_sample_failures
    os.environ["engine_script_start_timeout"] = engine_script_start_timeout
    os.environ["cpu_partitioning"] = cpu_partitioning
    os.environ["config_dir"] = os.path.join(base_run_dir, "config")
    os.environ["engine_config_dir"] = os.path.join(
        base_run_dir, "config", "engine"
    )

    toolbox_home = os.environ.get("TOOLBOX_HOME", "/opt/toolbox")
    os.environ["TOOLBOX_HOME"] = toolbox_home

    # ---- CPU partitioning ----
    if cpu_partitioning == "1":
        logger.info("CPU partitioning: Enabled")

        environment = (
            "container" if endpoint in ("k8s", "kube") else "process"
        )

        discover_cmd = (
            "/usr/local/bin/discover-cpu-partitioning.py"
            " --environment=%s" % environment
        )
        logger.info("Discovering CPU isolation: %s", discover_cmd)
        result = subprocess.run(
            discover_cmd, shell=True, capture_output=True, text=True
        )
        logger.info(result.stdout)

        isolated_cpus = ""
        housekeeping_cpus = ""
        for line in result.stdout.splitlines():
            if "isolated" in line:
                isolated_cpus = line.split()[2] if len(line.split()) >= 3 else ""
            if "housekeeping" in line:
                housekeeping_cpus = line.split()[2] if len(line.split()) >= 3 else ""

        cpu_args = " ".join(
            "--cpu %s" % cpu
            for cpu in isolated_cpus.split(",")
            if cpu
        )
        partitions = cpu_partitions or "1"
        partition_index = cpu_partition_index or "1"
        partition_cmd = (
            "/usr/local/bin/partition-cpus.py"
            " --partitions %s --partition-index %s %s"
            % (partitions, partition_index, cpu_args)
        )
        logger.info("Partitioning CPUs: %s", partition_cmd)
        result = subprocess.run(
            partition_cmd, shell=True, capture_output=True, text=True
        )
        logger.info(result.stdout)

        partition_cpus = ""
        for line in result.stdout.splitlines():
            if "partition" in line:
                partition_cpus = line.split()[2] if len(line.split()) >= 3 else ""

        os.environ["HK_CPUS"] = housekeeping_cpus
        os.environ["WORKLOAD_CPUS"] = partition_cpus
        logger.info("HK_CPUS=%s", housekeeping_cpus)
        logger.info("WORKLOAD_CPUS=%s", partition_cpus)

        if housekeeping_cpus:
            subprocess.run(
                "taskset --cpu-list --pid %s %d"
                % (housekeeping_cpus, os.getpid()),
                shell=True,
            )
    else:
        logger.info("CPU partitioning: Disabled")

    # ---- Shared engine directory ----
    engines_shared = "/shared-engines-dir"
    os.makedirs(engines_shared, exist_ok=True)
    os.environ["ENGINES_SHARED_DIR"] = engines_shared

    touch_file = os.path.join(engines_shared, cs_label)
    open(touch_file, "w").close()

    # ---- SSH key setup ----
    ssh_id_file = os.path.join(engines_shared, "rickshaw_ssh_id")
    os.environ["ssh_id_file"] = ssh_id_file

    ssh_id_content = os.environ.get("ssh_id", "")
    if ssh_id_content:
        logger.info("Creating %s from $ssh_id environment variable", ssh_id_file)
        with open(ssh_id_file, "w") as fp:
            fp.write(ssh_id_content.replace("\\n", "\n"))
        os.chmod(ssh_id_file, stat.S_IRUSR | stat.S_IWUSR)

    if not os.path.exists(ssh_id_file):
        logger.error("SSH private key %s not found", ssh_id_file)
        sys.exit(1)

    # ---- Download settings ----
    config_dir = os.environ["config_dir"]
    json_settings_file = os.path.join(engines_shared, "rickshaw-settings.json.xz")
    os.environ["json_settings_file"] = json_settings_file

    scp_from_controller(
        ssh_id_file, rickshaw_host,
        os.path.join(config_dir, "rickshaw-settings.json.xz"),
        engines_shared,
    )
    if not os.path.exists(json_settings_file):
        logger.error("Could not find rickshaw-settings.json.xz")
        sys.exit(1)

    # ---- Download engine scripts ----
    engine_config_dir = os.environ["engine_config_dir"]

    scp_from_controller(
        ssh_id_file, rickshaw_host,
        os.path.join(engine_config_dir, "engine.py"),
        "/usr/local/bin/",
    )
    scp_from_controller(
        ssh_id_file, rickshaw_host,
        os.path.join(engine_config_dir, "engine_lib.py"),
        "/usr/local/bin/",
    )

    # Download roadblock module files
    scp_from_controller(
        ssh_id_file, rickshaw_host,
        os.path.join(engine_config_dir, "roadblocker.py"),
        "/usr/local/bin/",
    )
    scp_from_controller(
        ssh_id_file, rickshaw_host,
        os.path.join(engine_config_dir, "roadblocker_config.py"),
        "/usr/local/bin/",
    )
    scp_from_controller(
        ssh_id_file, rickshaw_host,
        os.path.join(engine_config_dir, "roadblock.py"),
        "/usr/local/bin/",
    )

    # Set ROADBLOCK_HOME so engine_lib can find the roadblock module
    os.environ["ROADBLOCK_HOME"] = "/usr/local/bin"

    # ---- Exec engine ----
    engine_script = "/usr/local/bin/engine.py"
    if os.path.exists(engine_script):
        logger.info("Execing %s", engine_script)
        os.execv(
            sys.executable,
            [sys.executable, engine_script] + sys.argv[1:],
        )
    else:
        logger.error("Could not find %s", engine_script)
        sys.exit(1)


if __name__ == "__main__":
    main()
