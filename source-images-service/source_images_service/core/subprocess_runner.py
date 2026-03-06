"""Thin wrapper around subprocess.run — port of Perl toolbox::run::run_cmd()."""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING

from source_images_service.core.exceptions import SubprocessError

if TYPE_CHECKING:
    from source_images_service.core.job_manager import Job

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    """Result of a subprocess invocation."""

    command: str
    return_code: int
    output: str


def run_cmd(
    cmd: str,
    *,
    job: Job | None = None,
    timeout: float | None = None,
    check: bool = False,
    cwd: str | None = None,
) -> RunResult:
    """Execute *cmd* in a shell and return a RunResult.

    Matches the Perl ``run_cmd()`` semantics: shell execution, stdout+stderr
    merged into a single output string.

    Parameters
    ----------
    cmd:
        The shell command string to execute.
    job:
        If provided, command and output are appended to the job log when
        the job's log level is ``debug``.  Timeout errors are always logged.
    timeout:
        Optional timeout in seconds.
    check:
        If True, raise SubprocessError on non-zero return code.
    cwd:
        Optional working directory for the subprocess.
    """
    logger.debug("run_cmd: %s", cmd)
    if job is not None:
        job.append_debug_log(f"run_cmd: {cmd}")

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or "") + (exc.stderr or "")
        msg = f"Command timed out after {timeout}s: {cmd}"
        if job is not None:
            job.append_log(msg)
        raise SubprocessError(msg, cmd=cmd, return_code=-1, output=output) from exc

    output = result.stdout + result.stderr
    run_result = RunResult(
        command=cmd,
        return_code=result.returncode,
        output=output,
    )

    if job is not None:
        job.append_debug_log(f"rc={run_result.return_code}")
        if run_result.output:
            job.append_debug_log(run_result.output)

    if check and run_result.return_code != 0:
        raise SubprocessError(
            f"Command failed (rc={run_result.return_code}): {cmd}",
            cmd=cmd,
            return_code=run_result.return_code,
            output=run_result.output,
        )

    return run_result
