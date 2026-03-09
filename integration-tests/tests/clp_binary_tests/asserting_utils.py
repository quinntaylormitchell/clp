"""Utilities that raise pytest assertions on failure."""

import logging
import subprocess
from pathlib import Path
from typing import Any

import pytest

logger = logging.getLogger(__name__)


def run_and_capture(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    """
    Runs a command and captures its stdout and stderr.

    :param request: Pytest fixture request.
    :param cmd: Command and arguments to execute.
    :param kwargs: Additional keyword arguments passed through to the subprocess.
    :raise: Propagates `subprocess.run`'s errors.
    :return: A CompletedProcess instance.
    """
    log_debug_msg = f"Running command (capturing output): {cmd}"
    logger.debug(log_debug_msg)

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        **kwargs,
    )


def run_and_log_to_file(request: pytest.FixtureRequest, cmd: list[str], **kwargs: Any) -> None:
    """
    Runs a command and logs its stdout and stderr to the unique log file for this test run.

    :param request: Pytest fixture request.
    :param cmd: Command and arguments to execute.
    :param kwargs: Additional keyword arguments passed through to the subprocess.
    :raise: Propagates `subprocess.run`'s errors.
    """
    log_debug_msg = f"Running command (logging output): {cmd}"
    logger.debug(log_debug_msg)

    log_file_path = Path(request.config.getini("log_file_path"))
    with log_file_path.open("ab") as log_file:
        subprocess.run(cmd, stdout=log_file, stderr=log_file, text=True, check=True, **kwargs)
