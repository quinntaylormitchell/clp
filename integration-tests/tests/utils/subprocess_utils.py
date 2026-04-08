"""Utilities for subprocess management."""

import logging
import subprocess
from pathlib import Path

from tests.utils.classes import IntegrationTestExternalAction
from tests.utils.logging_utils import log_action_output_to_file

DEFAULT_CMD_TIMEOUT_SECONDS = 120.0
logger = logging.getLogger(__name__)


def execute_external_action(external_action: IntegrationTestExternalAction) -> None:
    """
    Executes an external action by running its respective `cmd` with `run_subprocess`. Stores the
    completed subprocess in the action object. Logs the subprocess output with
    `log_action_output_to_file`.

    :param external_action:
    """
    external_action.completed_proc = run_subprocess(external_action.cmd)
    log_action_output_to_file(external_action)


def run_subprocess(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    """
    Passes `cmd` to `subprocess.run()` with preset parameters:
        capture_output=True:                    Output will be logged and analysed later.
        timeout=DEFAULT_CMD_TIMEOUT_SECONDS:
        check=False:                            Error will be handled during verification.
        text=True:                              Output should be str for analysis purposes.

    :param cmd:
    """
    log_msg = f"Running '{Path(cmd[0]).name}' subprocess. Command: {cmd}"
    logger.info(log_msg)

    return subprocess.run(
        cmd,
        capture_output=True,
        timeout=DEFAULT_CMD_TIMEOUT_SECONDS,
        check=False,
        text=True,
    )
