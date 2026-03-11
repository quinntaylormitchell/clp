"""Module docstring."""

import logging
import subprocess
from pathlib import Path

from tests.utils.classes import IntegrationTestExternalAction
from tests.utils.logging_utils import log_subprocess_output_to_file

DEFAULT_CMD_TIMEOUT_SECONDS = 120.0
logger = logging.getLogger(__name__)


def execute_external_action(external_action: IntegrationTestExternalAction) -> None:
    """Docstring."""
    external_action.completed_proc = run_subprocess(external_action.cmd)
    log_subprocess_output_to_file(external_action)


def run_subprocess(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    """Docstring."""
    logger.info(f"Running '{Path(cmd[0]).name}' subprocess. Command: {cmd}")
    return subprocess.run(
        cmd,
        capture_output=True,
        timeout=DEFAULT_CMD_TIMEOUT_SECONDS,
        check=False,
        text=True,
    )
