"""Module docstring."""

import subprocess

from tests.utils.classes import IntegrationTestExternalAction

DEFAULT_CMD_TIMEOUT_SECONDS = 120.0


def execute_external_action(external_action: IntegrationTestExternalAction) -> None:
    """Docstring."""
    external_action.completed_proc = run_subprocess(external_action.cmd)


def run_subprocess(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    """Docstring."""
    return subprocess.run(
        cmd,
        capture_output=True,
        timeout=DEFAULT_CMD_TIMEOUT_SECONDS,
        check=False,
        text=True,
    )
