"""Docstring."""

from tests.utils.classes import IntegrationTestExternalAction
from tests.utils.subprocess_utils import execute_external_action


def run_grep_cmd(cmd: list[str]) -> IntegrationTestExternalAction:
    """Docstring."""
    grep_action = IntegrationTestExternalAction(cmd=cmd)
    execute_external_action(grep_action)
    return grep_action
