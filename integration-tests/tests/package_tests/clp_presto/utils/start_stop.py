"""Utilities that start/stop a Presto cluster."""

import logging

from tests.package_tests.clp_presto.utils.classes import (
    PrestoCluster,
)
from tests.package_tests.utils.modes import CLP_PRESTO_COMPONENTS
from tests.utils.classes import IntegrationTestExternalAction
from tests.utils.docker_utils import (
    list_running_containers_with_prefix,
)
from tests.utils.logging_utils import format_action_failure_msg
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import get_binary_path

logger = logging.getLogger(__name__)


def start_presto_cluster(
    presto_cluster: PrestoCluster,
) -> IntegrationTestExternalAction:
    """
    Starts a Presto cluster for the CLP package.

    :param presto_cluster:
    """
    path_config = presto_cluster.path_config
    start_presto_cmd: list[str] = [
        get_binary_path("docker"),
        "compose",
        "--file",
        str(path_config.docker_compose_file_path),
        "up",
        "--wait",
    ]
    start_presto_action = IntegrationTestExternalAction(cmd=start_presto_cmd)
    execute_external_action(start_presto_action)
    return start_presto_action


def stop_presto_cluster(presto_cluster: PrestoCluster) -> IntegrationTestExternalAction:
    """
    Stops a Presto cluster for the CLP package.

    :param presto_cluster:
    """
    path_config = presto_cluster.path_config
    stop_presto_cmd: list[str] = [
        get_binary_path("docker"),
        "compose",
        "--file",
        str(path_config.docker_compose_file_path),
        "down",
    ]
    stop_presto_action = IntegrationTestExternalAction(cmd=stop_presto_cmd)
    execute_external_action(stop_presto_action)
    return stop_presto_action


def verify_start_presto_action(
    start_presto_cluster_action: IntegrationTestExternalAction,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying the startup of the Presto cluster.")
    if start_presto_cluster_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The Presto startup subprocess returned a non-zero exit code.",
            start_presto_cluster_action,
        )

    verified, failure_message = _validate_presto_cluster_running()
    if not verified:
        return format_action_failure_msg(failure_message, start_presto_cluster_action)

    return True, ""


def verify_stop_presto_action(
    stop_presto_cluster_action: IntegrationTestExternalAction,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying the spindown of the Presto cluster.")
    if stop_presto_cluster_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The Presto spindown subprocess returned a non-zero exit code.",
            stop_presto_cluster_action,
        )

    verified, failure_message = _validate_presto_cluster_not_running()
    if not verified:
        return format_action_failure_msg(failure_message, stop_presto_cluster_action)

    return True, ""


def _validate_presto_cluster_running() -> tuple[bool, str]:
    """
    Validate that a Presto cluster is running.

    :param package_instance:
    """
    presto_components = CLP_PRESTO_COMPONENTS

    for component in presto_components:
        prefix = f"presto-clp-{component}-"
        running_matches = list_running_containers_with_prefix(prefix)
        if len(running_matches) == 0:
            return False, (
                f"No running container found for component '{component}' (expected name prefix"
                f" '{prefix}')."
            )

    return True, ""


def _validate_presto_cluster_not_running() -> tuple[bool, str]:
    """
    Validate that a Presto cluster is not running.

    :param package_instance:
    """
    presto_components = CLP_PRESTO_COMPONENTS

    for component in presto_components:
        prefix = f"presto-clp-{component}-"
        running_matches = list_running_containers_with_prefix(prefix)
        if len(running_matches) != 0:
            return False, (
                f"Running container found for component '{component}' (there should be no"
                f" components running with prefix expected name prefix '{prefix}')."
            )

    return True, ""
