"""Utilities that start/stop a Presto cluster."""

import logging

from tests.package_tests.clp_presto.utils.classes import (
    PrestoAction,
    PrestoCluster,
    PrestoVerificationResult,
)
from tests.package_tests.utils.modes import CLP_PRESTO_COMPONENTS
from tests.utils.docker_utils import (
    list_running_containers_with_prefix,
)
from tests.utils.utils import get_binary_path

logger = logging.getLogger(__name__)


def start_presto_cluster(
    presto_cluster: PrestoCluster,
) -> PrestoAction:
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
    return PrestoAction.from_cmd(start_presto_cmd)


def stop_presto_cluster(presto_cluster: PrestoCluster) -> PrestoAction:
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
    return PrestoAction.from_cmd(stop_presto_cmd)


def verify_start_presto_action(
    start_presto_cluster_action: PrestoAction,
) -> PrestoVerificationResult:
    """
    Verifies that `start_presto_cluster_action` exited successfully and that every Presto component
    is now running in a container.

    :param start_presto_cluster_action:
    :return: A successful `PrestoVerificationResult` if both checks pass; otherwise a failed
        result describing which check did not pass.
    """
    logger.info("Verifying the startup of the Presto cluster.")
    returncode_result = start_presto_cluster_action.verify_returncode()
    if not returncode_result:
        return returncode_result

    cluster_running_err = _validate_presto_cluster_running()
    if cluster_running_err is None:
        return start_presto_cluster_action.pass_verification()

    return start_presto_cluster_action.fail_verification(cluster_running_err)


def verify_stop_presto_action(
    stop_presto_cluster_action: PrestoAction,
) -> PrestoVerificationResult:
    """
    Verifies that `stop_presto_cluster_action` exited successfully and that no Presto component is
    still running in a container.

    :param stop_presto_cluster_action:
    :return: A successful `PrestoVerificationResult` if both checks pass; otherwise a failed
        result describing which check did not pass.
    """
    logger.info("Verifying the spindown of the Presto cluster.")
    returncode_result = stop_presto_cluster_action.verify_returncode()
    if not returncode_result:
        return returncode_result

    cluster_not_running_err = _validate_presto_cluster_not_running()
    if cluster_not_running_err is None:
        return stop_presto_cluster_action.pass_verification()

    return stop_presto_cluster_action.fail_verification(cluster_not_running_err)


def _validate_presto_cluster_running() -> str | None:
    """
    Validates that a Presto cluster is running.

    :return: `None` on success; otherwise a string describing the failure.
    """
    presto_components = CLP_PRESTO_COMPONENTS

    for component in presto_components:
        prefix = f"presto-clp-{component}-"
        running_matches = list_running_containers_with_prefix(prefix)
        if len(running_matches) == 0:
            return (
                f"No running container found for component '{component}' (expected name prefix"
                f" '{prefix}')."
            )

    return None


def _validate_presto_cluster_not_running() -> str | None:
    """
    Validates that a Presto cluster is not running.

    :return: `None` on success; otherwise a string describing the failure.
    """
    presto_components = CLP_PRESTO_COMPONENTS

    for component in presto_components:
        prefix = f"presto-clp-{component}-"
        running_matches = list_running_containers_with_prefix(prefix)
        if len(running_matches) != 0:
            return (
                f"Running container found for component '{component}' (there should be no"
                f" components running with prefix expected name prefix '{prefix}')."
            )

    return None
