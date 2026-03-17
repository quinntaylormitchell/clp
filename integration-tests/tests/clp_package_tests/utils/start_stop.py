"""Utilities that start/stop the CLP package."""

import logging

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.modes import compare_mode_signatures
from tests.clp_package_tests.utils.parsers import (
    get_start_clp_parser,
    get_stop_clp_parser,
)
from tests.utils.docker_utils import list_running_services_in_compose_project
from tests.utils.logging_utils import format_action_failure_msg
from tests.utils.subprocess_utils import execute_external_action

logger = logging.getLogger(__name__)


def start_clp_package(clp_package: ClpPackage) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info(f"Starting up the '{clp_package.mode_name}' package.")

    path_config = clp_package.path_config
    start_clp_cmd: list[str] = [
        str(path_config.start_clp_path),
        "--config",
        str(clp_package.temp_config_file_path),
    ]
    start_clp_action = ClpPackageExternalAction(
        cmd=start_clp_cmd,
        args_parser=get_start_clp_parser(),
    )
    execute_external_action(start_clp_action)

    return start_clp_action


def stop_clp_package(clp_package: ClpPackage) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info(f"Stopping the '{clp_package.mode_name}' package.")

    path_config = clp_package.path_config
    stop_clp_cmd: list[str] = [
        str(path_config.stop_clp_path),
        "--config",
        str(clp_package.temp_config_file_path),
    ]
    stop_clp_action = ClpPackageExternalAction(
        cmd=stop_clp_cmd,
        args_parser=get_stop_clp_parser(),
    )
    execute_external_action(stop_clp_action)

    return stop_clp_action


def verify_start_clp_action(
    start_clp_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying the startup of the '{clp_package.mode_name}' package.")
    if start_clp_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The start-clp.sh subprocess returned a non-zero exit code.",
            start_clp_action,
        )

    package_running_validated, failure_message = _validate_clp_package_running(clp_package)
    if not package_running_validated:
        return format_action_failure_msg(failure_message, start_clp_action)

    running_mode_correct_validated, failure_message = _validate_running_mode_correct(clp_package)
    if not running_mode_correct_validated:
        return format_action_failure_msg(failure_message, start_clp_action)

    return True, ""


def verify_stop_clp_action(
    stop_clp_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying the spindown of the '{clp_package.mode_name}' package.")
    if stop_clp_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The stop-clp.sh subprocess returned a non-zero exit code.", stop_clp_action
        )

    package_not_running_validated, failure_message = _validate_clp_package_not_running(clp_package)
    if not package_not_running_validated:
        return format_action_failure_msg(failure_message, stop_clp_action)

    return True, ""


def _validate_clp_package_running(clp_package: ClpPackage) -> tuple[bool, str]:
    """Docstring."""
    # Get list of services currently running in the Compose project.
    instance_id = clp_package.get_clp_instance_id()
    project_name = f"clp-package-{instance_id}"
    running_services = set(list_running_services_in_compose_project(project_name))

    # Compare with list of required components.
    required_components = set(clp_package.component_list)
    if required_components == running_services:
        return True, ""

    # Construct failure message.
    mode_name = clp_package.mode_name
    fail_msg = (
        f"'{mode_name}' package start up verification failure: components could not be validated."
    )

    missing_components = required_components - running_services
    if missing_components:
        fail_msg += f" Missing components: {missing_components}."

    unexpected_components = running_services - required_components
    if unexpected_components:
        fail_msg += f" Unexpected services: {unexpected_components}."

    return False, fail_msg


def _validate_running_mode_correct(clp_package: ClpPackage) -> tuple[bool, str]:
    """Docstring."""
    running_config = clp_package.get_running_config_from_shared_config_file()
    intended_config = clp_package.clp_config

    if compare_mode_signatures(intended_config, running_config):
        return True, ""

    # Construct failure message.
    mode_name = clp_package.mode_name
    fail_msg = (
        f"'{mode_name}' package start up verification failure: operating mode could not be"
        " validated."
    )

    return False, fail_msg


def _validate_clp_package_not_running(clp_package: ClpPackage) -> tuple[bool, str]:
    # Get list of services currently running in the Compose project.
    instance_id = clp_package.get_clp_instance_id()
    project_name = f"clp-package-{instance_id}"
    running_services = set(list_running_services_in_compose_project(project_name))

    # Make sure the set is empty.
    if not running_services:
        return True, ""

    # Construct failure message.
    mode_name = clp_package.mode_name
    fail_msg = (
        f"'{mode_name}' package stop verification failure: there are components of the package that"
        f" are still running: '{running_services}'"
    )
    return False, fail_msg
