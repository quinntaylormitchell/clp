"""Utilities that raise pytest assertions on failure."""

import logging

from tests.clp_package_tests.clp_package_utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.clp_package_utils.modes import compare_mode_signatures
from tests.utils.docker_utils import list_running_services_in_compose_project

logger = logging.getLogger(__name__)


def verify_start_clp_action(
    start_clp_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    if start_clp_action.completed_proc.returncode != 0:
        return False, "The start-clp.sh subprocess returned a non-zero exit code."

    package_running_validated, failure_message = _validate_clp_package_running(clp_package)
    if not package_running_validated:
        return False, failure_message

    running_mode_correct_validated, failure_message = _validate_running_mode_correct(clp_package)
    if not running_mode_correct_validated:
        return False, failure_message

    return True, ""


def verify_stop_clp_action(
    stop_clp_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    if stop_clp_action.completed_proc.returncode != 0:
        return False, "The stop-clp.sh subprocess returned a non-zero exit code."

    package_not_running_validated, failure_message = _validate_clp_package_not_running(clp_package)
    if not package_not_running_validated:
        return False, failure_message

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
    fail_msg = f"Component validation failed for the {mode_name} package test."

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
    fail_msg = f"Mode validation failed for the {mode_name} package test."

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
        f"There are components of the {mode_name} package that are still running: "
        f"{running_services}"
    )
    return False, fail_msg
