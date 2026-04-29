"""Utilities that start/stop the CLP package."""

import logging
from pathlib import Path

from tests.package_tests.utils.classes import ClpPackage
from tests.utils.classes import CmdArgs, ExternalAction
from tests.utils.docker_utils import list_running_services_in_compose_project
from tests.utils.logging_utils import format_action_failure_msg

logger = logging.getLogger(__name__)


class StartStopArgs(CmdArgs):
    """Docstring."""

    script_path: Path
    config: Path

    def to_cmd(self) -> list[str]:
        """Docstring."""
        return [
            str(self.script_path),
            "--config",
            str(self.config),
        ]


def start_clp_package(
    clp_package: ClpPackage,
) -> ExternalAction:
    """Docstring."""
    logger.info(f"Starting up the '{clp_package.mode_name}' package.")
    args: StartStopArgs = _construct_start_clp_args(clp_package)
    return ExternalAction(cmd=args.to_cmd(), args=args)


def _construct_start_clp_args(clp_package: ClpPackage) -> StartStopArgs:
    """Docstring."""
    path_config = clp_package.path_config
    return StartStopArgs(
        script_path=path_config.start_clp_path, config=clp_package.temp_config_file_path
    )


def stop_clp_package(
    clp_package: ClpPackage,
) -> ExternalAction:
    """Docstring."""
    logger.info(f"Stopping the '{clp_package.mode_name}' package.")
    args: StartStopArgs = _construct_stop_clp_args(clp_package)
    return ExternalAction(cmd=args.to_cmd(), args=args)


def _construct_stop_clp_args(clp_package: ClpPackage) -> StartStopArgs:
    """Docstring."""
    path_config = clp_package.path_config
    return StartStopArgs(
        script_path=path_config.stop_clp_path, config=clp_package.temp_config_file_path
    )


def verify_start_clp_action(
    start_clp_action: ExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying the startup of the '{clp_package.mode_name}' package.")
    if start_clp_action.completed_proc.returncode != 0:
        return False, format_action_failure_msg(
            "The start-clp.sh subprocess returned a non-zero exit code.",
            start_clp_action,
        )

    package_running_validated, failure_message = _validate_clp_package_running(clp_package)
    if not package_running_validated:
        return False, format_action_failure_msg(failure_message, start_clp_action)

    return True, ""


def verify_stop_clp_action(
    stop_clp_action: ExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying the spindown of the '{clp_package.mode_name}' package.")
    if stop_clp_action.completed_proc.returncode != 0:
        return False, format_action_failure_msg(
            "The stop-clp.sh subprocess returned a non-zero exit code.", stop_clp_action
        )

    package_not_running_validated, failure_message = _validate_clp_package_not_running(clp_package)
    if not package_not_running_validated:
        return False, format_action_failure_msg(failure_message, stop_clp_action)

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
