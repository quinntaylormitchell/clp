"""Docstring."""

import logging
import re

import pytest

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.parsers import (
    get_dataset_manager_parser,
)
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.logging_utils import format_action_failure_msg
from tests.utils.subprocess_utils import execute_external_action

logger = logging.getLogger(__name__)


def dataset_manager_list_clp_json(
    clp_package: ClpPackage,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info("Performing 'list' operation with dataset-manager.")

    cmd = _get_base_dataset_manager_cmd(clp_package)
    cmd.append("list")

    return _run_dataset_manager_action(cmd)


def dataset_manager_del_clp_json(
    clp_package: ClpPackage,
    datasets_to_del: list[IntegrationTestDataset] | None = None,
    del_all: bool = False,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info("Performing 'del' operation with dataset-manager.")

    cmd = _get_base_dataset_manager_cmd(clp_package)
    cmd.append("del")

    if del_all:
        cmd.append("--all")
    elif datasets_to_del is not None:
        for item in datasets_to_del:
            cmd.append(item.dataset_name)
    else:
        pytest.fail("You must specify either `datasets_to_del` or `del_all` arguments.")

    return _run_dataset_manager_action(cmd)


def verify_dataset_manager_list_action_clp_json(
    dataset_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying dataset-manager 'list'.")
    if dataset_manager_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The 'dataset-manager.sh list' subprocess returned a non-zero exit code.",
            dataset_manager_action,
        )

    dataset_list = _extract_dataset_names_from_output(dataset_manager_action)
    directories_in_package_archives = _get_names_of_directories_in_package_archives(clp_package)

    if dataset_list != directories_in_package_archives:
        return format_action_failure_msg(
            "Dataset-manager 'list' verification failure: mismatch between output dataset list"
            f" '{dataset_list}' and directories in var/archives"
            f" '{directories_in_package_archives}'",
            dataset_manager_action,
        )

    return True, ""


def verify_dataset_manager_del_action_clp_json(
    dataset_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying dataset-manager 'del'.")
    if dataset_manager_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The 'dataset-manager.sh del' subprocess returned a non-zero exit code.",
            dataset_manager_action,
        )

    # Get list of all datasets currently in archives.
    list_action = dataset_manager_list_clp_json(clp_package)
    verified, failure_message = verify_dataset_manager_list_action_clp_json(
        list_action, clp_package
    )
    if not verified:
        pytest.fail(
            "During dataset-manager 'del' verification, supporting call to dataset-manager 'list'"
            f" could not be verified: '{failure_message}' Subprocess log:"
            f" '{list_action.log_file_path}'"
        )

    current_datasets = _extract_dataset_names_from_output(list_action)
    parsed_args = dataset_manager_action.parsed_args
    datasets_specified_for_deletion = parsed_args.datasets
    del_all_flag = parsed_args.del_all
    if del_all_flag:
        if len(current_datasets) > 0:
            return format_action_failure_msg(
                f"Dataset-manager 'del --all' verification failure: There are datasets still"
                f" present in the database: '{current_datasets}'.",
                dataset_manager_action,
            )
    elif any(item in current_datasets for item in datasets_specified_for_deletion):
        return format_action_failure_msg(
            "Dataset-manager 'del' verification failure: Some datasets that were specified for"
            " deletion are still present in the database.",
            dataset_manager_action,
        )

    return True, ""


def _extract_dataset_names_from_output(
    dataset_manager_action: ClpPackageExternalAction,
) -> list[str]:
    dataset_list: list[str] = []
    output = _get_action_output(dataset_manager_action)
    output_lines = output.splitlines()
    num_datasets = 0
    for line in output_lines:
        match = re.search(r"Found (\d+) datasets", line)
        if match:
            num_datasets = int(match.group(1))
            output_lines.remove(line)
            break

    if num_datasets == 0:
        return dataset_list

    for line in output_lines:
        match = re.search(r"INFO \[dataset_manager\] (.+)", line)
        if match:
            dataset_list.append(match.group(1))

    return sorted(dataset_list)


def clear_package_archives_clp_json(
    clp_package: ClpPackage,
) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")
    del_action = dataset_manager_del_clp_json(
        clp_package=clp_package,
        del_all=True,
    )
    verified, failure_message = verify_dataset_manager_del_action_clp_json(del_action, clp_package)
    if not verified:
        pytest.fail(
            f"When clearing package archives, the call to dataset-manager 'del' could not be"
            f" verified: '{failure_message}' Subprocess log: '{del_action.log_file_path}'"
        )


def _get_base_dataset_manager_cmd(
    clp_package: ClpPackage,
) -> list[str]:
    """Build the common prefix shared by all dataset-manager commands."""
    return [
        str(clp_package.path_config.dataset_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
    ]


def _run_dataset_manager_action(cmd: list[str]) -> ClpPackageExternalAction:
    """Construct and immediately execute a ClpPackageExternalAction."""
    action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_dataset_manager_parser(),
    )
    execute_external_action(action)
    return action


def _get_action_output(action: ClpPackageExternalAction) -> str:
    """Return the combined stdout + stderr from a completed action."""
    return action.completed_proc.stdout + action.completed_proc.stderr


def _get_names_of_directories_in_package_archives(clp_package: ClpPackage) -> list[str]:
    directories_in_package_archives = []
    archives_dir = clp_package.path_config.package_archives_path
    for item in archives_dir.iterdir():
        if item.is_dir():
            directories_in_package_archives.append(item.name)
    return sorted(directories_in_package_archives)
