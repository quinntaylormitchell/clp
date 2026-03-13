"""Docstring."""

import logging
import re
from enum import auto, Enum

from clp_py_utils.clp_config import (
    ClpConfig,
    Package,
    QueryEngine,
    StorageEngine,
)

from tests.clp_package_tests.clp_package_utils.actions import (
    run_archive_manager_cmd,
    run_dataset_manager_cmd,
)
from tests.clp_package_tests.clp_package_utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
    ClpPackageModeConfig,
)
from tests.clp_package_tests.clp_package_utils.modes import (
    CLP_API_SERVER_COMPONENT,
    CLP_BASE_COMPONENTS,
)
from tests.utils.actions import run_grep_cmd
from tests.utils.classes import IntegrationTestDataset, IntegrationTestExternalAction
from tests.utils.utils import get_binary_path

logger = logging.getLogger(__name__)

# Mode description for clp-json.
CLP_JSON_MODE = ClpPackageModeConfig(
    mode_name="clp-json",
    clp_config=ClpConfig(
        package=Package(
            storage_engine=StorageEngine.CLP_S,
            query_engine=QueryEngine.CLP_S,
        ),
    ),
    component_list=(*CLP_BASE_COMPONENTS, CLP_API_SERVER_COMPONENT),
)


# Search types for clp-json.
class ClpJsonSearchType(Enum):
    """An enumeration of the types of search we can perform in `clp-json`."""

    BASIC = auto()
    IGNORE_CASE = auto()
    COUNT_RESULTS = auto()
    COUNT_BY_TIME = auto()
    TIME_RANGE = auto()


def verify_compress_action_clp_json(
    compress_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    if compress_action.completed_proc.returncode != 0:
        return False, "The compress.sh subprocess returned a non-zero exit code."

    # TODO: Waiting for PR 1299 (clp-json decompression) to be merged.
    assert clp_package
    return True, ""


def verify_search_action_clp_json(
    search_action: ClpPackageExternalAction,
    original_dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying search on the '{original_dataset.dataset_name}' dataset.")
    if search_action.completed_proc.returncode != 0:
        return False, "The search.sh subprocess returned a non-zero exit code."

    # Construct and run grep command.
    search_type = _get_search_type_for_action(search_action)
    grep_cmd = _convert_search_action_to_grep_cmd(search_action, search_type, original_dataset)
    grep_action: IntegrationTestExternalAction = run_grep_cmd(grep_cmd)
    assert grep_action.completed_proc.returncode == 0, "grep command returned non-zero exit code."

    # Compare grep result with search result.
    formatted_grep_result = _format_grep_result_for_search_type(
        grep_action.completed_proc.stdout, search_type
    )
    formatted_search_result = _format_search_result_for_search_type(
        search_action.completed_proc.stdout, search_type
    )
    if formatted_grep_result != formatted_search_result:
        fail_msg = (
            f"Mismatch between search result '{formatted_search_result}' and grep result "
            f"'{formatted_grep_result}'"
        )
        return False, fail_msg

    return True, ""


def _convert_search_action_to_grep_cmd(
    search_action: ClpPackageExternalAction,
    search_type: ClpJsonSearchType,
    original_dataset: IntegrationTestDataset,
) -> list[str]:
    grep_cmd_options = _get_grep_options_from_search_type(search_type)
    path_for_grep = original_dataset.path_to_dataset_logs
    return [
        get_binary_path("grep"),
        *grep_cmd_options,
        search_action.parsed_args.wildcard_query,
        path_for_grep,
    ]


def _get_search_type_for_action(
    search_action: ClpPackageExternalAction,
) -> ClpJsonSearchType:
    parsed_args = search_action.parsed_args

    if parsed_args.begin_time is not None or parsed_args.end_time is not None:
        return ClpJsonSearchType.TIME_RANGE
    if parsed_args.count_by_time is not None:
        return ClpJsonSearchType.COUNT_BY_TIME
    if parsed_args.count:
        return ClpJsonSearchType.COUNT_RESULTS
    if parsed_args.ignore_case:
        return ClpJsonSearchType.IGNORE_CASE
    return ClpJsonSearchType.BASIC

    # TODO: what if the command has an invalid combination of arguments?


def _get_grep_options_from_search_type(search_type: ClpJsonSearchType) -> list[str]:
    grep_cmd_options: list[str] = [
        "--recursive",
        "--no-filename",
        "--color=never",
    ]

    match search_type:
        case (
            ClpJsonSearchType.BASIC
            | ClpJsonSearchType.COUNT_RESULTS
            | ClpJsonSearchType.COUNT_BY_TIME
            | ClpJsonSearchType.TIME_RANGE
        ):
            return grep_cmd_options
        case ClpJsonSearchType.IGNORE_CASE:
            grep_cmd_options.append("--ignore-case")
            return grep_cmd_options
        case _:
            err_msg = (
                f"Search type {search_type} has not been configured for grep command construction."
            )
            raise ValueError(err_msg)


def _format_grep_result_for_search_type(grep_result: str, search_type: ClpJsonSearchType) -> str:
    match search_type:
        case ClpJsonSearchType.BASIC | ClpJsonSearchType.IGNORE_CASE | ClpJsonSearchType.TIME_RANGE:
            return grep_result
        case ClpJsonSearchType.COUNT_RESULTS | ClpJsonSearchType.COUNT_BY_TIME:
            return str(len(grep_result.splitlines())) + "\n"
        case _:
            err_msg = f"Search type {search_type} has not been configured for modification."
            raise ValueError(err_msg)


def _format_search_result_for_search_type(
    search_result: str, search_type: ClpJsonSearchType
) -> str:
    match search_type:
        case ClpJsonSearchType.BASIC | ClpJsonSearchType.IGNORE_CASE | ClpJsonSearchType.TIME_RANGE:
            return search_result
        case ClpJsonSearchType.COUNT_RESULTS | ClpJsonSearchType.COUNT_BY_TIME:
            match = re.search(r"count: (\d+)", search_result)
            if match:
                return match.group(1) + "\n"
            err_msg = f"The search result {search_result} wasn't in the correct format."
            raise ValueError(err_msg)
        case _:
            err_msg = f"Search type {search_type} has not been configured for modification."
            raise ValueError(err_msg)


def verify_dataset_manager_action_clp_json(
    dataset_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying dataset-manager action.")
    if dataset_manager_action.completed_proc.returncode != 0:
        return False, "The dataset-manager.sh subprocess returned a non-zero exit code."

    parsed_args = dataset_manager_action.parsed_args
    subcommand = parsed_args.subcommand
    match subcommand:
        case "list":
            _verify_dataset_manager_list_action_clp_json(dataset_manager_action, clp_package)
        case "del":
            _verify_dataset_manager_del_action_clp_json(dataset_manager_action, clp_package)
        case _:
            return (
                False,
                "The dataset-manager.sh command carried an unrecognized positional argument.",
            )

    return True, ""


def _verify_dataset_manager_list_action_clp_json(
    dataset_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    dataset_list = _extract_dataset_names_from_output(dataset_manager_action)
    directories_in_package_archives = _get_names_of_directories_in_package_archives(clp_package)

    if dataset_list != directories_in_package_archives:
        fail_msg = (
            f"Mismatch between dataset list '{dataset_list}' and directories in var/archives"
            f" '{directories_in_package_archives}'"
        )
        return False, fail_msg

    return True, ""


def _verify_dataset_manager_del_action_clp_json(
    dataset_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    verify_dataset_manager_cmd = [
        str(clp_package.path_config.dataset_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "list",
    ]
    verify_dataset_manager_action: ClpPackageExternalAction = run_dataset_manager_cmd(
        verify_dataset_manager_cmd
    )
    verify_dataset_manager_action_verified, failure_message = (
        verify_dataset_manager_action_clp_json(verify_dataset_manager_action, clp_package)
    )
    assert verify_dataset_manager_action_verified, failure_message

    current_datasets = _extract_dataset_names_from_output(verify_dataset_manager_action)

    parsed_args = dataset_manager_action.parsed_args
    datasets_specified_for_deletion = parsed_args.datasets
    del_all_flag = parsed_args.del_all
    if del_all_flag:
        # Verify that there are no datasets left.
        if len(current_datasets) > 0:
            fail_msg = (
                f"dataset-manager del --all failed: There are datasets still present in the"
                f" metadata database: {current_datasets}"
            )
            return False, fail_msg
    else:
        if len(datasets_specified_for_deletion) == 0:
            # No datasets were specified for deletion.
            return True, ""

        # Verify that the datasets specified for deletion are not present.
        if any(item in current_datasets for item in datasets_specified_for_deletion):
            fail_msg = (
                "dataset-manager del failed: Some datasets that were specified for deletion"
                " are still present in the metadata database."
            )
            return False, fail_msg

    return True, ""


def _extract_dataset_names_from_output(
    dataset_manager_action: ClpPackageExternalAction,
) -> list[str]:
    dataset_list: list[str] = []
    output = (
        dataset_manager_action.completed_proc.stdout + dataset_manager_action.completed_proc.stderr
    )
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


def _get_names_of_directories_in_package_archives(clp_package: ClpPackage) -> list[str]:
    directories_in_package_archives = []
    archives_dir = clp_package.path_config.package_archives_path
    for item in archives_dir.iterdir():
        if item.is_dir():
            directories_in_package_archives.append(item.name)
    return sorted(directories_in_package_archives)


def clear_package_archives_clp_json(clp_package: ClpPackage) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")
    path_config = clp_package.path_config
    dataset_manager_cmd = [
        str(path_config.dataset_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "del",
        "--all",
    ]
    dataset_manager_action: ClpPackageExternalAction = run_dataset_manager_cmd(dataset_manager_cmd)
    dataset_manager_action_verified, failure_message = verify_dataset_manager_action_clp_json(
        dataset_manager_action, clp_package
    )
    assert dataset_manager_action_verified, failure_message


def verify_archive_manager_action_clp_json(
    archive_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying archive-manager action.")
    if archive_manager_action.completed_proc.returncode != 0:
        return False, "The archive-manager.sh subprocess returned a non-zero exit code."

    parsed_args = archive_manager_action.parsed_args
    subcommand = parsed_args.subcommand
    match subcommand:
        case "find":
            _verify_archive_manager_find_action_clp_json(archive_manager_action, clp_package)
        case "del":
            _verify_archive_manager_del_action_clp_json(archive_manager_action, clp_package)
        case _:
            return (
                False,
                "The archive-manager.sh command carried an unrecognized positional argument.",
            )

    return True, ""


def _verify_archive_manager_find_action_clp_json(
    archive_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    parsed_args = archive_manager_action.parsed_args
    begin_ts: int = parsed_args.begin_ts
    end_ts: int | None = parsed_args.end_ts
    path_config = clp_package.path_config

    current_archive_id_list: list[str] = []
    # Chunk 1.
    if begin_ts > 0:
        verify_archive_manager_cmd = [
            str(path_config.archive_manager_path),
            "--config",
            str(clp_package.temp_config_file_path),
            "--dataset",
            parsed_args.dataset,
            "find",
            "--begin-ts",
            "0",
            "--end-ts",
            str(begin_ts),
        ]
        verify_archive_manager_action = run_archive_manager_cmd(verify_archive_manager_cmd)
        assert verify_archive_manager_action.completed_proc.returncode == 0
        current_archive_id_list.extend(
            _extract_archive_ids_from_find_output(verify_archive_manager_action)
        )

    # Chunk 2.
    # We already have the info from this from the original command output.
    current_archive_id_list.extend(_extract_archive_ids_from_find_output(archive_manager_action))

    # Chunk 3.
    if end_ts is not None:
        verify_archive_manager_cmd = [
            str(path_config.archive_manager_path),
            "--config",
            str(clp_package.temp_config_file_path),
            "--dataset",
            parsed_args.dataset,
            "find",
            "--begin-ts",
            str(end_ts),
        ]
        verify_archive_manager_action = run_archive_manager_cmd(verify_archive_manager_cmd)
        assert verify_archive_manager_action.completed_proc.returncode == 0
        current_archive_id_list.extend(
            _extract_archive_ids_from_find_output(verify_archive_manager_action)
        )

    directories_in_dataset_archive_dir = _get_names_of_directories_in_dataset_archive_dir(
        clp_package, parsed_args.dataset
    )
    if current_archive_id_list != directories_in_dataset_archive_dir:
        fail_msg = (
            f"Mismatch between output archive ID list '{current_archive_id_list}' and directories"
            f" in var/archives dataset directory '{directories_in_dataset_archive_dir}'"
        )
        return False, fail_msg

    return True, ""


def _verify_archive_manager_del_action_clp_json(
    archive_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    parsed_args = archive_manager_action.parsed_args
    del_subcommand = parsed_args.del_subcommand
    match del_subcommand:
        case "by-ids":
            # Run a "find all" command.
            verify_archive_manager_cmd = [
                str(clp_package.path_config.archive_manager_path),
                "--config",
                str(clp_package.temp_config_file_path),
                "--dataset",
                parsed_args.dataset,
                "find",
            ]
            verify_archive_manager_action = run_archive_manager_cmd(verify_archive_manager_cmd)
            verify_archive_manager_action_verified, failure_message = (
                verify_archive_manager_action_clp_json(verify_archive_manager_action, clp_package)
            )
            assert verify_archive_manager_action_verified, failure_message

            # Get ids from "find all" and compare.
            current_archive_ids_list = _extract_archive_ids_from_find_output(
                verify_archive_manager_action
            )
            if any(item in current_archive_ids_list for item in parsed_args.ids):
                fail_msg = (
                    "archive-manager del by-ids failed: Some archives that were specified for"
                    " deletion are still present in the metadata database."
                )
                return False, fail_msg
        case "by-filter":
            # Run a "find" command with begin_ts and end_ts.
            verify_archive_manager_cmd = [
                str(clp_package.path_config.archive_manager_path),
                "--config",
                str(clp_package.temp_config_file_path),
                "--dataset",
                parsed_args.dataset,
                "find",
                "--begin-ts",
                str(parsed_args.begin_ts),
            ]
            if parsed_args.end_ts is not None:
                verify_archive_manager_cmd.append("--end-ts")
                verify_archive_manager_cmd.append(str(parsed_args.end_ts))

            verify_archive_manager_action = run_archive_manager_cmd(verify_archive_manager_cmd)
            verify_archive_manager_action_verified, failure_message = (
                verify_archive_manager_action_clp_json(verify_archive_manager_action, clp_package)
            )
            assert verify_archive_manager_action_verified, failure_message

            # Get ids from "find" command with begin_ts and end_ts and compare
            current_archive_ids_list = _extract_archive_ids_from_find_output(
                verify_archive_manager_action
            )
            if len(current_archive_ids_list) > 0:
                fail_msg = (
                    "archive-manager del by-filter failed: Some archives that should have been"
                    " deleted were not deleted."
                )
                return False, fail_msg

        case _:
            fail_msg = (
                "archive-manager del failed: del needs a subcommand ('by-ids' or 'by-filter')"
            )
            return False, fail_msg

    return True, ""


def _extract_archive_ids_from_find_output(
    archive_manager_action: ClpPackageExternalAction,
) -> list[str]:
    output_archive_id_list: list[str] = []
    output = (
        archive_manager_action.completed_proc.stdout + archive_manager_action.completed_proc.stderr
    )
    output_lines = output.splitlines()
    num_archive_ids = 0
    for line in output_lines:
        match = re.search(r"Found (\d+) archives within the specified time range", line)
        if match:
            num_archive_ids = int(match.group(1))
            output_lines.remove(line)
            break

    if num_archive_ids == 0:
        return output_archive_id_list

    for line in output_lines:
        match = re.search(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            line,
        )
        if match:
            output_archive_id_list.append(match.group(0))

    return sorted(output_archive_id_list)


def _get_names_of_directories_in_dataset_archive_dir(
    clp_package: ClpPackage, dataset_name: str
) -> list[str]:
    directories_in_dataset_archive_dir = []
    dataset_archive_dir = clp_package.path_config.package_archives_path / dataset_name
    for item in dataset_archive_dir.iterdir():
        if item.is_dir():
            directories_in_dataset_archive_dir.append(item.name)
    return sorted(directories_in_dataset_archive_dir)
