"""Docstring."""

import logging
import re
from enum import auto, Enum

from clp_package_utils.general import EXTRACT_FILE_CMD
from clp_py_utils.clp_config import (
    ClpConfig,
    Package,
    QueryEngine,
    StorageEngine,
)

from tests.clp_package_tests.utils.actions import (
    run_archive_manager_cmd,
    run_decompress_cmd,
)
from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
    ClpPackageModeConfig,
)
from tests.clp_package_tests.utils.modes import (
    CLP_BASE_COMPONENTS,
)
from tests.utils.actions import run_grep_cmd
from tests.utils.classes import IntegrationTestDataset, IntegrationTestExternalAction
from tests.utils.utils import (
    clear_directory,
    get_binary_path,
    is_dir_tree_content_equal,
)

logger = logging.getLogger(__name__)

# Mode description for this module.
CLP_TEXT_MODE = ClpPackageModeConfig(
    mode_name="clp-text",
    clp_config=ClpConfig(
        package=Package(
            storage_engine=StorageEngine.CLP,
            query_engine=QueryEngine.CLP,
        ),
        api_server=None,
        log_ingestor=None,
    ),
    component_list=(*CLP_BASE_COMPONENTS,),
)


# Search types for this mode.
class ClpTextSearchType(Enum):
    """An enumeration of the types of search we can perform in `clp-text`."""

    BASIC = auto()
    FILE_PATH = auto()
    IGNORE_CASE = auto()
    COUNT_RESULTS = auto()
    COUNT_BY_TIME = auto()
    TIME_RANGE = auto()


def verify_compress_action_clp_text(
    compress_action: ClpPackageExternalAction,
    clp_package: ClpPackage,
    original_dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    if compress_action.completed_proc.returncode != 0:
        return False, "The compress.sh subprocess returned a non-zero exit code."

    # Decompress the contents of `clp-package/var/data/archives`.
    path_config = clp_package.path_config
    clear_directory(path_config.package_decompression_dir)

    decompress_cmd = [
        str(path_config.decompress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        EXTRACT_FILE_CMD,
        "--extraction-dir",
        str(path_config.package_decompression_dir),
    ]
    decompress_action = run_decompress_cmd(decompress_cmd)
    assert decompress_action.completed_proc.returncode == 0, (
        "decompress.sh command returned non-zero exit code."
    )

    # Verify equality between original logs and decompressed logs.
    original_logs_path = original_dataset.path_to_dataset_logs
    decompressed_logs_path = path_config.package_decompression_dir / original_logs_path.relative_to(
        original_logs_path.anchor
    )

    equal = is_dir_tree_content_equal(original_logs_path, decompressed_logs_path)
    clear_directory(path_config.package_decompression_dir)
    if equal:
        return True, ""
    fail_msg = (
        f"Mismatch between original logs at '{original_logs_path}' and decompressed logs at "
        f"'{decompressed_logs_path}'"
    )
    return False, fail_msg


def verify_search_action_clp_text(
    search_action: ClpPackageExternalAction,
    original_dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
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
    search_type: ClpTextSearchType,
    original_dataset: IntegrationTestDataset,
) -> list[str]:
    grep_cmd_options = _get_grep_options_from_search_type(search_type)
    path_for_grep = search_action.parsed_args.file_path or original_dataset.path_to_dataset_logs
    return [
        get_binary_path("grep"),
        *grep_cmd_options,
        search_action.parsed_args.wildcard_query,
        path_for_grep,
    ]


def _get_search_type_for_action(
    search_action: ClpPackageExternalAction,
) -> ClpTextSearchType:
    parsed_args = search_action.parsed_args

    if parsed_args.begin_time is not None or parsed_args.end_time is not None:
        return ClpTextSearchType.TIME_RANGE
    if parsed_args.count_by_time is not None:
        return ClpTextSearchType.COUNT_BY_TIME
    if parsed_args.count:
        return ClpTextSearchType.COUNT_RESULTS
    if parsed_args.ignore_case:
        return ClpTextSearchType.IGNORE_CASE
    if parsed_args.file_path:
        return ClpTextSearchType.FILE_PATH
    return ClpTextSearchType.BASIC

    # TODO: what if the command has an invalid combination of arguments?


def _get_grep_options_from_search_type(search_type: ClpTextSearchType) -> list[str]:
    grep_cmd_options: list[str] = [
        "--recursive",
        "--no-filename",
        "--color=never",
    ]

    match search_type:
        case (
            ClpTextSearchType.BASIC
            | ClpTextSearchType.FILE_PATH
            | ClpTextSearchType.COUNT_RESULTS
            | ClpTextSearchType.COUNT_BY_TIME
            | ClpTextSearchType.TIME_RANGE
        ):
            return grep_cmd_options
        case ClpTextSearchType.IGNORE_CASE:
            grep_cmd_options.append("--ignore-case")
            return grep_cmd_options
        case _:
            err_msg = (
                f"Search type {search_type} has not been configured for grep command construction."
            )
            raise ValueError(err_msg)


def _format_grep_result_for_search_type(grep_result: str, search_type: ClpTextSearchType) -> str:
    match search_type:
        case (
            ClpTextSearchType.BASIC
            | ClpTextSearchType.FILE_PATH
            | ClpTextSearchType.IGNORE_CASE
            | ClpTextSearchType.TIME_RANGE
        ):
            return grep_result
        case ClpTextSearchType.COUNT_RESULTS | ClpTextSearchType.COUNT_BY_TIME:
            return str(len(grep_result.splitlines())) + "\n"
        case _:
            err_msg = f"Search type {search_type} has not been configured for modification."
            raise ValueError(err_msg)


def _format_search_result_for_search_type(
    search_result: str, search_type: ClpTextSearchType
) -> str:
    match search_type:
        case (
            ClpTextSearchType.BASIC
            | ClpTextSearchType.FILE_PATH
            | ClpTextSearchType.IGNORE_CASE
            | ClpTextSearchType.TIME_RANGE
        ):
            return search_result
        case ClpTextSearchType.COUNT_RESULTS | ClpTextSearchType.COUNT_BY_TIME:
            match = re.search(r"count: (\d+)", search_result)
            if match:
                return match.group(1) + "\n"
            err_msg = f"The search result {search_result} wasn't in the correct format."
            raise ValueError(err_msg)
        case _:
            err_msg = f"Search type {search_type} has not been configured for modification."
            raise ValueError(err_msg)


def clear_package_archives_clp_text(clp_package: ClpPackage) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")
    path_config = clp_package.path_config

    # Find.
    archive_manager_cmd = [
        str(path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "find",
        "--begin-ts",
        "0",
    ]
    archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
    archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_text(
        archive_manager_action, clp_package
    )
    assert archive_manager_action_verified, failure_message

    # Delete.
    archive_ids_to_delete = _extract_archive_ids_from_find_output(archive_manager_action)
    logger.info(f"archive_ids_to_delete is {archive_ids_to_delete}")
    if len(archive_ids_to_delete) > 0:
        archive_manager_cmd = [
            str(path_config.archive_manager_path),
            "--config",
            str(clp_package.temp_config_file_path),
            "del",
            "by-ids",
            *archive_ids_to_delete,
        ]
        archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
        archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_text(
            archive_manager_action, clp_package
        )
        assert archive_manager_action_verified, failure_message


def verify_archive_manager_action_clp_text(
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
            _verify_archive_manager_find_action_clp_text(archive_manager_action, clp_package)
        case "del":
            _verify_archive_manager_del_action_clp_text(archive_manager_action, clp_package)
        case _:
            return (
                False,
                "The archive-manager.sh command carried an unrecognized positional argument.",
            )

    return True, ""


def _verify_archive_manager_find_action_clp_text(
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
            "find",
            "--begin-ts",
            str(end_ts),
        ]
        verify_archive_manager_action = run_archive_manager_cmd(verify_archive_manager_cmd)
        assert verify_archive_manager_action.completed_proc.returncode == 0
        current_archive_id_list.extend(
            _extract_archive_ids_from_find_output(verify_archive_manager_action)
        )

    directories_in_package_archives = _get_names_of_directories_in_package_archives(clp_package)
    if current_archive_id_list != directories_in_package_archives:
        fail_msg = (
            f"Mismatch between output archive ID list '{current_archive_id_list}' and directories"
            f" in var/archives '{directories_in_package_archives}'"
        )
        return False, fail_msg

    return True, ""


def _verify_archive_manager_del_action_clp_text(
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
                "find",
            ]
            verify_archive_manager_action = run_archive_manager_cmd(verify_archive_manager_cmd)
            verify_archive_manager_action_verified, failure_message = (
                verify_archive_manager_action_clp_text(verify_archive_manager_action, clp_package)
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
                "find",
                "--begin-ts",
                str(parsed_args.begin_ts),
            ]
            if parsed_args.end_ts is not None:
                verify_archive_manager_cmd.append("--end-ts")
                verify_archive_manager_cmd.append(str(parsed_args.end_ts))

            verify_archive_manager_action = run_archive_manager_cmd(verify_archive_manager_cmd)
            verify_archive_manager_action_verified, failure_message = (
                verify_archive_manager_action_clp_text(verify_archive_manager_action, clp_package)
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


def _get_names_of_directories_in_package_archives(clp_package: ClpPackage) -> list[str]:
    directories_in_package_archives = []
    archives_dir = clp_package.path_config.package_archives_path
    for item in archives_dir.iterdir():
        if item.is_dir():
            directories_in_package_archives.append(item.name)
    return sorted(directories_in_package_archives)
