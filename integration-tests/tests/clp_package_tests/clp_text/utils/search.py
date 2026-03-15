"""Docstring."""

import logging
import re
from enum import auto, Enum

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
    ClpPackageTestPathConfig,
)
from tests.clp_package_tests.utils.parsers import (
    get_search_parser,
)
from tests.utils.classes import (
    IntegrationTestDataset,
    IntegrationTestExternalAction,
)
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import (
    get_binary_path,
)

logger = logging.getLogger(__name__)


class ClpTextSearchType(Enum):
    """An enumeration of the types of search we can perform in `clp-text`."""

    BASIC = auto()
    FILE_PATH = auto()
    IGNORE_CASE = auto()
    COUNT_RESULTS = auto()
    COUNT_BY_TIME = auto()
    TIME_RANGE = auto()


def search_clp_text(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
    search_type: ClpTextSearchType,
    wildcard_query: str,
) -> tuple[bool, str]:
    """Docstring."""
    log_msg = f"Searching the '{clp_package.mode_name}' package."
    logger.info(log_msg)

    search_cmd: list[str] = [
        str(clp_package_test_path_config.search_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--raw",
        wildcard_query,
    ]

    search_action = ClpPackageExternalAction(
        cmd=search_cmd,
        args_parser=get_search_parser(),
    )
    execute_external_action(search_action)

    # TODO: remove
    assert search_type

    return verify_search_action_clp_text(search_action, dataset)


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
    grep_action = IntegrationTestExternalAction(cmd=grep_cmd)
    execute_external_action(grep_action)
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
