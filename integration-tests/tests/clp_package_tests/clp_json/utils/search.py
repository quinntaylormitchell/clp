"""Docstring."""

import logging
import re
from enum import auto, Enum

import pytest

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.parsers import (
    get_search_parser,
)
from tests.utils.classes import (
    IntegrationTestDataset,
    IntegrationTestExternalAction,
)
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import get_binary_path

logger = logging.getLogger(__name__)


class ClpJsonSearchType(Enum):
    """An enumeration of the types of search we can perform in `clp-json`."""

    BASIC = auto()
    IGNORE_CASE = auto()
    COUNT_RESULTS = auto()
    COUNT_BY_TIME = auto()
    TIME_RANGE = auto()


def search_clp_json(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
    search_type: ClpJsonSearchType,
    wildcard_query: str,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = f"Searching the '{dataset.dataset_name}' dataset."  # TODO: "Performing <SEARCH_TYPE>"
    logger.info(log_msg)

    search_cmd: list[str] = _build_search_cmd_for_search_type_clp_json(
        clp_package,
        dataset,
        search_type,
        wildcard_query,
    )
    search_action = ClpPackageExternalAction(
        cmd=search_cmd,
        args_parser=get_search_parser(),
    )
    execute_external_action(search_action)
    return search_action


def _build_search_cmd_for_search_type_clp_json(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
    search_type: ClpJsonSearchType,
    wildcard_query: str,
) -> list[str]:
    """Docstring."""
    clp_package_test_path_config = clp_package.path_config
    search_cmd: list[str] = [
        str(clp_package_test_path_config.search_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        dataset.metadata_dict["dataset_name"],
    ]

    match search_type:
        case ClpJsonSearchType.BASIC:
            pass
        case ClpJsonSearchType.IGNORE_CASE:
            search_cmd.append("--ignore-case")
        case ClpJsonSearchType.COUNT_RESULTS:
            search_cmd.append("--count")
        case ClpJsonSearchType.COUNT_BY_TIME:
            search_cmd.append("--count-by-time")
            search_cmd.append(str(10))
        case ClpJsonSearchType.TIME_RANGE:
            search_cmd.append("--begin-time")
            search_cmd.append(str(dataset.metadata_dict["begin_ts_ms"]))
            search_cmd.append("--end-time")
            search_cmd.append(str(dataset.metadata_dict["end_ts_ms"]))
        case _:
            pytest.fail(f"Unsupported search type for clp-json: '{search_type}'")

    search_cmd.append("--raw")
    search_cmd.append(wildcard_query)

    return search_cmd


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
            err_msg = f"Search type '{search_type}' has not been configured for modification."
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
            err_msg = f"The search result '{search_result}' wasn't in the correct format."
            raise ValueError(err_msg)
        case _:
            err_msg = f"Search type '{search_type}' has not been configured for modification."
            raise ValueError(err_msg)
