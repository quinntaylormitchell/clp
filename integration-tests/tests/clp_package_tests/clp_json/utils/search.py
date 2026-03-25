"""Docstring."""

import logging
import re
from typing import Any

import pytest

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.parsers import (
    ClpPackageSearchType,
    construct_search_arg_dict,
    construct_search_cmd,
)
from tests.utils.classes import (
    IntegrationTestDataset,
    IntegrationTestExternalAction,
)
from tests.utils.logging_utils import format_action_failure_msg
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import get_binary_path

logger = logging.getLogger(__name__)


def search_clp_json(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
    search_type: ClpPackageSearchType,
    wildcard_query: str,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info(f"Performing '{search_type.name}' search on the '{dataset.dataset_name}' dataset.")

    arg_dict: dict[str, Any] = construct_search_arg_dict(
        clp_package, dataset, search_type, wildcard_query
    )
    search_action = ClpPackageExternalAction(cmd=construct_search_cmd(arg_dict), arg_dict=arg_dict)
    execute_external_action(search_action)

    return search_action


def verify_search_action_clp_json(
    search_action: ClpPackageExternalAction,
    search_type: ClpPackageSearchType,
    original_dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying search.")
    if search_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The 'search.sh' subprocess returned a non-zero exit code.", search_action
        )

    # Construct and run grep command.
    grep_action = IntegrationTestExternalAction(
        cmd=_construct_grep_verirfication_cmd(search_action, search_type, original_dataset)
    )
    execute_external_action(grep_action)

    if grep_action.completed_proc.returncode != 0:
        pytest.fail(
            "During search action verification, internal grep command returned a non-zero exit"
            f" code. Subprocess log: {grep_action.log_file_path}"
        )

    # Compare grep result with search result.
    formatted_grep_result = _format_grep_result_for_search_type(
        grep_action.completed_proc.stdout, search_type
    )
    formatted_search_result = _format_search_result_for_search_type(
        search_action.completed_proc.stdout, search_type
    )
    if formatted_grep_result != formatted_search_result:
        return format_action_failure_msg(
            f"Search verification failure: mismatch between formatted search result"
            f" '{formatted_search_result}' and formatted grep result '{formatted_grep_result}'.",
            search_action,
            grep_action,
        )

    return True, ""


def _construct_grep_verirfication_cmd(
    search_action: ClpPackageExternalAction,
    search_type: ClpPackageSearchType,
    original_dataset: IntegrationTestDataset,
) -> list[str]:
    grep_cmd_options = _get_grep_options_from_search_type(search_type)
    path_for_grep = original_dataset.path_to_dataset_logs
    return [
        get_binary_path("grep"),
        *grep_cmd_options,
        search_action.arg_dict["wildcard_query"],
        path_for_grep,
    ]


def _get_grep_options_from_search_type(search_type: ClpPackageSearchType) -> list[str]:
    grep_cmd_options: list[str] = [
        "--recursive",
        "--no-filename",
        "--color=never",
    ]

    match search_type:
        case (
            ClpPackageSearchType.BASIC
            | ClpPackageSearchType.COUNT_RESULTS
            | ClpPackageSearchType.COUNT_BY_TIME
            | ClpPackageSearchType.TIME_RANGE
        ):
            return grep_cmd_options
        case ClpPackageSearchType.IGNORE_CASE:
            grep_cmd_options.append("--ignore-case")
            return grep_cmd_options
        case _:
            pytest.fail(
                f"Search type '{search_type.name}' not configured for grep command construction."
            )


def _format_grep_result_for_search_type(grep_result: str, search_type: ClpPackageSearchType) -> str:
    match search_type:
        case (
            ClpPackageSearchType.BASIC
            | ClpPackageSearchType.IGNORE_CASE
            | ClpPackageSearchType.TIME_RANGE
        ):
            return grep_result
        case ClpPackageSearchType.COUNT_RESULTS | ClpPackageSearchType.COUNT_BY_TIME:
            return str(len(grep_result.splitlines())) + "\n"
        case _:
            pytest.fail(
                f"Search type '{search_type.name}' not configured for grep result formatting."
            )


def _format_search_result_for_search_type(
    search_result: str, search_type: ClpPackageSearchType
) -> str:
    match search_type:
        case (
            ClpPackageSearchType.BASIC
            | ClpPackageSearchType.IGNORE_CASE
            | ClpPackageSearchType.TIME_RANGE
        ):
            return search_result
        case ClpPackageSearchType.COUNT_RESULTS | ClpPackageSearchType.COUNT_BY_TIME:
            match = re.search(r"count: (\d+)", search_result)
            if match:
                return match.group(1) + "\n"
            pytest.fail(f"The search result '{search_result}' wasn't in the correct format.")
        case _:
            pytest.fail(
                f"Search type '{search_type.name}' not configured for search result formatting."
            )
