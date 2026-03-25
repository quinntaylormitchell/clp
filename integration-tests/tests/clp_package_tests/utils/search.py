"""Functions and classes to facilitate CLP package search."""

import logging
import re
from enum import auto, Enum
from typing import Any

import pytest
from clp_py_utils.clp_config import StorageEngine

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.utils.classes import (
    IntegrationTestDataset,
    IntegrationTestExternalAction,
)
from tests.utils.logging_utils import format_action_failure_msg
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import (
    get_binary_path,
)

logger = logging.getLogger(__name__)


DEFAULT_COUNT_BY_TIME_INTERVAL = 10


class ClpPackageSearchType(Enum):
    """An enumeration of the types of search we can perform with the CLP package."""

    BASIC = auto()
    FILE_PATH = auto()
    IGNORE_CASE = auto()
    COUNT_RESULTS = auto()
    COUNT_BY_TIME = auto()
    TIME_RANGE = auto()


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


def search_clp_text(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
    search_type: ClpPackageSearchType,
    wildcard_query: str,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info(f"Performing '{search_type.name}' search.")

    arg_dict: dict[str, Any] = construct_search_arg_dict(
        clp_package, dataset, search_type, wildcard_query
    )
    search_action = ClpPackageExternalAction(cmd=construct_search_cmd(arg_dict), arg_dict=arg_dict)
    execute_external_action(search_action)

    return search_action


def construct_search_arg_dict(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
    search_type: ClpPackageSearchType,
    wildcard_query: str,
) -> dict[str, Any]:
    """Docstring."""
    path_config = clp_package.path_config

    arg_dict: dict[str, Any] = {
        "script_path": path_config.search_path,
        "config": clp_package.temp_config_file_path,
    }

    if clp_package.clp_config.package.storage_engine == StorageEngine.CLP_S:
        arg_dict["dataset"] = dataset.metadata_dict["dataset"]

    match search_type:
        case ClpPackageSearchType.BASIC:
            pass
        case ClpPackageSearchType.FILE_PATH:
            arg_dict["file_path"] = (
                dataset.path_to_dataset_logs
                / dataset.metadata_dict["file_structure"]["file_names"][0]
            )
        case ClpPackageSearchType.IGNORE_CASE:
            arg_dict["ignore_case"] = True
        case ClpPackageSearchType.COUNT_RESULTS:
            arg_dict["count"] = True
        case ClpPackageSearchType.COUNT_BY_TIME:
            arg_dict["count_by_time"] = DEFAULT_COUNT_BY_TIME_INTERVAL
        case ClpPackageSearchType.TIME_RANGE:
            arg_dict["begin_time"] = dataset.metadata_dict["begin_ts"]
            arg_dict["end_time"] = dataset.metadata_dict["end_ts"]
        case _:
            pytest.fail(f"Unsupported search type for CLP package: '{search_type}'")

    arg_dict["raw"] = True
    arg_dict["wildcard_query"] = wildcard_query

    return arg_dict


def construct_search_cmd(arg_dict: dict[str, Any]) -> list[str]:
    """Docstring."""
    search_cmd: list[str] = [
        str(arg_dict["script_path"]),
        "--config",
        str(arg_dict["config"]),
    ]
    if "dataset" in arg_dict:
        search_cmd.append("--dataset")
        search_cmd.append(arg_dict["dataset"])
    if "file_path" in arg_dict:
        search_cmd.append("--file-path")
        search_cmd.append(str(arg_dict["file_path"]))
    if arg_dict.get("ignore_case"):
        search_cmd.append("--ignore-case")
    if arg_dict.get("count"):
        search_cmd.append("--count")
    if "count_by_time" in arg_dict:
        search_cmd.append("--count-by-time")
        search_cmd.append(str(arg_dict["count_by_time"]))
    if "begin_time" in arg_dict:
        search_cmd.append("--begin-time")
        search_cmd.append(str(arg_dict["begin_time"]))
    if "end_time" in arg_dict:
        search_cmd.append("--end-time")
        search_cmd.append(str(arg_dict["end_time"]))
    if arg_dict.get("raw"):
        search_cmd.append("--raw")
    search_cmd.append(arg_dict["wildcard_query"])

    return search_cmd


def verify_search_action(
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
        cmd=_construct_grep_verification_cmd(search_action, search_type, original_dataset)
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


def _construct_grep_verification_cmd(
    search_action: ClpPackageExternalAction,
    search_type: ClpPackageSearchType,
    original_dataset: IntegrationTestDataset,
) -> list[str]:
    grep_cmd_options = _get_grep_options_from_search_type(search_type)
    arg_dict = search_action.arg_dict
    if "file_path" in arg_dict:
        path_for_grep = arg_dict["file_path"]
    else:
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
            | ClpPackageSearchType.FILE_PATH
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
            | ClpPackageSearchType.FILE_PATH
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
            | ClpPackageSearchType.FILE_PATH
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
