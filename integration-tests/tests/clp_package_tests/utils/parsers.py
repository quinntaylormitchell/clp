"""Module docstring."""

from enum import auto, Enum
from typing import Any

import pytest
from clp_py_utils.clp_config import StorageEngine
from strenum import StrEnum

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
)
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.utils import get_rand_subdirectory_name

DEFAULT_COUNT_BY_TIME_INTERVAL = 10


class ClpPackageSearchType(Enum):
    """An enumeration of the types of search we can perform with the CLP package."""

    BASIC = auto()
    FILE_PATH = auto()
    IGNORE_CASE = auto()
    COUNT_RESULTS = auto()
    COUNT_BY_TIME = auto()
    TIME_RANGE = auto()


class ClpPackageArchiveManagerType(Enum):
    """Docstring."""

    FIND = auto()
    DEL_BY_IDS = auto()
    DEL_BY_FILTER = auto()


class ClpPackageArchiveManagerSubcommand(StrEnum):
    """Docstring."""

    FIND_COMMAND = "find"
    DEL_COMMAND = "del"


class ClpPackageArchiveManagerDelSubcommand(StrEnum):
    """Docstring."""

    BY_IDS_COMMAND = "by-ids"
    BY_FILTER_COMMAND = "by-filter"


class ClpPackageDatasetManagerType(Enum):
    """Docstring."""

    LIST = auto()
    DEL = auto()


class ClpPackageDatasetManagerSubcommand(StrEnum):
    """Docstring."""

    LIST_COMMAND = "list"
    DEL_COMMAND = "del"


def construct_archive_manager_arg_dict(  # noqa: PLR0913
    clp_package: ClpPackage,
    archive_manager_type: ClpPackageArchiveManagerType,
    dataset: IntegrationTestDataset | None,
    begin_ts: int | None = None,
    end_ts: int | None = None,
    ids_to_del: list[str] | None = None,
) -> dict[str, Any]:
    """Docstring."""
    path_config = clp_package.path_config

    arg_dict: dict[str, Any] = {
        "script_path": path_config.archive_manager_path,
        "config": clp_package.temp_config_file_path,
    }

    if dataset is not None:
        arg_dict["dataset"] = dataset.dataset_name

    match archive_manager_type:
        case ClpPackageArchiveManagerType.FIND:
            arg_dict["subcommand"] = ClpPackageArchiveManagerSubcommand.FIND_COMMAND
            if begin_ts is not None:
                arg_dict["begin_ts"] = begin_ts
            if end_ts is not None:
                arg_dict["end_ts"] = end_ts
        case ClpPackageArchiveManagerType.DEL_BY_IDS:
            arg_dict["subcommand"] = ClpPackageArchiveManagerSubcommand.DEL_COMMAND
            arg_dict["del_subcommand"] = ClpPackageArchiveManagerDelSubcommand.BY_IDS_COMMAND
            if ids_to_del is not None:
                arg_dict["ids"] = ids_to_del
            else:
                sample_id = get_rand_subdirectory_name(path_config.package_archives_path)
                arg_dict["ids"] = [sample_id]
        case ClpPackageArchiveManagerType.DEL_BY_FILTER:
            arg_dict["subcommand"] = ClpPackageArchiveManagerSubcommand.DEL_COMMAND
            arg_dict["del_subcommand"] = ClpPackageArchiveManagerDelSubcommand.BY_FILTER_COMMAND
            if begin_ts is not None:
                arg_dict["begin_ts"] = begin_ts
            if end_ts is None:
                pytest.fail(
                    "`end_ts` parameter cannot be 'None' when using archive-manager"
                    " 'del by-filter'."
                )
            arg_dict["end_ts"] = end_ts
        case _:
            pytest.fail(
                "Unsupported archive_management task type for CLP package:"
                f" '{archive_manager_type}'"
            )

    return arg_dict


def construct_archive_manager_cmd(arg_dict: dict[str, Any]) -> list[str]:
    """Docstring."""
    cmd: list[str] = [
        str(arg_dict["script_path"]),
        "--config",
        str(arg_dict["config"]),
    ]

    if "dataset" in arg_dict:
        cmd.append("--dataset")
        cmd.append(arg_dict["dataset"])

    cmd.append(arg_dict["subcommand"])

    if "del_subcommand" in arg_dict:
        cmd.append(arg_dict["del_subcommand"])
    if "ids" in arg_dict:
        cmd.extend(arg_dict["ids"])
    if "begin_ts" in arg_dict:
        cmd.append("--begin-ts")
        cmd.append(str(arg_dict["begin_ts"]))
    if "end_ts" in arg_dict:
        cmd.append("--end-ts")
        cmd.append(str(arg_dict["end_ts"]))

    return cmd


def construct_compress_arg_dict(
    clp_package: ClpPackage, dataset: IntegrationTestDataset
) -> dict[str, Any]:
    """Docstring."""
    path_config = clp_package.path_config

    arg_dict: dict[str, Any] = {
        "script_path": path_config.compress_path,
        "config": clp_package.temp_config_file_path,
    }

    if clp_package.clp_config.package.storage_engine == StorageEngine.CLP_S:
        arg_dict["dataset"] = dataset.metadata_dict["dataset"]
        arg_dict["timestamp_key"] = dataset.metadata_dict["timestamp_key"]
        arg_dict["unstructured"] = dataset.metadata_dict["data"]["unstructured"]

    arg_dict["paths"] = [dataset.path_to_dataset_logs]

    return arg_dict


def construct_compress_cmd(arg_dict: dict[str, Any]) -> list[str]:
    """Docstring."""
    compress_cmd: list[str] = [
        str(arg_dict["script_path"]),
        "--config",
        str(arg_dict["config"]),
    ]
    if "dataset" in arg_dict:
        compress_cmd.append("--dataset")
        compress_cmd.append(arg_dict["dataset"])
    if "timestamp_key" in arg_dict:
        compress_cmd.append("--timestamp-key")
        compress_cmd.append(arg_dict["timestamp_key"])
    if arg_dict.get("unstructured"):
        compress_cmd.append("--unstructured")
    if "paths" in arg_dict:
        compress_cmd.extend(arg_dict["paths"])

    return compress_cmd


def construct_dataset_manager_arg_dict(
    clp_package: ClpPackage,
    dataset_manager_type: ClpPackageDatasetManagerType,
    datasets_to_del: list[IntegrationTestDataset] | None = None,
    del_all: bool = False,
) -> dict[str, Any]:
    """Docstring."""
    path_config = clp_package.path_config

    arg_dict: dict[str, Any] = {
        "script_path": path_config.dataset_manager_path,
        "config": clp_package.temp_config_file_path,
    }

    match dataset_manager_type:
        case ClpPackageDatasetManagerType.LIST:
            arg_dict["subcommand"] = ClpPackageDatasetManagerSubcommand.LIST_COMMAND
        case ClpPackageDatasetManagerType.DEL:
            arg_dict["subcommand"] = ClpPackageDatasetManagerSubcommand.DEL_COMMAND
            arg_dict["del_all"] = del_all
            if datasets_to_del is not None:
                dataset_names: list[str] = []
                for dataset in datasets_to_del:
                    dataset_names.append(dataset.dataset_name)
                arg_dict["datasets"] = dataset_names
            elif not del_all:
                pytest.fail(
                    "You must specify either `datasets_to_del` or `del_all` arguments for"
                    " dataset-manager."
                )
        case _:
            pytest.fail(
                "Unsupported archive_management task type for CLP package:"
                f" '{dataset_manager_type}'"
            )

    return arg_dict


def construct_dataset_manager_cmd(arg_dict: dict[str, Any]) -> list[str]:
    """Docstring."""
    cmd: list[str] = [
        str(arg_dict["script_path"]),
        "--config",
        str(arg_dict["config"]),
    ]

    cmd.append(arg_dict["subcommand"])

    if arg_dict.get("del_all"):
        cmd.append("--all")
    if "datasets" in arg_dict:
        cmd.extend(arg_dict["datasets"])

    return cmd


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


def construct_start_clp_arg_dict(clp_package: ClpPackage) -> dict[str, Any]:
    """Docstring."""
    path_config = clp_package.path_config

    return {
        "script_path": path_config.start_clp_path,
        "config": clp_package.temp_config_file_path,
    }


def construct_start_clp_cmd(arg_dict: dict[str, Any]) -> list[str]:
    """Docstring."""
    return [
        str(arg_dict["script_path"]),
        "--config",
        str(arg_dict["config"]),
    ]


def construct_stop_clp_arg_dict(clp_package: ClpPackage) -> dict[str, Any]:
    """Docstring."""
    path_config = clp_package.path_config

    return {
        "script_path": path_config.stop_clp_path,
        "config": clp_package.temp_config_file_path,
    }


def construct_stop_clp_cmd(arg_dict: dict[str, Any]) -> list[str]:
    """Docstring."""
    return [
        str(arg_dict["script_path"]),
        "--config",
        str(arg_dict["config"]),
    ]
