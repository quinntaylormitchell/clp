"""Tests for the clp-text package."""

import logging
import re
from enum import auto, Enum

import pytest
from clp_py_utils.clp_config import (
    ClpConfig,
    Package,
    QueryEngine,
    StorageEngine,
)

from tests.utils.asserting_utils import (
    validate_package_instance,
    verify_package_compression,
    verify_package_search,
)
from tests.utils.clp_mode_utils import CLP_BASE_COMPONENTS
from tests.utils.config import (
    PackageCompressionJob,
    PackageInstance,
    PackageModeConfig,
    PackageSearchJob,
)
from tests.utils.package_utils import run_package_compression_script, run_package_search_script
from tests.utils.utils import load_json_to_dict, validate_dir_exists

logger = logging.getLogger(__name__)


# Mode description for this module.
CLP_TEXT_MODE = PackageModeConfig(
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


# Pytest markers for this module.
pytestmark = [
    pytest.mark.package,
    pytest.mark.clp_text,
    pytest.mark.parametrize(
        "fixt_package_test_config", [CLP_TEXT_MODE], indirect=True, ids=[CLP_TEXT_MODE.mode_name]
    ),
]


@pytest.mark.startup
def test_clp_text_startup(fixt_package_instance: PackageInstance) -> None:
    """
    Validates that the `clp-text` package starts up successfully.

    :param fixt_package_instance:
    """
    validate_package_instance(fixt_package_instance)


@pytest.mark.compression
def test_clp_text_compression_text_multifile(
    request: pytest.FixtureRequest, fixt_package_instance: PackageInstance
) -> None:
    """
    Validate that the `clp-text` package successfully compresses the `text_multifile` sample
    dataset.

    :param request:
    :param fixt_package_instance:
    """
    validate_package_instance(fixt_package_instance)

    _compress_and_verify_clp_text(request, fixt_package_instance, "text_multifile")


@pytest.mark.search
def test_clp_text_search_text_multifile(
    request: pytest.FixtureRequest, fixt_package_instance: PackageInstance
) -> None:
    """
    Validates that the `clp-text` package successfully searches the `text_multifile` sample dataset.

    :param request:
    :param fixt_package_instance:
    """
    validate_package_instance(fixt_package_instance)

    compression_job = _compress_and_verify_clp_text(
        request, fixt_package_instance, "text_multifile"
    )

    for search_type in ClpTextSearchType:
        _search_and_verify_clp_text(
            request,
            fixt_package_instance,
            compression_job,
            search_type,
            "Saturn",
        )


def _compress_and_verify_clp_text(
    request: pytest.FixtureRequest, package_instance: PackageInstance, sample_dataset_name: str
) -> PackageCompressionJob:
    """
    Compress the dataset called `sample_dataset_name` and verify that it was compressed correctly.

    :param request:
    :param package_instance:
    :param sample_dataset_name:
    """
    # Ensure that a sample dataset called `sample_dataset_name` exists.
    package_test_config = package_instance.package_test_config
    path_to_dataset_dir = (
        package_test_config.path_config.clp_text_test_data_path / sample_dataset_name
    )
    validate_dir_exists(path_to_dataset_dir)

    # Retrieve sample dataset metadata and parse.
    metadata_dict = load_json_to_dict(path_to_dataset_dir / "metadata.json")
    path_to_original_dataset = (
        path_to_dataset_dir / metadata_dict["subdirectory_containing_log_files"]
    )
    begin_ts_ms = metadata_dict["begin_ts_ms"]
    end_ts_ms = metadata_dict["end_ts_ms"]

    # Set up compression job.
    compression_job = PackageCompressionJob(
        metadata_dict=metadata_dict,
        sample_dataset_name=sample_dataset_name,
        options=None,
        path_to_original_dataset=path_to_original_dataset,
        begin_ts_ms=begin_ts_ms,
        end_ts_ms=end_ts_ms,
    )

    # Run compression job.
    run_package_compression_script(request, compression_job, package_test_config)

    # Check the correctness of compression.
    verify_package_compression(request, compression_job, package_test_config)

    return compression_job


def _search_and_verify_clp_text(
    request: pytest.FixtureRequest,
    package_instance: PackageInstance,
    compression_job: PackageCompressionJob,
    search_type: ClpTextSearchType,
    query: str,
) -> None:
    # Set up search job.
    package_test_config = package_instance.package_test_config
    options = [
        *_get_options_from_search_type(search_type, compression_job),
    ]
    search_name = _search_type_to_search_name(search_type)
    if search_type == ClpTextSearchType.FILE_PATH:
        subpath_to_search = (
            compression_job.path_to_original_dataset
            / compression_job.metadata_dict["subfile_for_file_path_search"]
        )
    else:
        subpath_to_search = None
    search_job = PackageSearchJob(
        search_name=search_name,
        compression_job=compression_job,
        options=options,
        query=query,
        subpath_to_search=subpath_to_search,
    )

    # Run search job.
    search_result = run_package_search_script(
        request, compression_job, search_job, package_test_config
    )

    # Check the correctness of search.
    prepared_search_result = _modify_search_result_for_search_type(search_type, search_result)
    grep_cmd_options = _get_grep_options_from_search_type(search_type)
    grep_cmd_pipe = _get_grep_pipe_from_search_type(search_type)
    verify_package_search(
        request, search_job, prepared_search_result, grep_cmd_options, grep_cmd_pipe
    )


def _get_options_from_search_type(
    search_type: ClpTextSearchType, compression_job: PackageCompressionJob
) -> list[str]:
    match search_type:
        case ClpTextSearchType.BASIC:
            return [
                "--raw",
            ]
        case ClpTextSearchType.FILE_PATH:
            return [
                "--file-path",
                (
                    compression_job.path_to_original_dataset
                    / compression_job.metadata_dict["subfile_for_file_path_search"]
                ),
                "--raw",
            ]
        case ClpTextSearchType.IGNORE_CASE:
            return [
                "--ignore-case",
                "--raw",
            ]
        case ClpTextSearchType.COUNT_RESULTS:
            return [
                "--count",
                "--raw",
            ]
        case ClpTextSearchType.COUNT_BY_TIME:
            return [
                "--count-by-time",
                "10",
                "--raw",
            ]
        case ClpTextSearchType.TIME_RANGE:
            return [
                "--begin-time",
                str(compression_job.begin_ts_ms),
                "--end-time",
                str(compression_job.end_ts_ms),
                "--raw",
            ]
        case _:
            err_msg = (
                f"Search type {search_type} has not been configured for options list construction."
            )
            raise ValueError(err_msg)


def _search_type_to_search_name(search_type: ClpTextSearchType) -> str:
    match search_type:
        case ClpTextSearchType.BASIC:
            return "basic"
        case ClpTextSearchType.FILE_PATH:
            return "file path"
        case ClpTextSearchType.IGNORE_CASE:
            return "ignore case"
        case ClpTextSearchType.COUNT_RESULTS:
            return "count results"
        case ClpTextSearchType.COUNT_BY_TIME:
            return "count by time"
        case ClpTextSearchType.TIME_RANGE:
            return "time range"
        case _:
            err_msg = (
                f"Search type {search_type} has not been configured for search name construction."
            )
            raise ValueError(err_msg)


def _modify_search_result_for_search_type(
    search_type: ClpTextSearchType, search_result: str
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


def _get_grep_options_from_search_type(search_type: ClpTextSearchType) -> list[str]:
    grep_cmd_options: list[str] = [
        "--recursive",
        "--no-filename",
        "--color=never",
    ]

    match search_type:
        case ClpTextSearchType.BASIC:
            return grep_cmd_options
        case ClpTextSearchType.FILE_PATH:
            return grep_cmd_options
        case ClpTextSearchType.IGNORE_CASE:
            grep_cmd_options.append("--ignore-case")
            return grep_cmd_options
        case ClpTextSearchType.COUNT_RESULTS:
            grep_cmd_options.append("--count")
            return grep_cmd_options
        case ClpTextSearchType.COUNT_BY_TIME:
            grep_cmd_options.append("--count")
            return grep_cmd_options
        case ClpTextSearchType.TIME_RANGE:
            return grep_cmd_options
        case _:
            err_msg = (
                f"Search type {search_type} has not been configured for grep command construction."
            )
            raise ValueError(err_msg)


def _get_grep_pipe_from_search_type(search_type: ClpTextSearchType) -> str | None:
    match search_type:
        case (
            ClpTextSearchType.BASIC
            | ClpTextSearchType.FILE_PATH
            | ClpTextSearchType.IGNORE_CASE
            | ClpTextSearchType.TIME_RANGE
        ):
            return None
        case ClpTextSearchType.COUNT_RESULTS | ClpTextSearchType.COUNT_BY_TIME:
            return "awk '{s+=$1} END {print s}'"
        case _:
            err_msg = (
                f"Search type {search_type} has not been configured for grep pipe construction."
            )
            raise ValueError(err_msg)
