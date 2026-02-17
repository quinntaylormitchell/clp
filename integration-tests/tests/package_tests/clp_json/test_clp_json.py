"""Tests for the clp-json package."""

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
from tests.utils.clp_mode_utils import CLP_API_SERVER_COMPONENT, CLP_BASE_COMPONENTS
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
CLP_JSON_MODE = PackageModeConfig(
    mode_name="clp-json",
    clp_config=ClpConfig(
        package=Package(
            storage_engine=StorageEngine.CLP_S,
            query_engine=QueryEngine.CLP_S,
        ),
    ),
    component_list=(*CLP_BASE_COMPONENTS, CLP_API_SERVER_COMPONENT),
)


# Search types for this mode.
class ClpJsonSearchType(Enum):
    """An enumeration of the types of search we can perform in `clp-json`."""

    BASIC = auto()
    IGNORE_CASE = auto()
    COUNT_RESULTS = auto()
    COUNT_BY_TIME = auto()
    TIME_RANGE = auto()


# Pytest markers for this module.
pytestmark = [
    pytest.mark.package,
    pytest.mark.clp_json,
    pytest.mark.parametrize(
        "fixt_package_test_config", [CLP_JSON_MODE], indirect=True, ids=[CLP_JSON_MODE.mode_name]
    ),
]


@pytest.mark.startup
def test_clp_json_startup(fixt_package_instance: PackageInstance) -> None:
    """
    Validates that the `clp-json` package starts up successfully.

    :param fixt_package_instance:
    """
    validate_package_instance(fixt_package_instance)


@pytest.mark.compression
def test_clp_json_compression_json_multifile(
    request: pytest.FixtureRequest, fixt_package_instance: PackageInstance
) -> None:
    """
    Validate that the `clp-json` package successfully compresses the `json_multifile` sample
    dataset.

    :param request:
    :param fixt_package_instance:
    """
    validate_package_instance(fixt_package_instance)

    _compress_and_verify_clp_json(request, fixt_package_instance, "json_multifile")


@pytest.mark.search
def test_clp_json_search_json_multifile(
    request: pytest.FixtureRequest, fixt_package_instance: PackageInstance
) -> None:
    """
    Validates that the `clp-json` package successfully searches the `json_multifile` sample dataset.

    :param request:
    :param fixt_package_instance:
    """
    validate_package_instance(fixt_package_instance)

    compression_job = _compress_and_verify_clp_json(
        request, fixt_package_instance, "json_multifile"
    )

    for search_type in ClpJsonSearchType:
        _search_and_verify_clp_json(
            request,
            fixt_package_instance,
            compression_job,
            search_type,
            '"detail":"Roll program complete, heads down attitude achieved for ascent"',
        )


def _compress_and_verify_clp_json(
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
        package_test_config.path_config.clp_json_test_data_path / sample_dataset_name
    )
    validate_dir_exists(path_to_dataset_dir)

    # Retrieve sample dataset metadata and parse.
    metadata_dict = load_json_to_dict(path_to_dataset_dir / "metadata.json")
    path_to_original_dataset = (
        path_to_dataset_dir / metadata_dict["subdirectory_containing_log_files"]
    )
    timestamp_key = metadata_dict["timestamp_key"]
    begin_ts_ms = metadata_dict["begin_ts_ms"]
    end_ts_ms = metadata_dict["end_ts_ms"]

    # Set up compression job.
    compression_job = PackageCompressionJob(
        metadata_dict=metadata_dict,
        sample_dataset_name=sample_dataset_name,
        options=[
            "--timestamp-key",
            timestamp_key,
            "--dataset",
            sample_dataset_name,
        ],
        path_to_original_dataset=path_to_original_dataset,
        begin_ts_ms=begin_ts_ms,
        end_ts_ms=end_ts_ms,
    )

    # Run compression job.
    run_package_compression_script(request, compression_job, package_test_config)

    # Check the correctness of compression.
    verify_package_compression(request, compression_job, package_test_config)

    return compression_job


def _search_and_verify_clp_json(
    request: pytest.FixtureRequest,
    package_instance: PackageInstance,
    compression_job: PackageCompressionJob,
    search_type: ClpJsonSearchType,
    query: str,
) -> None:
    # Set up search job.
    package_test_config = package_instance.package_test_config
    options = [
        "--dataset",
        compression_job.sample_dataset_name,
        *_get_options_from_search_type(search_type, compression_job),
    ]
    search_name = _search_type_to_search_name(search_type)
    search_job = PackageSearchJob(
        search_name=search_name,
        compression_job=compression_job,
        options=options,
        query=query,
        subpath_to_search=None,
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
    search_type: ClpJsonSearchType, compression_job: PackageCompressionJob
) -> list[str]:
    match search_type:
        case ClpJsonSearchType.BASIC:
            return [
                "--raw",
            ]
        case ClpJsonSearchType.IGNORE_CASE:
            return [
                "--ignore-case",
                "--raw",
            ]
        case ClpJsonSearchType.COUNT_RESULTS:
            return [
                "--count",
                "--raw",
            ]
        case ClpJsonSearchType.COUNT_BY_TIME:
            return [
                "--count-by-time",
                "10",
                "--raw",
            ]
        case ClpJsonSearchType.TIME_RANGE:
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


def _search_type_to_search_name(search_type: ClpJsonSearchType) -> str:
    match search_type:
        case ClpJsonSearchType.BASIC:
            return "basic"
        case ClpJsonSearchType.IGNORE_CASE:
            return "ignore case"
        case ClpJsonSearchType.COUNT_RESULTS:
            return "count results"
        case ClpJsonSearchType.COUNT_BY_TIME:
            return "count by time"
        case ClpJsonSearchType.TIME_RANGE:
            return "time range"
        case _:
            err_msg = (
                f"Search type {search_type} has not been configured for search name construction."
            )
            raise ValueError(err_msg)


def _modify_search_result_for_search_type(
    search_type: ClpJsonSearchType, search_result: str
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


def _get_grep_options_from_search_type(search_type: ClpJsonSearchType) -> list[str]:
    grep_cmd_options: list[str] = [
        "--recursive",
        "--no-filename",
        "--color=never",
    ]

    match search_type:
        case ClpJsonSearchType.BASIC:
            return grep_cmd_options
        case ClpJsonSearchType.IGNORE_CASE:
            grep_cmd_options.append("--ignore-case")
            return grep_cmd_options
        case ClpJsonSearchType.COUNT_RESULTS:
            grep_cmd_options.append("--count")
            return grep_cmd_options
        case ClpJsonSearchType.COUNT_BY_TIME:
            grep_cmd_options.append("--count")
            return grep_cmd_options
        case ClpJsonSearchType.TIME_RANGE:
            return grep_cmd_options
        case _:
            err_msg = (
                f"Search type {search_type} has not been configured for grep command construction."
            )
            raise ValueError(err_msg)


def _get_grep_pipe_from_search_type(search_type: ClpJsonSearchType) -> str | None:
    match search_type:
        case ClpJsonSearchType.BASIC | ClpJsonSearchType.IGNORE_CASE | ClpJsonSearchType.TIME_RANGE:
            return None
        case ClpJsonSearchType.COUNT_RESULTS | ClpJsonSearchType.COUNT_BY_TIME:
            return "awk '{s+=$1} END {print s}'"
        case _:
            err_msg = (
                f"Search type {search_type} has not been configured for grep pipe construction."
            )
            raise ValueError(err_msg)
