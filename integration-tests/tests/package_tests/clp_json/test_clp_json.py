"""Tests for the clp-json package."""

import logging

import pytest

from tests.package_tests.classes import (
    ClpPackage,
)
from tests.package_tests.clp_json.utils.dataset_manager import (
    dataset_manager_del,
    dataset_manager_list,
    verify_dataset_manager_del_action_clp_json,
    verify_dataset_manager_list_action_clp_json,
)
from tests.package_tests.clp_json.utils.mode import CLP_JSON_MODE
from tests.package_tests.utils.archive_manager import (
    archive_manager_del_by_filter,
    archive_manager_del_by_ids,
    archive_manager_find,
    extract_archive_ids_from_find_output,
    verify_archive_manager_del_by_filter_action,
    verify_archive_manager_del_by_ids_action,
    verify_archive_manager_find_action,
)
from tests.package_tests.utils.compress import (
    compress_clp_package,
    verify_compress_action,
)
from tests.package_tests.utils.search import (
    ClpPackageSearchType,
    search_clp_package,
    verify_search_action,
)
from tests.utils.classes import (
    SampleDataset,
)

logger = logging.getLogger(__name__)


# Pytest markers for this module.
pytestmark = [
    pytest.mark.package,
    pytest.mark.clp_json,
    pytest.mark.parametrize(
        "clp_package", [CLP_JSON_MODE], indirect=True, ids=[CLP_JSON_MODE.mode_name]
    ),
]


@pytest.mark.startup
def test_clp_json_startup(clp_package: ClpPackage) -> None:
    """
    Verifies that the clp-json package starts up successfully.

    :param clp_package:
    """
    logger.info("Starting test: 'test_clp_json_startup'")

    assert clp_package

    logger.info("Test complete: 'test_clp_json_startup'")


@pytest.mark.compression
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_compression_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that the clp-json package can compress the `json_multifile` dataset.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_compression_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_compression_json_multifile'")


@pytest.mark.search
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_search_basic_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that a basic search on the compressed `json_multifile` dataset returns the expected
    results.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_search_basic_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.BASIC,
        wildcard_query=json_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.BASIC, json_multifile)
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_search_basic_json_multifile'")


@pytest.mark.search
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_search_ignore_case_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that a case-insensitive search on the compressed `json_multifile` dataset returns the
    expected results.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_search_ignore_case_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.IGNORE_CASE,
        wildcard_query=json_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.IGNORE_CASE, json_multifile)
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_search_ignore_case_json_multifile'")


@pytest.mark.search
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_search_count_results_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that a count-results search on the compressed `json_multifile` dataset returns the
    expected count.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_search_count_results_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.COUNT_RESULTS,
        wildcard_query=json_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.COUNT_RESULTS, json_multifile)
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_search_count_results_json_multifile'")


@pytest.mark.search
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_search_count_by_time_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that a count-by-time search on the compressed `json_multifile` dataset returns the
    expected aggregation.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_search_count_by_time_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.COUNT_BY_TIME,
        wildcard_query=json_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.COUNT_BY_TIME, json_multifile)
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_search_count_by_time_json_multifile'")


@pytest.mark.search
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_search_time_range_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that a time-range search on the compressed `json_multifile` dataset returns the
    expected results.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_search_time_range_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.TIME_RANGE,
        wildcard_query=json_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.TIME_RANGE, json_multifile)
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_search_time_range_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.dataset_manager
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_dataset_manager_list_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that the dataset-manager `list` action reports the compressed `json_multifile` dataset.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_dataset_manager_list_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    dataset_manager_list_action = dataset_manager_list(clp_package=clp_package)
    result = verify_dataset_manager_list_action_clp_json(
        dataset_manager_list_action, [json_multifile.dataset_name]
    )
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_dataset_manager_list_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.dataset_manager
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_dataset_manager_del_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that the dataset-manager `del` action removes the compressed `json_multifile` dataset.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_dataset_manager_del_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    dataset_manager_del_action = dataset_manager_del(
        clp_package=clp_package,
        datasets_to_del=[json_multifile],
    )
    result = verify_dataset_manager_del_action_clp_json(dataset_manager_del_action, clp_package)
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_dataset_manager_del_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_archive_manager_find_all_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that the archive-manager `find` action lists all archives for the compressed
    `json_multifile` dataset.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_archive_manager_find_all_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    archive_manager_find_all_action = archive_manager_find(
        clp_package=clp_package,
        dataset=json_multifile,
    )
    result = verify_archive_manager_find_action(
        archive_manager_find_all_action, clp_package, json_multifile
    )
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_archive_manager_find_all_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_archive_manager_find_range_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that the archive-manager `find` action with a time-range filter lists the expected
    archives for the compressed `json_multifile` dataset.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_archive_manager_find_range_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    archive_manager_find_range_action = archive_manager_find(
        clp_package=clp_package,
        dataset=json_multifile,
        begin_ts=json_multifile.metadata.begin_ts,
        end_ts=json_multifile.metadata.end_ts,
    )
    result = verify_archive_manager_find_action(
        archive_manager_find_range_action, clp_package, json_multifile
    )
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_archive_manager_find_range_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_archive_manager_del_by_ids_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that the archive-manager `del` action removes archives identified by ID for the
    compressed `json_multifile` dataset.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_archive_manager_del_by_ids_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    find_action = archive_manager_find(clp_package=clp_package, dataset=json_multifile)
    find_result = verify_archive_manager_find_action(find_action, clp_package, json_multifile)
    assert find_result, find_result.failure_message
    archive_ids = extract_archive_ids_from_find_output(find_action)

    archive_manager_del_by_ids_action = archive_manager_del_by_ids(
        clp_package=clp_package,
        ids=archive_ids,
        dataset=json_multifile,
    )
    result = verify_archive_manager_del_by_ids_action(
        archive_manager_del_by_ids_action, clp_package, json_multifile
    )
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_archive_manager_del_by_ids_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_archive_manager_del_by_filter_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that the archive-manager `del` action with a time-range filter removes the expected
    archives for the compressed `json_multifile` dataset.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_archive_manager_del_by_filter_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    archive_manager_del_by_filter_action = archive_manager_del_by_filter(
        clp_package=clp_package,
        dataset=json_multifile,
        begin_ts=json_multifile.metadata.begin_ts,
        end_ts=json_multifile.metadata.end_ts,
    )
    result = verify_archive_manager_del_by_filter_action(
        archive_manager_del_by_filter_action, clp_package, json_multifile
    )
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_archive_manager_del_by_filter_json_multifile'")
