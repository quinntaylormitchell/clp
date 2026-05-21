"""Tests for the clp-text package."""

import logging

import pytest

from tests.package_tests.classes import (
    ClpPackage,
)
from tests.package_tests.clp_text.utils.clear_archives import (
    clear_package_archives_clp_text,
)
from tests.package_tests.clp_text.utils.mode import CLP_TEXT_MODE
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
    pytest.mark.clp_text,
    pytest.mark.parametrize(
        "clp_package", [CLP_TEXT_MODE], indirect=True, ids=[CLP_TEXT_MODE.mode_name]
    ),
]


@pytest.mark.startup
def test_clp_text_startup(clp_package: ClpPackage) -> None:
    """
    Verifies that the clp-text package starts up successfully.

    :param clp_package:
    """
    logger.info("Starting test: 'test_clp_text_startup'")

    assert clp_package

    logger.info("Test complete: 'test_clp_text_startup'")


@pytest.mark.compression
def test_clp_text_compression_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that the clp-text package can compress the `text_multifile` dataset.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_compression_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_compression_text_multifile'")


@pytest.mark.search
def test_clp_text_search_basic_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that a basic search on the compressed `text_multifile` dataset returns the expected
    results.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_search_basic_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.BASIC,
        wildcard_query=text_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.BASIC, text_multifile)
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_basic_text_multifile'")


@pytest.mark.search
def test_clp_text_search_file_path_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that a file-path-filtered search on the compressed `text_multifile` dataset returns the
    expected results.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_search_file_path_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.FILE_PATH,
        wildcard_query=text_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.FILE_PATH, text_multifile)
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_file_path_text_multifile'")


@pytest.mark.search
def test_clp_text_search_ignore_case_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that a case-insensitive search on the compressed `text_multifile` dataset returns the
    expected results.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_search_ignore_case_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.IGNORE_CASE,
        wildcard_query=text_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.IGNORE_CASE, text_multifile)
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_ignore_case_text_multifile'")


@pytest.mark.search
def test_clp_text_search_count_results_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that a count-results search on the compressed `text_multifile` dataset returns the
    expected count.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_search_count_results_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.COUNT_RESULTS,
        wildcard_query=text_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.COUNT_RESULTS, text_multifile)
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_count_results_text_multifile'")


@pytest.mark.search
def test_clp_text_search_count_by_time_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that a count-by-time search on the compressed `text_multifile` dataset returns the
    expected aggregation.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_search_count_by_time_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.COUNT_BY_TIME,
        wildcard_query=text_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.COUNT_BY_TIME, text_multifile)
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_count_by_time_text_multifile'")


@pytest.mark.search
def test_clp_text_search_time_range_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that a time-range search on the compressed `text_multifile` dataset returns the
    expected results.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_search_time_range_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.TIME_RANGE,
        wildcard_query=text_multifile.metadata.single_match_wildcard_query,
    )
    result = verify_search_action(search_action, ClpPackageSearchType.TIME_RANGE, text_multifile)
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_time_range_text_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_text_archive_manager_find_all_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that the archive-manager `find` action lists all archives produced by compressing the
    `text_multifile` dataset.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_archive_manager_find_all_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    archive_manager_find_all_action = archive_manager_find(clp_package=clp_package)
    result = verify_archive_manager_find_action(archive_manager_find_all_action, clp_package)
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_archive_manager_find_all_text_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_text_archive_manager_find_range_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that the archive-manager `find` action with a time-range filter lists the expected
    archives produced by compressing the `text_multifile` dataset.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_archive_manager_find_range_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    archive_manager_find_range_action = archive_manager_find(
        clp_package=clp_package,
        begin_ts=text_multifile.metadata.begin_ts,
        end_ts=text_multifile.metadata.end_ts,
    )
    result = verify_archive_manager_find_action(archive_manager_find_range_action, clp_package)
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_archive_manager_find_range_text_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_text_archive_manager_del_by_ids_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that the archive-manager `del` action removes archives identified by ID for the
    compressed `text_multifile` dataset.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_archive_manager_del_by_ids_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    find_action = archive_manager_find(clp_package=clp_package)
    find_result = verify_archive_manager_find_action(find_action, clp_package)
    assert find_result, find_result.failure_message
    archive_ids = extract_archive_ids_from_find_output(find_action)

    archive_manager_del_by_ids_action = archive_manager_del_by_ids(
        clp_package=clp_package,
        ids=archive_ids,
    )
    result = verify_archive_manager_del_by_ids_action(
        archive_manager_del_by_ids_action, clp_package
    )
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_archive_manager_del_by_ids_text_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_text_archive_manager_del_by_filter_text_multifile(
    clp_package: ClpPackage,
    text_multifile: SampleDataset,
) -> None:
    """
    Verifies that the archive-manager `del` action with a time-range filter removes the expected
    archives for the compressed `text_multifile` dataset.

    :param clp_package:
    :param text_multifile:
    """
    logger.info("Starting test: 'test_clp_text_archive_manager_del_by_filter_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    result = verify_compress_action(compress_action, clp_package, text_multifile)
    assert result, result.failure_message

    archive_manager_del_by_filter_action = archive_manager_del_by_filter(
        clp_package=clp_package,
        begin_ts=text_multifile.metadata.begin_ts,
        end_ts=text_multifile.metadata.end_ts,
    )
    result = verify_archive_manager_del_by_filter_action(
        archive_manager_del_by_filter_action, clp_package
    )
    assert result, result.failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_archive_manager_del_by_filter_text_multifile'")
