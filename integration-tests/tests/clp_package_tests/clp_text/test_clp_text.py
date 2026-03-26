"""Tests for the clp-text package."""

import logging

import pytest

from tests.clp_package_tests.clp_text.utils.clear_archives import (
    clear_package_archives_clp_text,
)
from tests.clp_package_tests.clp_text.utils.mode import CLP_TEXT_MODE
from tests.clp_package_tests.utils.archive_manager import (
    archive_manager_clp_package,
    ClpPackageArchiveManagerType,
    verify_archive_manager_del_action,
    verify_archive_manager_find_action,
)
from tests.clp_package_tests.utils.classes import (
    ClpPackage,
)
from tests.clp_package_tests.utils.compress import (
    compress_clp_package,
    verify_compress_action,
)
from tests.clp_package_tests.utils.search import (
    ClpPackageSearchType,
    search_clp_package,
    verify_search_action,
)
from tests.utils.classes import (
    IntegrationTestDataset,
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
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_startup'")

    assert clp_package

    logger.info("Test complete: 'test_clp_text_startup'")


@pytest.mark.compression
def test_clp_text_compression_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_compression_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_compression_text_multifile'")


@pytest.mark.search
def test_clp_text_search_basic_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_search_basic_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.BASIC,
        wildcard_query="Saturn",
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.BASIC, text_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_basic_text_multifile'")


@pytest.mark.search
def test_clp_text_search_file_path_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_search_file_path_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.FILE_PATH,
        wildcard_query="Saturn",
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.FILE_PATH, text_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_file_path_text_multifile'")


@pytest.mark.search
def test_clp_text_search_ignore_case_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_search_ignore_case_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.IGNORE_CASE,
        wildcard_query="Saturn",
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.IGNORE_CASE, text_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_ignore_case_text_multifile'")


@pytest.mark.search
def test_clp_text_search_count_results_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_search_count_results_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.COUNT_RESULTS,
        wildcard_query="Saturn",
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.COUNT_RESULTS, text_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_count_results_text_multifile'")


@pytest.mark.search
def test_clp_text_search_count_by_time_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_search_count_by_time_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.COUNT_BY_TIME,
        wildcard_query="Saturn",
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.COUNT_BY_TIME, text_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_count_by_time_text_multifile'")


@pytest.mark.search
def test_clp_text_search_time_range_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_search_time_range_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=text_multifile,
        search_type=ClpPackageSearchType.TIME_RANGE,
        wildcard_query="Saturn",
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.TIME_RANGE, text_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_time_range_text_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_text_archive_manager_find_all_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_archive_manager_find_all_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    archive_manager_find_all_action = archive_manager_clp_package(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
    )
    verified, failure_message = verify_archive_manager_find_action(
        archive_manager_find_all_action, clp_package
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_archive_manager_find_all_text_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_text_archive_manager_find_range_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_archive_manager_find_range_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    archive_manager_find_range_action = archive_manager_clp_package(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
        begin_ts=text_multifile.metadata_dict["begin_ts"],
        end_ts=text_multifile.metadata_dict["end_ts"],
    )
    verified, failure_message = verify_archive_manager_find_action(
        archive_manager_find_range_action, clp_package
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_archive_manager_find_range_text_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_text_archive_manager_del_by_ids_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_archive_manager_del_by_ids_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    archive_manager_del_by_ids_action = archive_manager_clp_package(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.DEL_BY_IDS,
    )
    verified, failure_message = verify_archive_manager_del_action(
        archive_manager_del_by_ids_action, clp_package
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_archive_manager_del_by_ids_text_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_text_archive_manager_del_by_filter_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_archive_manager_del_by_filter_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_package(clp_package, text_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, text_multifile)
    assert verified, failure_message

    archive_manager_del_by_filter_action = archive_manager_clp_package(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.DEL_BY_FILTER,
        begin_ts=text_multifile.metadata_dict["begin_ts"],
        end_ts=text_multifile.metadata_dict["end_ts"],
    )
    verified, failure_message = verify_archive_manager_del_action(
        archive_manager_del_by_filter_action, clp_package
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_archive_manager_del_by_filter_text_multifile'")
