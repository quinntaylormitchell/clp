"""Tests for the clp-json package."""

import logging

import pytest

from tests.package_tests.clp_json.utils.clear_archives import (
    clear_package_archives_clp_json,
)
from tests.package_tests.clp_json.utils.dataset_manager import (
    ClpPackageDatasetManagerType,
    dataset_manager_clp_json,
    verify_dataset_manager_del_action_clp_json,
    verify_dataset_manager_list_action_clp_json,
)
from tests.package_tests.clp_json.utils.mode import CLP_JSON_MODE
from tests.package_tests.utils.archive_manager import (
    archive_manager_clp_package,
    ClpPackageArchiveManagerType,
    verify_archive_manager_del_action,
    verify_archive_manager_find_action,
)
from tests.package_tests.utils.classes import (
    ClpPackage,
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
    IntegrationTestDataset,
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
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_startup'")

    assert clp_package

    logger.info("Test complete: 'test_clp_json_startup'")


@pytest.mark.compression
def test_clp_json_compression_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_compression_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_compression_json_multifile'")


@pytest.mark.search
def test_clp_json_search_basic_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_search_basic_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.BASIC,
        wildcard_query=json_multifile.metadata_dict["single_match_wildcard_query"],
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.BASIC, json_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_search_basic_json_multifile'")


@pytest.mark.search
def test_clp_json_search_ignore_case_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_search_ignore_case_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.IGNORE_CASE,
        wildcard_query=json_multifile.metadata_dict["single_match_wildcard_query"],
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.IGNORE_CASE, json_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_search_ignore_case_json_multifile'")


@pytest.mark.search
def test_clp_json_search_count_results_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_search_count_results_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.COUNT_RESULTS,
        wildcard_query=json_multifile.metadata_dict["single_match_wildcard_query"],
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.COUNT_RESULTS, json_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_search_count_results_json_multifile'")


@pytest.mark.search
def test_clp_json_search_count_by_time_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_search_count_by_time_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.COUNT_BY_TIME,
        wildcard_query=json_multifile.metadata_dict["single_match_wildcard_query"],
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.COUNT_BY_TIME, json_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_search_count_by_time_json_multifile'")


@pytest.mark.search
def test_clp_json_search_time_range_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_search_time_range_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    search_action = search_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpPackageSearchType.TIME_RANGE,
        wildcard_query=json_multifile.metadata_dict["single_match_wildcard_query"],
    )
    verified, failure_message = verify_search_action(
        search_action, ClpPackageSearchType.TIME_RANGE, json_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_search_time_range_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.dataset_manager
def test_clp_json_dataset_manager_list_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_dataset_manager_list_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    dataset_manager_list_action = dataset_manager_clp_json(
        clp_package=clp_package,
        dataset_manager_type=ClpPackageDatasetManagerType.LIST,
    )
    verified, failure_message = verify_dataset_manager_list_action_clp_json(
        dataset_manager_list_action, clp_package
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_dataset_manager_list_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.dataset_manager
def test_clp_json_dataset_manager_del_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_dataset_manager_del_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    dataset_manager_del_action = dataset_manager_clp_json(
        clp_package=clp_package,
        dataset_manager_type=ClpPackageDatasetManagerType.DEL,
        datasets_to_del=[
            json_multifile,
        ],
    )
    verified, failure_message = verify_dataset_manager_del_action_clp_json(
        dataset_manager_del_action, clp_package
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_dataset_manager_del_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_json_archive_manager_find_all_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_archive_manager_find_all_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    archive_manager_find_all_action = archive_manager_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
    )
    verified, failure_message = verify_archive_manager_find_action(
        archive_manager_find_all_action, clp_package, json_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_archive_manager_find_all_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_json_archive_manager_find_range_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_archive_manager_find_range_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    archive_manager_find_range_action = archive_manager_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
        begin_ts=json_multifile.metadata_dict["begin_ts"],
        end_ts=json_multifile.metadata_dict["end_ts"],
    )
    verified, failure_message = verify_archive_manager_find_action(
        archive_manager_find_range_action, clp_package, json_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_archive_manager_find_range_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_json_archive_manager_del_by_ids_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_archive_manager_del_by_ids_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    archive_manager_del_by_ids_action = archive_manager_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        archive_manager_type=ClpPackageArchiveManagerType.DEL_BY_IDS,
    )
    verified, failure_message = verify_archive_manager_del_action(
        archive_manager_del_by_ids_action, clp_package, json_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_archive_manager_del_by_ids_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_json_archive_manager_del_by_filter_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_archive_manager_del_by_filter_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    archive_manager_del_by_filter_action = archive_manager_clp_package(
        clp_package=clp_package,
        dataset=json_multifile,
        archive_manager_type=ClpPackageArchiveManagerType.DEL_BY_FILTER,
        begin_ts=json_multifile.metadata_dict["begin_ts"],
        end_ts=json_multifile.metadata_dict["end_ts"],
    )
    archive_manager_del_by_filter_verified, failure_message = verify_archive_manager_del_action(
        archive_manager_del_by_filter_action, clp_package, json_multifile
    )
    assert archive_manager_del_by_filter_verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_archive_manager_del_by_filter_json_multifile'")
