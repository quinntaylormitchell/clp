"""Tests for the clp-json package."""

import logging

import pytest

from tests.clp_package_tests.clp_json.utils.clear_archives import clear_package_archives_clp_json
from tests.clp_package_tests.clp_json.utils.dataset_manager import (
    ClpPackageDatasetManagerType,
    dataset_manager_clp_json,
    verify_dataset_manager_del_action_clp_json,
    verify_dataset_manager_list_action_clp_json,
)
from tests.clp_package_tests.clp_json.utils.mode import CLP_JSON_MODE
from tests.clp_package_tests.utils.archive_manager import (
    archive_manager_clp_json,
    ClpPackageArchiveManagerType,
    verify_archive_manager_del_action,
    verify_archive_manager_find_action,
)
from tests.clp_package_tests.utils.classes import (
    ClpPackage,
)
from tests.clp_package_tests.utils.compress import compress_clp_json, verify_compress_action
from tests.clp_package_tests.utils.search import (
    ClpPackageSearchType,
    search_clp_json,
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

    compress_action = compress_clp_json(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_compression_json_multifile'")


@pytest.mark.search
def test_clp_json_search_json_multifile_basic(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_search_json_multifile_basic'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_json(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    for search_type in ClpPackageSearchType:
        if search_type is ClpPackageSearchType.FILE_PATH:
            continue
        search_action = search_clp_json(
            clp_package=clp_package,
            dataset=json_multifile,
            search_type=search_type,
            wildcard_query=(
                '"detail":"Roll program complete, heads down attitude achieved for ascent"'
            ),
        )
        verified, failure_message = verify_search_action(search_action, search_type, json_multifile)
        assert verified, failure_message

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_search_json_multifile_basic'")


@pytest.mark.admin_tools
@pytest.mark.dataset_manager
def test_clp_json_dataset_manager_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_dataset_manager_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_json(clp_package, json_multifile)
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

    logger.info("Test complete: 'test_clp_json_dataset_manager_json_multifile'")


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_json_archive_manager_json_multifile(
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_archive_manager_json_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_action = compress_clp_json(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message

    archive_manager_find_all_action = archive_manager_clp_json(
        clp_package=clp_package,
        dataset=json_multifile,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
    )
    verified, failure_message = verify_archive_manager_find_action(
        archive_manager_find_all_action, clp_package, json_multifile
    )
    assert verified, failure_message

    archive_manager_find_range_action = archive_manager_clp_json(
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

    archive_manager_del_by_ids_action = archive_manager_clp_json(
        clp_package=clp_package,
        dataset=json_multifile,
        archive_manager_type=ClpPackageArchiveManagerType.DEL_BY_IDS,
    )
    verified, failure_message = verify_archive_manager_del_action(
        archive_manager_del_by_ids_action, clp_package, json_multifile
    )
    assert verified, failure_message

    archive_manager_del_by_filter_action = archive_manager_clp_json(
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

    logger.info("Test complete: 'test_clp_json_archive_manager_json_multifile'")
