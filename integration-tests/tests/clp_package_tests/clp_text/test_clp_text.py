"""Tests for the clp-text package."""

import logging

import pytest

from tests.clp_package_tests.clp_text.utils.archive_manager import (
    archive_manager_clp_text,
    clear_package_archives_clp_text,
    verify_archive_manager_del_action_clp_text,
    verify_archive_manager_find_action_clp_text,
)
from tests.clp_package_tests.clp_text.utils.compress import (
    compress_clp_text,
    verify_compress_action_clp_text,
)
from tests.clp_package_tests.clp_text.utils.mode import CLP_TEXT_MODE
from tests.clp_package_tests.clp_text.utils.search import (
    search_clp_text,
    verify_search_action_clp_text,
)
from tests.clp_package_tests.utils.classes import (
    ClpPackage,
)
from tests.clp_package_tests.utils.parsers import ClpPackageArchiveManagerType, ClpPackageSearchType
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

    # Do nothing; CLP package startup is verified before the fixture is given to this test.
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

    compress_action = compress_clp_text(clp_package, text_multifile)
    verified, failure_message = verify_compress_action_clp_text(
        compress_action, clp_package, text_multifile
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_compression_text_multifile'")


@pytest.mark.search
def test_clp_text_search_text_multifile_basic(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_search_text_multifile_basic'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_text(clp_package, text_multifile)
    verified, failure_message = verify_compress_action_clp_text(
        compress_action, clp_package, text_multifile
    )
    assert verified, failure_message

    for search_type in ClpPackageSearchType:
        search_action = search_clp_text(
            clp_package=clp_package,
            dataset=text_multifile,
            search_type=search_type,
            wildcard_query="Saturn",
        )
        verified, failure_message = verify_search_action_clp_text(
            search_action, search_type, text_multifile
        )
        assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_search_text_multifile_basic'")


@pytest.mark.admin_tools
def test_clp_text_archive_manager_text_multifile(
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_text_archive_manager_text_multifile'")

    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_text(clp_package, text_multifile)
    verified, failure_message = verify_compress_action_clp_text(
        compress_action, clp_package, text_multifile
    )
    assert verified, failure_message

    find_all_action = archive_manager_clp_text(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
    )
    verified, failure_message = verify_archive_manager_find_action_clp_text(
        find_all_action, clp_package
    )
    assert verified, failure_message

    find_range_action = archive_manager_clp_text(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
        begin_ts=text_multifile.metadata_dict["begin_ts"],
        end_ts=text_multifile.metadata_dict["end_ts"],
    )
    verified, failure_message = verify_archive_manager_find_action_clp_text(
        find_range_action, clp_package
    )
    assert verified, failure_message

    del_by_ids_action = archive_manager_clp_text(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.DEL_BY_IDS,
    )
    verified, failure_message = verify_archive_manager_del_action_clp_text(
        del_by_ids_action, clp_package
    )
    assert verified, failure_message

    del_by_filter_action = archive_manager_clp_text(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.DEL_BY_FILTER,
        begin_ts=text_multifile.metadata_dict["begin_ts"],
        end_ts=text_multifile.metadata_dict["end_ts"],
    )
    verified, failure_message = verify_archive_manager_del_action_clp_text(
        del_by_filter_action, clp_package
    )
    assert verified, failure_message

    clear_package_archives_clp_text(clp_package)

    logger.info("Test complete: 'test_clp_text_archive_manager_text_multifile'")
