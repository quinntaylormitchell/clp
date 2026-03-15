"""Tests for the clp-text package."""

import pytest

from tests.clp_package_tests.clp_text.utils.archive_manager import (
    _verify_archive_manager_del_action_clp_text,
    _verify_archive_manager_find_action_clp_text,
    archive_manager_del_by_filter_clp_text,
    archive_manager_del_by_ids_clp_text,
    archive_manager_find_clp_text,
    clear_package_archives_clp_text,
)
from tests.clp_package_tests.clp_text.utils.compress import (
    compress_clp_text,
    verify_compress_action_clp_text,
)
from tests.clp_package_tests.clp_text.utils.mode import CLP_TEXT_MODE
from tests.clp_package_tests.clp_text.utils.search import (
    ClpTextSearchType,
    search_clp_text,
    verify_search_action_clp_text,
)
from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageTestPathConfig,
)
from tests.utils.classes import (
    IntegrationTestDataset,
)

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
    # Do nothing; CLP package startup is verified before the fixture is given to this test.
    assert clp_package


@pytest.mark.compression
def test_clp_text_compression_text_multifile(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_text(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=text_multifile,
    )
    compress_action_verified, failure_message = verify_compress_action_clp_text(
        compress_action, clp_package, text_multifile
    )
    assert compress_action_verified, failure_message

    clear_package_archives_clp_text(clp_package)


@pytest.mark.search
def test_clp_text_search_text_multifile_basic(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_text(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=text_multifile,
    )
    compress_action_verified, failure_message = verify_compress_action_clp_text(
        compress_action, clp_package, text_multifile
    )
    assert compress_action_verified, failure_message

    for search_type in ClpTextSearchType:
        search_action = search_clp_text(
            clp_package_test_path_config=clp_package_test_path_config,
            clp_package=clp_package,
            dataset=text_multifile,
            search_type=search_type,
            wildcard_query="Saturn",
        )
        search_action_verified, failure_message = verify_search_action_clp_text(
            search_action, text_multifile
        )
        assert search_action_verified, failure_message

    clear_package_archives_clp_text(clp_package)


@pytest.mark.admin_tools
def test_clp_text_archive_manager_text_multifile(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    clear_package_archives_clp_text(clp_package)

    compress_action = compress_clp_text(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=text_multifile,
    )
    compress_action_verified, failure_message = verify_compress_action_clp_text(
        compress_action, clp_package, text_multifile
    )
    assert compress_action_verified, failure_message

    find_all_action = archive_manager_find_clp_text(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
    )
    archive_manager_find_all_verified, failure_message = (
        _verify_archive_manager_find_action_clp_text(find_all_action, clp_package)
    )
    assert archive_manager_find_all_verified, failure_message

    find_range_action = archive_manager_find_clp_text(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        begin_ts=text_multifile.metadata_dict["begin_ts_ms"],
        end_ts=text_multifile.metadata_dict["end_ts_ms"],
    )
    archive_manager_find_range_verified, failure_message = (
        _verify_archive_manager_find_action_clp_text(find_range_action, clp_package)
    )
    assert archive_manager_find_range_verified, failure_message

    del_by_ids_action = archive_manager_del_by_ids_clp_text(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
    )
    archive_manager_del_by_ids_verified, failure_message = (
        _verify_archive_manager_del_action_clp_text(del_by_ids_action, clp_package)
    )
    assert archive_manager_del_by_ids_verified, failure_message

    del_by_filter_action = archive_manager_del_by_filter_clp_text(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        begin_ts=text_multifile.metadata_dict["begin_ts_ms"],
        end_ts=text_multifile.metadata_dict["end_ts_ms"],
    )
    archive_manager_del_by_filter_verified, failure_message = (
        _verify_archive_manager_del_action_clp_text(del_by_filter_action, clp_package)
    )
    assert archive_manager_del_by_filter_verified, failure_message

    clear_package_archives_clp_text(clp_package)
