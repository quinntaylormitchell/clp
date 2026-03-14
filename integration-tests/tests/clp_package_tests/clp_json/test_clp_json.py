"""Tests for the clp-json package."""

import logging

import pytest

from tests.clp_package_tests.clp_json.utils.archive_manager import (
    archive_manager_del_by_filter_clp_json,
    archive_manager_del_by_ids_clp_json,
    archive_manager_find_clp_json,
)
from tests.clp_package_tests.clp_json.utils.compress import compress_clp_json
from tests.clp_package_tests.clp_json.utils.dataset_manager import (
    clear_package_archives_clp_json,
    dataset_manager_del_clp_json,
    dataset_manager_list_clp_json,
)
from tests.clp_package_tests.clp_json.utils.mode import CLP_JSON_MODE
from tests.clp_package_tests.clp_json.utils.search import (
    ClpJsonSearchType,
    search_clp_json,
)
from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageTestPathConfig,
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
    # Do nothing; CLP package startup is verified before the fixture is given to this test.
    assert clp_package


@pytest.mark.compression
def test_clp_json_compression_json_multifile(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    clear_package_archives_clp_json(clp_package)

    compress_action_verified, failure_message = compress_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=json_multifile,
    )
    assert compress_action_verified, failure_message

    clear_package_archives_clp_json(clp_package)


@pytest.mark.search
def test_clp_json_search_json_multifile_basic(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    clear_package_archives_clp_json(clp_package)

    compress_action_verified, failure_message = compress_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=json_multifile,
    )
    assert compress_action_verified, failure_message

    search_action_verified, failure_message = search_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=json_multifile,
        search_type=ClpJsonSearchType.BASIC,
        wildcard_query='"detail":"Roll program complete, heads down attitude achieved for ascent"',
    )
    assert search_action_verified, failure_message

    clear_package_archives_clp_json(clp_package)


@pytest.mark.admin_tools
@pytest.mark.dataset_manager
def test_clp_json_dataset_manager_json_multifile(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    clear_package_archives_clp_json(clp_package)

    compress_action_verified, failure_message = compress_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=json_multifile,
    )
    assert compress_action_verified, failure_message

    dataset_manager_list_action_verified, failure_message = dataset_manager_list_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
    )
    assert dataset_manager_list_action_verified, failure_message

    dataset_manager_del_action_verified, failure_message = dataset_manager_del_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        datasets_to_del=[
            json_multifile,
        ],
    )
    assert dataset_manager_del_action_verified, failure_message

    clear_package_archives_clp_json(clp_package)


@pytest.mark.admin_tools
@pytest.mark.archive_manager
def test_clp_json_archive_manager_json_multifile(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    clear_package_archives_clp_json(clp_package)

    compress_action_verified, failure_message = compress_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=json_multifile,
    )
    assert compress_action_verified, failure_message

    archive_manager_find_all_verified, failure_message = archive_manager_find_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=json_multifile,
    )
    assert archive_manager_find_all_verified, failure_message

    archive_manager_find_range_verified, failure_message = archive_manager_find_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=json_multifile,
        begin_ts=json_multifile.metadata_dict["begin_ts_ms"],
        end_ts=json_multifile.metadata_dict["end_ts_ms"],
    )
    assert archive_manager_find_range_verified, failure_message

    archive_manager_del_by_ids_verified, failure_message = archive_manager_del_by_ids_clp_json(
        clp_package_test_path_config=clp_package_test_path_config,
        clp_package=clp_package,
        dataset=json_multifile,
    )
    assert archive_manager_del_by_ids_verified, failure_message

    archive_manager_del_by_filter_verified, failure_message = (
        archive_manager_del_by_filter_clp_json(
            clp_package_test_path_config=clp_package_test_path_config,
            clp_package=clp_package,
            dataset=json_multifile,
            begin_ts=json_multifile.metadata_dict["begin_ts_ms"],
            end_ts=json_multifile.metadata_dict["end_ts_ms"],
        )
    )
    assert archive_manager_del_by_filter_verified, failure_message

    clear_package_archives_clp_json(clp_package)
