"""Tests for the clp-json package."""

import logging
from pathlib import Path

import pytest

from tests.clp_package_tests.clp_json.clp_json_utils import (
    clear_package_archives_clp_json,
    CLP_JSON_MODE,
    verify_archive_manager_action_clp_json,
    verify_compress_action_clp_json,
    verify_dataset_manager_action_clp_json,
    verify_search_action_clp_json,
)
from tests.clp_package_tests.clp_package_utils.actions import (
    run_archive_manager_cmd,
    run_compress_cmd,
    run_dataset_manager_cmd,
    run_search_cmd,
)
from tests.clp_package_tests.clp_package_utils.classes import (
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

    logger.info("Compressing the `json_multifile` dataset.")
    compress_cmd = [
        str(clp_package_test_path_config.compress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        json_multifile.metadata_dict["dataset_name"],
        "--timestamp-key",
        json_multifile.metadata_dict["timestamp_key"],
        str(json_multifile.path_to_dataset_logs),
    ]
    compress_action = run_compress_cmd(compress_cmd)
    compress_action_verified, failure_message = verify_compress_action_clp_json(
        compress_action, clp_package
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
    # Initial cleanup.
    clear_package_archives_clp_json(clp_package)

    # Compress.
    compress_cmd = [
        str(clp_package_test_path_config.compress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        json_multifile.metadata_dict["dataset_name"],
        "--timestamp-key",
        json_multifile.metadata_dict["timestamp_key"],
        str(json_multifile.path_to_dataset_logs),
    ]
    compress_action = run_compress_cmd(compress_cmd)
    compress_action_verified, failure_message = verify_compress_action_clp_json(
        compress_action, clp_package
    )
    assert compress_action_verified, failure_message

    # Search.
    search_cmd = [
        str(clp_package_test_path_config.search_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        json_multifile.metadata_dict["dataset_name"],
        "--raw",
        '"detail":"Roll program complete, heads down attitude achieved for ascent"',
    ]
    search_action = run_search_cmd(search_cmd)
    search_action_verified, failure_message = verify_search_action_clp_json(
        search_action, json_multifile
    )
    assert search_action_verified, failure_message

    # Cleanup.
    clear_package_archives_clp_json(clp_package)


@pytest.mark.admin_tools
def test_clp_json_dataset_manager_json_multifile(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    # Initial cleanup.
    clear_package_archives_clp_json(clp_package)

    # Compress.
    compress_cmd = [
        str(clp_package_test_path_config.compress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        json_multifile.metadata_dict["dataset_name"],
        "--timestamp-key",
        json_multifile.metadata_dict["timestamp_key"],
        str(json_multifile.path_to_dataset_logs),
    ]
    compress_action = run_compress_cmd(compress_cmd)
    compress_action_verified, failure_message = verify_compress_action_clp_json(
        compress_action, clp_package
    )
    assert compress_action_verified, failure_message

    # Dataset-manager tests.
    dataset_manager_cmd = [
        str(clp_package_test_path_config.dataset_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "list",
    ]
    dataset_manager_action = run_dataset_manager_cmd(dataset_manager_cmd)
    dataset_manager_action_verified, failure_message = verify_dataset_manager_action_clp_json(
        dataset_manager_action, clp_package
    )
    assert dataset_manager_action_verified, failure_message

    dataset_manager_cmd = [
        str(clp_package_test_path_config.dataset_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "del",
        json_multifile.metadata_dict["dataset_name"],
    ]
    dataset_manager_action = run_dataset_manager_cmd(dataset_manager_cmd)
    dataset_manager_action_verified, failure_message = verify_dataset_manager_action_clp_json(
        dataset_manager_action, clp_package
    )
    assert dataset_manager_action_verified, failure_message

    # Cleanup.
    clear_package_archives_clp_json(clp_package)


@pytest.mark.admin_tools
def test_clp_json_archive_manager_json_multifile(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    # Initial cleanup.
    clear_package_archives_clp_json(clp_package)

    # Compress.
    compress_cmd = [
        str(clp_package_test_path_config.compress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        json_multifile.metadata_dict["dataset_name"],
        "--timestamp-key",
        json_multifile.metadata_dict["timestamp_key"],
        str(json_multifile.path_to_dataset_logs),
    ]
    compress_action = run_compress_cmd(compress_cmd)
    compress_action_verified, failure_message = verify_compress_action_clp_json(
        compress_action, clp_package
    )
    assert compress_action_verified, failure_message

    # Archive-manager tests.
    archive_manager_cmd = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        json_multifile.metadata_dict["dataset_name"],
        "find",
    ]
    archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
    archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_json(
        archive_manager_action, clp_package
    )
    assert archive_manager_action_verified, failure_message

    archive_manager_cmd = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        json_multifile.metadata_dict["dataset_name"],
        "find",
        "--begin-ts",
        str(json_multifile.metadata_dict["begin_ts_ms"]),
        "--end-ts",
        str(json_multifile.metadata_dict["end_ts_ms"]),
    ]
    archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
    archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_json(
        archive_manager_action, clp_package
    )
    assert archive_manager_action_verified, failure_message

    sample_id = _get_rand_subdirectory_name(clp_package_test_path_config.package_archives_path)
    archive_manager_cmd = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        json_multifile.metadata_dict["dataset_name"],
        "del",
        "by-ids",
        sample_id,
    ]
    archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
    archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_json(
        archive_manager_action, clp_package
    )
    assert archive_manager_action_verified, failure_message

    archive_manager_cmd = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        json_multifile.metadata_dict["dataset_name"],
        "del",
        "by-filter",
        "--begin-ts",
        str(json_multifile.metadata_dict["begin_ts_ms"]),
        "--end-ts",
        str(json_multifile.metadata_dict["end_ts_ms"]),
    ]
    archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
    archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_json(
        archive_manager_action, clp_package
    )
    assert archive_manager_action_verified, failure_message

    # Cleanup.
    clear_package_archives_clp_json(clp_package)


def _get_rand_subdirectory_name(path_to_parent: Path) -> str:
    for item in path_to_parent.iterdir():
        if item.is_dir():
            return item.name

    return ""
