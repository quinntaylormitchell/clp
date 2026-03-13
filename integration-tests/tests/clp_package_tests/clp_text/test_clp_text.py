"""Tests for the clp-text package."""

from pathlib import Path

import pytest

from tests.clp_package_tests.clp_package_utils.actions import (
    run_archive_manager_cmd,
    run_compress_cmd,
    run_search_cmd,
)
from tests.clp_package_tests.clp_package_utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
    ClpPackageTestPathConfig,
)
from tests.clp_package_tests.clp_text.clp_text_utils import (
    clear_package_archives_clp_text,
    CLP_TEXT_MODE,
    verify_archive_manager_action_clp_text,
    verify_compress_action_clp_text,
    verify_search_action_clp_text,
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
    # Initial cleanup.
    clear_package_archives_clp_text(clp_package)

    # Compress.
    compress_cmd: list[str] = [
        str(clp_package_test_path_config.compress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        str(text_multifile.path_to_dataset_logs),
    ]
    compress_action: ClpPackageExternalAction = run_compress_cmd(compress_cmd)
    compress_action_verified, failure_message = verify_compress_action_clp_text(
        compress_action, clp_package, text_multifile
    )
    assert compress_action_verified, failure_message

    # Cleanup.
    clear_package_archives_clp_text(clp_package)


@pytest.mark.search
def test_clp_text_search_text_multifile_basic(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    # Initial cleanup.
    clear_package_archives_clp_text(clp_package)

    # Compress.
    compress_cmd: list[str] = [
        str(clp_package_test_path_config.compress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        str(text_multifile.path_to_dataset_logs),
    ]
    compress_action: ClpPackageExternalAction = run_compress_cmd(compress_cmd)
    compress_action_verified, failure_message = verify_compress_action_clp_text(
        compress_action, clp_package, text_multifile
    )
    assert compress_action_verified, failure_message

    # Search.
    search_cmd = [
        str(clp_package_test_path_config.search_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--raw",
        "Saturn",
    ]
    search_action: ClpPackageExternalAction = run_search_cmd(search_cmd)
    search_action_verified, failure_message = verify_search_action_clp_text(
        search_action, text_multifile
    )
    assert search_action_verified, failure_message

    # Cleanup.
    clear_package_archives_clp_text(clp_package)


@pytest.mark.admin_tools
def test_clp_text_archive_manager_text_multifile(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    # Initial cleanup.
    clear_package_archives_clp_text(clp_package)

    # Compress.
    compress_cmd: list[str] = [
        str(clp_package_test_path_config.compress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        str(text_multifile.path_to_dataset_logs),
    ]
    compress_action: ClpPackageExternalAction = run_compress_cmd(compress_cmd)
    compress_action_verified, failure_message = verify_compress_action_clp_text(
        compress_action, clp_package, text_multifile
    )
    assert compress_action_verified, failure_message

    # Archive-manager tests.
    archive_manager_cmd = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "find",
    ]
    archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
    archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_text(
        archive_manager_action, clp_package
    )
    assert archive_manager_action_verified, failure_message

    archive_manager_cmd = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "find",
        "--begin-ts",
        str(text_multifile.metadata_dict["begin_ts_ms"]),
        "--end-ts",
        str(text_multifile.metadata_dict["end_ts_ms"]),
    ]
    archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
    archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_text(
        archive_manager_action, clp_package
    )
    assert archive_manager_action_verified, failure_message

    sample_id = _get_rand_subdirectory_name(clp_package_test_path_config.package_archives_path)
    archive_manager_cmd = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "del",
        "by-ids",
        sample_id,
    ]
    archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
    archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_text(
        archive_manager_action, clp_package
    )
    assert archive_manager_action_verified, failure_message

    archive_manager_cmd = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "del",
        "by-filter",
        "--begin-ts",
        str(text_multifile.metadata_dict["begin_ts_ms"]),
        "--end-ts",
        str(text_multifile.metadata_dict["end_ts_ms"]),
    ]
    archive_manager_action = run_archive_manager_cmd(archive_manager_cmd)
    archive_manager_action_verified, failure_message = verify_archive_manager_action_clp_text(
        archive_manager_action, clp_package
    )
    assert archive_manager_action_verified, failure_message

    # Cleanup.
    clear_package_archives_clp_text(clp_package)


def _get_rand_subdirectory_name(path_to_parent: Path) -> str:
    for item in path_to_parent.iterdir():
        if item.is_dir():
            return item.name

    return ""
