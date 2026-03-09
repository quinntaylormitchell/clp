"""Tests for the clp-text package."""

import logging

import pytest

from tests.clp_package_tests.clp_package_utils.actions import (
    run_compress_cmd,
    run_search_cmd,
)
from tests.clp_package_tests.clp_package_utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
    ClpPackageTestPathConfig,
)
from tests.clp_package_tests.clp_text.clp_text_utils import (
    CLP_TEXT_MODE,
    verify_compress_action_clp_text,
    verify_search_action_clp_text,
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
    clp_package.clear_archives()

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
    clp_package.clear_archives()


@pytest.mark.search
def test_clp_text_search_text_multifile_basic(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    text_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    # Initial cleanup.
    clp_package.clear_archives()

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
    clp_package.clear_archives()
