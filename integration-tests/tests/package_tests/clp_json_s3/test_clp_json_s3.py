"""Tests for the clp-json-s3 package."""

import logging

import pytest

from tests.package_tests.clp_json.utils.clear_archives import (
    clear_package_archives_clp_json,
)
from tests.package_tests.clp_json_s3.utils.compress_s3 import (
    compress_s3_clp_package,
)
from tests.package_tests.clp_json_s3.utils.mode import CLP_JSON_S3_MODE
from tests.package_tests.utils.classes import (
    ClpPackage,
)
from tests.utils.classes import (
    IntegrationTestDataset,
)

logger = logging.getLogger(__name__)


# Pytest markers for this module.
pytestmark = [
    pytest.mark.package,
    pytest.mark.clp_json_s3,
    pytest.mark.parametrize(
        "clp_package",
        [CLP_JSON_S3_MODE],
        indirect=True,
        ids=[CLP_JSON_S3_MODE.mode_name],
    ),
]


@pytest.mark.startup
def test_clp_json_s3_startup(clp_package: ClpPackage) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_s3_startup'")

    assert clp_package

    logger.info("Test complete: 'test_clp_json_s3_startup'")


@pytest.mark.compression_s3
def test_clp_json_s3_compression_json_s3_multifile(
    clp_package: ClpPackage,
    json_s3_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_json_s3_compression_json_s3_multifile'")

    clear_package_archives_clp_json(clp_package)

    compress_s3_clp_package(clp_package, json_s3_multifile)

    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_json_s3_compression_json_s3_multifile'")
