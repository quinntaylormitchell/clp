"""Tests for the clp-json-s3 package."""

import logging

import pytest

from tests.package_tests.classes import (
    ClpPackage,
)
from tests.package_tests.clp_json_s3.utils.compress_s3 import (
    compress_s3_clp_package,
)
from tests.package_tests.clp_json_s3.utils.mode import CLP_JSON_S3_MODE
from tests.utils.classes import (
    SampleDataset,
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
    """
    Verifies that the clp-json-s3 package starts up successfully.

    :param clp_package:
    """
    logger.info("Starting test: 'test_clp_json_s3_startup'")

    assert clp_package

    logger.info("Test complete: 'test_clp_json_s3_startup'")


@pytest.mark.compression_s3
@pytest.mark.usefixtures("clear_package_archives")
def test_clp_json_s3_compression_json_s3_multifile(
    clp_package: ClpPackage,
    json_s3_multifile: SampleDataset,
) -> None:
    """
    Verifies that the clp-json-s3 package can compress the `json_s3_multifile` dataset from S3.

    :param clp_package:
    :param json_s3_multifile:
    """
    logger.info("Starting test: 'test_clp_json_s3_compression_json_s3_multifile'")

    compress_s3_clp_package(clp_package, json_s3_multifile)

    logger.info("Test complete: 'test_clp_json_s3_compression_json_s3_multifile'")
