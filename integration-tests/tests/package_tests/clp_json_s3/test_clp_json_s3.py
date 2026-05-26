"""Tests for the clp-json-s3 package."""

import logging

import pytest

from tests.package_tests.classes import ClpPackage
from tests.package_tests.clp_json_s3.classes import S3Dataset
from tests.package_tests.clp_json_s3.utils.compress_s3 import (
    compress_s3_clp_package,
    verify_compress_s3_action,
)
from tests.package_tests.clp_json_s3.utils.mode import (
    ALL_S3_MODE_PARAMS,
    ARCHIVE_OUTPUT_FS,
    ARCHIVE_OUTPUT_S3,
    LOGS_INPUT_FS,
    LOGS_INPUT_S3,
    make_s3_mode_param,
    STREAM_OUTPUT_FS,
    STREAM_OUTPUT_S3,
)
from tests.package_tests.utils.compress import compress_clp_package
from tests.utils.classes import SampleDataset

logger = logging.getLogger(__name__)


# Pytest markers for this module.
pytestmark = [
    pytest.mark.package,
    pytest.mark.clp_json_s3,
]


@pytest.mark.startup
@pytest.mark.parametrize(
    "clp_package",
    [*ALL_S3_MODE_PARAMS],
    indirect=True,
)
def test_clp_json_s3_startup(clp_package: ClpPackage) -> None:
    """
    Verifies that the clp-json-s3 package starts up successfully. Tested on all S3 mode
    parameterizations.

    :param clp_package:
    """
    logger.info("Starting test: 'test_clp_json_s3_startup'")

    assert clp_package

    logger.info("Test complete: 'test_clp_json_s3_startup'")


@pytest.mark.compression_s3
@pytest.mark.usefixtures("clear_package_archives")
@pytest.mark.parametrize(
    "clp_package",
    [
        make_s3_mode_param(LOGS_INPUT_FS, ARCHIVE_OUTPUT_FS, STREAM_OUTPUT_S3),
        make_s3_mode_param(LOGS_INPUT_FS, ARCHIVE_OUTPUT_S3, STREAM_OUTPUT_FS),
        make_s3_mode_param(LOGS_INPUT_FS, ARCHIVE_OUTPUT_S3, STREAM_OUTPUT_S3),
    ],
    indirect=True,
)
def test_clp_json_s3_compression_json_multifile(
    clp_package: ClpPackage,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that the clp-json-s3 package can compress the `json_multifile` sample dataset into S3.
    Tested on all S3 mode parameterizations that have `logs_input` set to `fs`.

    :param clp_package:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_json_s3_compression_json_multifile'")

    compress_action = compress_clp_package(clp_package, json_multifile)
    result = verify_compress_s3_action(compress_action, clp_package, json_multifile)
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_s3_compression_json_multifile'")


@pytest.mark.compression_s3
@pytest.mark.usefixtures("clear_package_archives")
@pytest.mark.parametrize(
    "clp_package",
    [
        make_s3_mode_param(LOGS_INPUT_S3, ARCHIVE_OUTPUT_FS, STREAM_OUTPUT_FS),
        make_s3_mode_param(LOGS_INPUT_S3, ARCHIVE_OUTPUT_FS, STREAM_OUTPUT_S3),
        make_s3_mode_param(LOGS_INPUT_S3, ARCHIVE_OUTPUT_S3, STREAM_OUTPUT_FS),
        make_s3_mode_param(LOGS_INPUT_S3, ARCHIVE_OUTPUT_S3, STREAM_OUTPUT_S3),
    ],
    indirect=True,
)
def test_clp_json_s3_compression_json_s3_multifile(
    clp_package: ClpPackage,
    json_s3_multifile: S3Dataset,
) -> None:
    """
    Verifies that the clp-json-s3 package can compress the `json_s3_multifile` S3 dataset into S3.
    Tested on all S3 mode parameterizations that have `logs_input` set to `s3`.

    :param clp_package:
    :param json_s3_multifile:
    """
    logger.info("Starting test: 'test_clp_json_s3_compression_json_s3_multifile'")

    compress_s3_action = compress_s3_clp_package(clp_package, json_s3_multifile)
    result = verify_compress_s3_action(compress_s3_action, clp_package, json_s3_multifile)
    assert result, result.failure_message

    logger.info("Test complete: 'test_clp_json_s3_compression_json_s3_multifile'")
