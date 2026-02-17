"""Provides utility functions related to the CLP package used across `integration-tests`."""

import logging
from subprocess import SubprocessError

import pytest

from tests.utils.asserting_utils import run_and_log_to_file
from tests.utils.config import (
    PackageCompressionJob,
    PackageSearchJob,
    PackageTestConfig,
)
from tests.utils.logging_utils import construct_log_err_msg

logger = logging.getLogger(__name__)

DEFAULT_CMD_TIMEOUT_SECONDS = 120.0


def start_clp_package(
    request: pytest.FixtureRequest, package_test_config: PackageTestConfig
) -> None:
    """
    Starts an instance of the CLP package.

    :param request: Pytest fixture request.
    :param package_test_config:
    :raise: Propagates `run_and_log_to_file`'s errors.
    """
    path_config = package_test_config.path_config
    start_script_path = path_config.start_script_path
    temp_config_file_path = package_test_config.temp_config_file_path

    # fmt: off
    start_cmd = [
        str(start_script_path),
        "--config", str(temp_config_file_path),
    ]
    # fmt: on

    try:
        run_and_log_to_file(request, start_cmd, timeout=DEFAULT_CMD_TIMEOUT_SECONDS)
    except (SubprocessError, OSError):
        mode_name = package_test_config.mode_config.mode_name
        err_msg = f"The '{mode_name}' package failed to start."
        logger.error(construct_log_err_msg(err_msg))
        pytest.fail(err_msg)


def stop_clp_package(
    request: pytest.FixtureRequest, package_test_config: PackageTestConfig
) -> None:
    """
    Stops the running instance of the CLP package.

    :param request: Pytest fixture request.
    :param package_test_config:
    :raise: Propagates `run_and_log_to_file`'s errors.
    """
    path_config = package_test_config.path_config
    stop_script_path = path_config.stop_script_path
    temp_config_file_path = package_test_config.temp_config_file_path

    # fmt: off
    stop_cmd = [
        str(stop_script_path),
        "--config", str(temp_config_file_path),
    ]
    # fmt: on

    try:
        run_and_log_to_file(request, stop_cmd, timeout=DEFAULT_CMD_TIMEOUT_SECONDS)
    except (SubprocessError, OSError):
        mode_name = package_test_config.mode_config.mode_name
        err_msg = f"The '{mode_name}' package failed to stop."
        logger.error(construct_log_err_msg(err_msg))
        pytest.fail(err_msg)


def run_package_compression_script(
    request: pytest.FixtureRequest,
    compression_job: PackageCompressionJob,
    package_test_config: PackageTestConfig,
) -> None:
    """
    Constructs and runs a compression command on the CLP package.

    :param request:
    :param compression_job:
    :param package_test_config:
    """
    path_config = package_test_config.path_config
    compress_script_path = path_config.compress_script_path
    temp_config_file_path = package_test_config.temp_config_file_path

    compress_cmd = [
        str(compress_script_path),
        "--config",
        str(temp_config_file_path),
    ]

    if compression_job.options is not None:
        compress_cmd.extend(compression_job.options)

    compress_cmd.append(str(compression_job.path_to_original_dataset))

    # Run compression command for this job and assert that it succeeds.
    logger.info("Compressing the '%s' sample dataset...", compression_job.sample_dataset_name)
    run_and_log_to_file(request, compress_cmd, timeout=DEFAULT_CMD_TIMEOUT_SECONDS)


def run_package_search_script(
    request: pytest.FixtureRequest,
    compression_job: PackageCompressionJob,
    search_job: PackageSearchJob,
    package_test_config: PackageTestConfig,
) -> str:
    """
    Constructs and runs a search command on the CLP package.

    :param request:
    :param compression_job:
    :param search_job:
    :param package_test_config:
    :return: The result of the search command.
    """
    path_config = package_test_config.path_config
    search_script_path = path_config.search_script_path
    temp_config_file_path = package_test_config.temp_config_file_path

    search_cmd = [str(search_script_path), "--config", str(temp_config_file_path)]

    if search_job.options is not None:
        search_cmd.extend(search_job.options)

    search_cmd.append(search_job.query)

    # Run search command for this job and assert that it succeeds.
    logger.info(
        "Performing '%s' search on the '%s' sample dataset...",
        search_job.search_name,
        compression_job.sample_dataset_name,
    )
    result = run_and_log_to_file(request, search_cmd, timeout=DEFAULT_CMD_TIMEOUT_SECONDS)

    return result.stdout.decode()
