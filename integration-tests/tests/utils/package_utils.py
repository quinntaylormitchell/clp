"""Provides utility functions related to the CLP package used across `integration-tests`."""

# TODO: rename this file "sbin_utils.py" or something like that

from typing import Any
import logging
import subprocess
from subprocess import SubprocessError

import pytest

from tests.utils.asserting_utils import run_and_capture, run_and_log_to_file
from tests.utils.config import (
    PackageCompressionJob,
    PackageSearchJob,
    PackageTestConfig,
    NewPackageJob,
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
    result = run_and_capture(search_cmd, timeout=DEFAULT_CMD_TIMEOUT_SECONDS)

    return result.stdout.decode()


def run_package_job(job: NewPackageJob) -> None:
    """
    Constructs and runs the command described by `job`. Stores the completed process in
    `job.completed_proc`.
    """
    job_cmd = [
        job.cmd_name,
        "--config",
        job.config_path_str,
    ]

    if job.cmd_options is not None:
        options_list = _dict_to_list_dfs(job.cmd_options)
        job_cmd.extend(options_list)

    if job.cmd_args is not None:
        job_cmd.extend(job.cmd_args)

    job.completed_proc = subprocess.run(job_cmd, capture_output=True, text=True,)


def _dict_to_list_dfs(data: dict[str, Any]) -> list[str]:
    """Writes a depth-first list of the keys and values in a dictionary."""
    result = []
    for key, value in data.items():
        result.append(str(key))

        if isinstance(value, dict):
            result.extend(_dict_to_list_dfs(value))
        else:
            result.append(str(value))

    return result


def NEW_run_package_archive_manager_script(job: NewPackageJob) -> subprocess.CompletedProcess[str]:
    archive_manager_cmd = [
        job.cmd_name,
        "--config",
        job.config_path_str,
    ]

    # [--dataset DATASET]
    # {find,del} ...
        # {find}
            # [--begin-ts BEGIN_TS]
            # [--end-ts END_TS]
        # {del}
            # [--dry-run]
            # {by-ids,by-filter} ...
                # {by-ids}
                    # ids
                # {by-filter}
                    # [--begin-ts BEGIN_TS]
                    # --end-ts END_TS

    job.completed_proc = subprocess.run(archive_manager_cmd, text=True)


def NEW_run_package_dataset_manager_script() -> subprocess.CompletedProcess[str]:
    dataset_manager_cmd = [
        "/home/quinnmitchell/clp/build/clp-package/sbin/admin-tools/dataset-manager.sh",
        "--config",
        "/home/quinnmitchell/clp/build/integration-tests/temp_config_files",
    ]

    # {list,del} ...
        # {list}
        # {del}
            # [-a]
            # [datasets ...]

    return subprocess.run(dataset_manager_cmd, text=True)


def NEW_run_package_compress_from_s3_script() -> subprocess.CompletedProcess[str]:
    compress_from_s3_cmd = [
        "/home/quinnmitchell/clp/build/clp-package/sbin/compress_from_s3.sh",
        "--config",
        "/home/quinnmitchell/clp/build/integration-tests/temp_config_files",
    ]

    # [--dataset DATASET]
    # [--timestamp-key TIMESTAMP_KEY]
    # [--unstructured]
    # [--no-progress-reporting]
    # {s3-object,s3-key-prefix}

    return subprocess.run(compress_from_s3_cmd, text=True)


def NEW_run_package_compress_script() -> subprocess.CompletedProcess[str]:
    compress_cmd = [
        "/home/quinnmitchell/clp/build/clp-package/sbin/compress.sh",
        "--config",
        "/home/quinnmitchell/clp/build/integration-tests/temp_config_files",
    ]

    # [--dataset DATASET]
    # [--timestamp-key TIMESTAMP_KEY]
    # [--unstructured]
    # [--no-progress-reporting]
    # [-f PATH_LIST]
    # [PATH ...]

    return subprocess.run(compress_cmd, text=True)


def NEW_run_package_decompress_script() -> subprocess.CompletedProcess[str]:
    decompress_cmd = [
        "/home/quinnmitchell/clp/build/clp-package/sbin/decompress.sh",
        "--config",
        "/home/quinnmitchell/clp/build/integration-tests/temp_config_files",
    ]

    # {x,i,j}

    return subprocess.run(decompress_cmd, text=True)


def NEW_run_package_search_script() -> subprocess.CompletedProcess[str]:
    search_cmd = [
        "/home/quinnmitchell/clp/build/clp-package/sbin/search.sh",
        "--config",
        "/home/quinnmitchell/clp/build/integration-tests/temp_config_files",
    ]

    # [--dataset DATASET]
    # [--begin-time BEGIN_TIME]
    # [--end-time END_TIME]
    # [--ignore-case]
    # [--file-path FILE_PATH]
    # [--count]
    # [--count-by-time COUNT_BY_TIME]
    # [--raw]

    return subprocess.run(search_cmd, text=True)


def NEW_run_package_start_script() -> subprocess.CompletedProcess[str]:
    start_cmd = [
        "/home/quinnmitchell/clp/build/clp-package/sbin/start-clp.sh",
        "--config",
        "/home/quinnmitchell/clp/build/integration-tests/temp_config_files",
    ]
    
    return subprocess.run(start_cmd, text=True)


def NEW_run_package_stop_script() -> subprocess.CompletedProcess[str]:
    start_cmd = [
        "/home/quinnmitchell/clp/build/clp-package/sbin/stop-clp.sh",
        "--config",
        "/home/quinnmitchell/clp/build/integration-tests/temp_config_files",
    ]
    
    return subprocess.run(start_cmd, text=True)
