"""Utilities that raise pytest assertions on failure."""

import logging
import shlex
import subprocess
from pathlib import Path
from typing import Any

import pytest
from clp_package_utils.general import EXTRACT_FILE_CMD
from clp_py_utils.clp_config import ClpConfig
from pydantic import ValidationError

from tests.utils.clp_mode_utils import compare_mode_signatures
from tests.utils.config import (
    PackageCompressionJob,
    PackageInstance,
    PackageSearchJob,
    PackageTestConfig,
)
from tests.utils.docker_utils import list_running_services_in_compose_project
from tests.utils.logging_utils import construct_log_err_msg
from tests.utils.utils import (
    clear_directory,
    get_binary_path,
    is_dir_tree_content_equal,
    load_yaml_to_dict,
)

logger = logging.getLogger(__name__)


def run_and_log_to_file(
    request: pytest.FixtureRequest, cmd: list[str], **kwargs: Any
) -> subprocess.CompletedProcess[bytes]:
    """
    Runs a command with subprocess.

    :param request: Pytest fixture request.
    :param cmd: Command and arguments to execute.
    :param kwargs: Additional keyword arguments passed through to the subprocess.
    :raise: Propagates `subprocess.run`'s errors.
    :return: The completed process.
    """
    log_file_path = Path(request.config.getini("log_file_path"))
    with log_file_path.open("ab") as log_file:
        log_debug_msg = f"Now running command: {cmd}"
        logger.debug(log_debug_msg)
        result = subprocess.run(cmd, capture_output=True, check=False, **kwargs)
        log_file.write(result.stdout)
        log_file.write(result.stderr)

    result.check_returncode()
    return result


def validate_package_instance(package_instance: PackageInstance) -> None:
    """
    Validate that the given package instance is running by performing two checks: validate that the
    instance has exactly the set of running components that it should have, and validate that the
    instance is running in the correct mode.

    :param package_instance:
    """
    mode_name = package_instance.package_test_config.mode_config.mode_name
    logger.info("Validating that the '%s' package is running correctly...", mode_name)

    # Ensure that all package components are running.
    _validate_package_running(package_instance)

    # Ensure that the package is running in the correct mode.
    _validate_running_mode_correct(package_instance)


def _validate_package_running(package_instance: PackageInstance) -> None:
    """
    Validate that the given package instance is running by checking that the set of services running
    in the Compose project exactly matches the list of required components.

    :param package_instance:
    :raise pytest.fail: if the sets of running services and required components do not match.
    """
    mode_name = package_instance.package_test_config.mode_config.mode_name
    logger.debug("Validating that all components of the '%s' package are running...", mode_name)

    # Get list of services currently running in the Compose project.
    instance_id = package_instance.clp_instance_id
    project_name = f"clp-package-{instance_id}"
    running_services = set(list_running_services_in_compose_project(project_name))

    # Compare with list of required components.
    required_components = set(package_instance.package_test_config.mode_config.component_list)
    if required_components == running_services:
        return

    # Construct error message.
    err_msg = f"Component validation failed for the {mode_name} package test."

    missing_components = required_components - running_services
    if missing_components:
        err_msg += f" Missing components: {missing_components}."

    unexpected_components = running_services - required_components
    if unexpected_components:
        err_msg += f" Unexpected services: {unexpected_components}."

    logger.error(construct_log_err_msg(err_msg))
    pytest.fail(err_msg)


def _validate_running_mode_correct(package_instance: PackageInstance) -> None:
    """
    Validate that the mode described in the shared config of the instance matches the intended mode
    defined by the instance configuration. Calls pytest.fail if the shared config fails validation
    or if the running mode does not match the intended mode.

    :param package_instance:
    :raise: Propagates `load_yaml_to_dict`'s errors.
    :raise pytest.fail: if the ClpConfig object cannot be validated.
    :raise pytest.fail: if the running ClpConfig does not match the intended ClpConfig.
    """
    mode_name = package_instance.package_test_config.mode_config.mode_name
    logger.debug(
        "Validating that the '%s' package is running in the correct configuration...", mode_name
    )

    shared_config_dict = load_yaml_to_dict(package_instance.shared_config_file_path)
    try:
        running_config = ClpConfig.model_validate(shared_config_dict)
    except ValidationError as err:
        err_msg = f"The shared config file could not be validated: {err}"
        logger.error(construct_log_err_msg(err_msg))
        pytest.fail(err_msg)

    intended_config = package_instance.package_test_config.mode_config.clp_config

    if not compare_mode_signatures(intended_config, running_config):
        err_msg = f"Mode validation failed for the {mode_name} package test."
        logger.error(construct_log_err_msg(err_msg))
        pytest.fail(err_msg)


def verify_package_compression(
    request: pytest.FixtureRequest,
    compression_job: PackageCompressionJob,
    package_test_config: PackageTestConfig,
) -> None:
    """
    Verify that compression has been executed correctly by decompressing the contents of
    `clp-package/var/data/archives` and comparing the decompressed logs to the originals stored at
    `compression_job.path_to_original_dataset`.

    :param compression_job:
    :param package_test_config:
    """
    mode = package_test_config.mode_config.mode_name
    logger.info(
        "Verifying that the '%s' sample dataset was compressed correctly...",
        compression_job.sample_dataset_name,
    )

    if mode == "clp-json":
        # TODO: Waiting for PR 1299 to be merged.
        assert True
    elif mode == "clp-text":
        # Decompress the contents of `clp-package/var/data/archives`.
        path_config = package_test_config.path_config
        decompress_script_path = path_config.decompress_script_path
        decompression_dir = path_config.package_decompression_dir
        temp_config_file_path = package_test_config.temp_config_file_path

        clear_directory(decompression_dir)

        decompress_cmd = [
            str(decompress_script_path),
            "--config",
            str(temp_config_file_path),
            EXTRACT_FILE_CMD,
            "--extraction-dir",
            str(decompression_dir),
        ]

        # Run decompression command and assert that it succeeds.
        run_and_log_to_file(request, decompress_cmd)

        # Verify content equality.
        path_to_original_dataset = compression_job.path_to_original_dataset
        output_path = decompression_dir / path_to_original_dataset.relative_to(
            path_to_original_dataset.anchor
        )

        try:
            if not is_dir_tree_content_equal(path_to_original_dataset, output_path):
                err_msg = (
                    f"Mismatch between clp input {path_to_original_dataset} and output"
                    f" {output_path}."
                )
                logger.error(construct_log_err_msg(err_msg))
                pytest.fail(err_msg)
        finally:
            clear_directory(decompression_dir)


def verify_package_search(
    request: pytest.FixtureRequest,
    search_job: PackageSearchJob,
    search_result: str,
    grep_cmd_options: list[str],
    grep_cmd_pipe: str | None,
) -> None:
    """Docstring for verify_package_search"""
    logger.info(
        "Verifying that the '%s' search was performed correctly on the '%s' sample dataset...",
        search_job.search_name,
        search_job.compression_job.sample_dataset_name,
    )

    # Construct and run grep command.
    grep_cmd = [
        get_binary_path("grep"),
        *grep_cmd_options,
        search_job.query,
    ]
    if search_job.subpath_to_search is not None:
        path_for_grep = (
            search_job.compression_job.path_to_original_dataset / search_job.subpath_to_search
        )
    else:
        path_for_grep = search_job.compression_job.path_to_original_dataset
    grep_cmd.append(str(path_for_grep))
    result = run_and_log_to_file(request, grep_cmd)

    if grep_cmd_pipe is not None:
        result = run_and_log_to_file(request, shlex.split(grep_cmd_pipe), input=result.stdout)

    # Compare grep result with search result.
    grep_result_str = result.stdout.decode()
    if grep_result_str != search_result:
        err_msg = (
            f"Mismatch between search result '{search_result}' and grep result '{grep_result_str}'"
        )
        logger.error(construct_log_err_msg(err_msg))
        pytest.fail(err_msg)
