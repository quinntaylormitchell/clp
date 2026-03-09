"""Docstring."""

import pytest

from tests.clp_package_tests.clp_package_utils.classes import ClpPackageExternalAction
from tests.clp_package_tests.clp_package_utils.parsers import (
    get_archive_manager_parser,
    get_compress_from_s3_parser,
    get_compress_parser,
    get_dataset_manager_parser,
    get_decompress_parser,
    get_search_parser,
    get_start_clp_parser,
    get_stop_clp_parser,
)
from tests.utils.subprocess_utils import execute_external_action


def run_archive_manager_cmd(cmd: list[str]) -> ClpPackageExternalAction:
    """Docstring."""
    archive_manager_action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_archive_manager_parser(),
    )
    execute_external_action(archive_manager_action)
    return archive_manager_action


def run_compress_from_s3_cmd(cmd: list[str]) -> ClpPackageExternalAction:
    """Docstring."""
    compress_from_s3_action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_compress_from_s3_parser(),
    )
    execute_external_action(compress_from_s3_action)
    return compress_from_s3_action


def run_compress_cmd(cmd: list[str]) -> ClpPackageExternalAction:
    """Docstring."""
    compress_action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_compress_parser(),
    )
    execute_external_action(compress_action)
    return compress_action


def run_dataset_manager_cmd(cmd: list[str]) -> ClpPackageExternalAction:
    """Docstring."""
    dataset_manager_action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_dataset_manager_parser(),
    )
    execute_external_action(dataset_manager_action)
    return dataset_manager_action


def run_decompress_cmd(cmd: list[str]) -> ClpPackageExternalAction:
    """Docstring."""
    decompress_action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_decompress_parser(),
    )
    execute_external_action(decompress_action)
    return decompress_action


def run_search_cmd(cmd: list[str]) -> ClpPackageExternalAction:
    """Docstring."""
    search_action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_search_parser(),
    )
    if not search_action.parsed_args.raw:
        pytest.fail("The '--raw' flag has to be specified in search commands for the test to work.")
    execute_external_action(search_action)
    return search_action


def run_start_clp_cmd(cmd: list[str]) -> ClpPackageExternalAction:
    """Docstring."""
    start_clp_action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_start_clp_parser(),
    )
    execute_external_action(start_clp_action)
    return start_clp_action


def run_stop_clp_cmd(cmd: list[str]) -> ClpPackageExternalAction:
    """Docstring."""
    stop_clp_action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_stop_clp_parser(),
    )
    execute_external_action(stop_clp_action)
    return stop_clp_action
