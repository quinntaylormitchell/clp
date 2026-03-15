"""Docstring."""

import logging
import re
from pathlib import Path

import pytest

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
    ClpPackageTestPathConfig,
)
from tests.clp_package_tests.utils.parsers import (
    get_archive_manager_parser,
)
from tests.utils.subprocess_utils import execute_external_action

logger = logging.getLogger(__name__)


def clear_package_archives_clp_text(clp_package: ClpPackage) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")
    path_config = clp_package.path_config

    # Find all.
    find_archive_manager_action = archive_manager_find_clp_text(
        clp_package_test_path_config=path_config,
        clp_package=clp_package,
    )
    find_archive_manager_action_verified, failure_message = (
        _verify_archive_manager_find_action_clp_text(find_archive_manager_action, clp_package)
    )
    assert find_archive_manager_action_verified, failure_message

    # Delete.
    archive_ids_to_delete = _extract_archive_ids_from_find_output(find_archive_manager_action)
    if len(archive_ids_to_delete) > 0:
        del_by_ids_action = archive_manager_del_by_ids_clp_text(
            clp_package_test_path_config=clp_package.path_config,
            clp_package=clp_package,
            ids_to_del=archive_ids_to_delete,
        )
        archive_manager_action_verified, failure_message = (
            _verify_archive_manager_del_action_clp_text(del_by_ids_action, clp_package)
        )
        assert archive_manager_action_verified, failure_message


def archive_manager_find_clp_text(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    begin_ts: int | None = None,
    end_ts: int | None = None,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = "Performing 'find' operation with archive manager."
    logger.info(log_msg)

    archive_manager_cmd: list[str] = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "find",
    ]
    if begin_ts is not None:
        archive_manager_cmd.append("--begin-ts")
        archive_manager_cmd.append(str(begin_ts))
    if end_ts is not None:
        archive_manager_cmd.append("--end-ts")
        archive_manager_cmd.append(str(end_ts))

    archive_manager_action = ClpPackageExternalAction(
        cmd=archive_manager_cmd,
        args_parser=get_archive_manager_parser(),
    )
    execute_external_action(archive_manager_action)

    return archive_manager_action


def archive_manager_del_by_ids_clp_text(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    ids_to_del: list[str] | None = None,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = "Performing 'del by-ids' operation with archive manager."
    logger.info(log_msg)

    archive_manager_cmd: list[str] = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "del",
        "by-ids",
    ]

    if ids_to_del is not None:
        archive_manager_cmd.extend(ids_to_del)
    else:
        sample_id = _get_rand_subdirectory_name(clp_package_test_path_config.package_archives_path)
        archive_manager_cmd.append(sample_id)

    archive_manager_action = ClpPackageExternalAction(
        cmd=archive_manager_cmd,
        args_parser=get_archive_manager_parser(),
    )
    execute_external_action(archive_manager_action)

    return archive_manager_action


def archive_manager_del_by_filter_clp_text(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    begin_ts: int | None,
    end_ts: int,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = "Performing 'del by-filter' operation with archive manager."
    logger.info(log_msg)

    archive_manager_cmd: list[str] = [
        str(clp_package_test_path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "del",
        "by-filter",
    ]

    if begin_ts is not None:
        archive_manager_cmd.append("--begin-ts")
        archive_manager_cmd.append(str(begin_ts))

    if end_ts is None:
        pytest.fail(
            "You must use the '--end-ts' flag when performing 'del by-filter' with archive manager."
        )

    archive_manager_cmd.append("--end-ts")
    archive_manager_cmd.append(str(end_ts))

    archive_manager_action = ClpPackageExternalAction(
        cmd=archive_manager_cmd,
        args_parser=get_archive_manager_parser(),
    )
    execute_external_action(archive_manager_action)

    return archive_manager_action


def _get_rand_subdirectory_name(path_to_parent: Path) -> str:
    for item in path_to_parent.iterdir():
        if item.is_dir():
            return item.name

    return ""


def _verify_archive_manager_find_action_clp_text(
    archive_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    logger.info("Verifying archive-manager find action.")
    if archive_manager_action.completed_proc.returncode != 0:
        return False, "The archive-manager.sh subprocess returned a non-zero exit code."

    parsed_args = archive_manager_action.parsed_args
    begin_ts: int = parsed_args.begin_ts
    end_ts: int | None = parsed_args.end_ts
    path_config = clp_package.path_config

    current_archive_id_list: list[str] = []
    # Chunk 1.
    if begin_ts > 0:
        verify_archive_manager_cmd = [
            str(path_config.archive_manager_path),
            "--config",
            str(clp_package.temp_config_file_path),
            "find",
            "--begin-ts",
            "0",
            "--end-ts",
            str(begin_ts),
        ]
        verify_archive_manager_action = ClpPackageExternalAction(
            cmd=verify_archive_manager_cmd,
            args_parser=get_archive_manager_parser(),
        )
        execute_external_action(verify_archive_manager_action)
        assert verify_archive_manager_action.completed_proc.returncode == 0
        current_archive_id_list.extend(
            _extract_archive_ids_from_find_output(verify_archive_manager_action)
        )

    # Chunk 2.
    # We already have the info from this from the original command output.
    current_archive_id_list.extend(_extract_archive_ids_from_find_output(archive_manager_action))

    # Chunk 3.
    if end_ts is not None:
        verify_archive_manager_cmd = [
            str(path_config.archive_manager_path),
            "--config",
            str(clp_package.temp_config_file_path),
            "find",
            "--begin-ts",
            str(end_ts),
        ]
        verify_archive_manager_action = ClpPackageExternalAction(
            cmd=verify_archive_manager_cmd,
            args_parser=get_archive_manager_parser(),
        )
        execute_external_action(verify_archive_manager_action)
        assert verify_archive_manager_action.completed_proc.returncode == 0
        current_archive_id_list.extend(
            _extract_archive_ids_from_find_output(verify_archive_manager_action)
        )

    directories_in_package_archives = _get_names_of_directories_in_package_archives(clp_package)
    if current_archive_id_list != directories_in_package_archives:
        fail_msg = (
            f"Mismatch between output archive ID list '{current_archive_id_list}' and directories"
            f" in var/archives '{directories_in_package_archives}'"
        )
        return False, fail_msg

    return True, ""


def _verify_archive_manager_del_action_clp_text(
    archive_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    logger.info("Verifying archive-manager del action.")
    if archive_manager_action.completed_proc.returncode != 0:
        return False, "The archive-manager.sh subprocess returned a non-zero exit code."

    parsed_args = archive_manager_action.parsed_args
    del_subcommand = parsed_args.del_subcommand
    match del_subcommand:
        case "by-ids":
            # Run a "find all" command.
            verify_archive_manager_cmd = [
                str(clp_package.path_config.archive_manager_path),
                "--config",
                str(clp_package.temp_config_file_path),
                "find",
            ]
            verify_archive_manager_action = ClpPackageExternalAction(
                cmd=verify_archive_manager_cmd,
                args_parser=get_archive_manager_parser(),
            )
            execute_external_action(verify_archive_manager_action)
            verify_archive_manager_action_verified, failure_message = (
                _verify_archive_manager_find_action_clp_text(
                    verify_archive_manager_action, clp_package
                )
            )
            assert verify_archive_manager_action_verified, failure_message

            # Get ids from "find all" and compare.
            current_archive_ids_list = _extract_archive_ids_from_find_output(
                verify_archive_manager_action
            )
            if any(item in current_archive_ids_list for item in parsed_args.ids):
                fail_msg = (
                    "archive-manager del by-ids failed: Some archives that were specified for"
                    " deletion are still present in the metadata database."
                )
                return False, fail_msg
        case "by-filter":
            # Run a "find" command with begin_ts and end_ts.
            verify_archive_manager_cmd = [
                str(clp_package.path_config.archive_manager_path),
                "--config",
                str(clp_package.temp_config_file_path),
                "find",
                "--begin-ts",
                str(parsed_args.begin_ts),
            ]
            if parsed_args.end_ts is not None:
                verify_archive_manager_cmd.append("--end-ts")
                verify_archive_manager_cmd.append(str(parsed_args.end_ts))

            verify_archive_manager_action = ClpPackageExternalAction(
                cmd=verify_archive_manager_cmd,
                args_parser=get_archive_manager_parser(),
            )
            execute_external_action(verify_archive_manager_action)
            verify_archive_manager_action_verified, failure_message = (
                _verify_archive_manager_find_action_clp_text(
                    verify_archive_manager_action, clp_package
                )
            )
            assert verify_archive_manager_action_verified, failure_message

            # Get ids from "find" command with begin_ts and end_ts and compare
            current_archive_ids_list = _extract_archive_ids_from_find_output(
                verify_archive_manager_action
            )
            if len(current_archive_ids_list) > 0:
                fail_msg = (
                    "archive-manager del by-filter failed: Some archives that should have been"
                    " deleted were not deleted."
                )
                return False, fail_msg

        case _:
            fail_msg = (
                "archive-manager del failed: del needs a subcommand ('by-ids' or 'by-filter')"
            )
            return False, fail_msg

    return True, ""


def _extract_archive_ids_from_find_output(
    archive_manager_action: ClpPackageExternalAction,
) -> list[str]:
    output_archive_id_list: list[str] = []
    output = (
        archive_manager_action.completed_proc.stdout + archive_manager_action.completed_proc.stderr
    )
    output_lines = output.splitlines()
    num_archive_ids = 0
    for line in output_lines:
        match = re.search(r"Found (\d+) archives within the specified time range", line)
        if match:
            num_archive_ids = int(match.group(1))
            output_lines.remove(line)
            break

    if num_archive_ids == 0:
        return output_archive_id_list

    for line in output_lines:
        match = re.search(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            line,
        )
        if match:
            output_archive_id_list.append(match.group(0))

    return sorted(output_archive_id_list)


def _get_names_of_directories_in_package_archives(clp_package: ClpPackage) -> list[str]:
    directories_in_package_archives = []
    archives_dir = clp_package.path_config.package_archives_path
    for item in archives_dir.iterdir():
        if item.is_dir():
            directories_in_package_archives.append(item.name)
    return sorted(directories_in_package_archives)
