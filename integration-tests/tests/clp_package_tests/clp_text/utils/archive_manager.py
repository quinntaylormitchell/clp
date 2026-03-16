"""Docstring."""

import logging
import re

import pytest
from strenum import StrEnum

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.parsers import (
    get_archive_manager_parser,
)
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import get_rand_subdirectory_name

logger = logging.getLogger(__name__)


class DelSubcommand(StrEnum):
    """Docstring."""

    BY_IDS = "by-ids"
    BY_FILTER = "by-filter"


def clear_package_archives_clp_text(clp_package: ClpPackage) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")

    # Find all.
    find_archive_manager_action = archive_manager_find_clp_text(clp_package)
    find_archive_manager_action_verified, failure_message = (
        verify_archive_manager_find_action_clp_text(find_archive_manager_action, clp_package)
    )
    assert find_archive_manager_action_verified, failure_message

    # Delete.
    archive_ids_to_delete = _extract_archive_ids_from_find_output(find_archive_manager_action)
    if len(archive_ids_to_delete) > 0:
        del_by_ids_action = archive_manager_del_by_ids_clp_text(clp_package, archive_ids_to_delete)
        archive_manager_action_verified, failure_message = (
            verify_archive_manager_del_action_clp_text(del_by_ids_action, clp_package)
        )
        assert archive_manager_action_verified, failure_message


def archive_manager_find_clp_text(
    clp_package: ClpPackage,
    begin_ts: int | None = None,
    end_ts: int | None = None,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info("Performing 'find' operation with archive manager.")

    cmd = _get_base_archive_manager_cmd(clp_package)
    cmd.append("find")
    if begin_ts is not None:
        cmd.append("--begin-ts")
        cmd.append(str(begin_ts))
    if end_ts is not None:
        cmd.append("--end-ts")
        cmd.append(str(end_ts))

    return _run_archive_manager_action(cmd)


def archive_manager_del_by_ids_clp_text(
    clp_package: ClpPackage,
    ids_to_del: list[str] | None = None,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info("Performing 'del by-ids' operation with archive manager.")

    cmd = _get_base_archive_manager_cmd(clp_package)
    cmd.append("del")
    cmd.append(DelSubcommand.BY_IDS)

    if ids_to_del is not None:
        cmd.extend(ids_to_del)
    else:
        sample_id = get_rand_subdirectory_name(clp_package.path_config.package_archives_path)
        cmd.append(sample_id)

    return _run_archive_manager_action(cmd)


def archive_manager_del_by_filter_clp_text(
    clp_package: ClpPackage,
    begin_ts: int | None,
    end_ts: int,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info("Performing 'del by-filter' operation with archive manager.")

    if end_ts is None:
        pytest.fail(
            "You must use the '--end-ts' flag when performing 'del by-filter' with archive manager."
        )

    cmd = _get_base_archive_manager_cmd(clp_package)
    cmd.append("del")
    cmd.append(DelSubcommand.BY_FILTER)

    if begin_ts is not None:
        cmd.append("--begin-ts")
        cmd.append(str(begin_ts))

    cmd.append("--end-ts")
    cmd.append(str(end_ts))

    return _run_archive_manager_action(cmd)


def verify_archive_manager_find_action_clp_text(
    archive_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying archive-manager find action.")
    if archive_manager_action.completed_proc.returncode != 0:
        return False, "The archive-manager.sh find subprocess returned a non-zero exit code."

    parsed_args = archive_manager_action.parsed_args
    begin_ts: int = parsed_args.begin_ts
    end_ts: int | None = parsed_args.end_ts

    current_archive_id_list: list[str] = []

    # Find archives before begin_ts.
    if begin_ts > 0:
        chunk1_action = archive_manager_find_clp_text(
            clp_package=clp_package,
            begin_ts=0,
            end_ts=begin_ts,
        )
        assert chunk1_action.completed_proc.returncode == 0
        current_archive_id_list.extend(_extract_archive_ids_from_find_output(chunk1_action))

    # Add the archives from the original command.
    current_archive_id_list.extend(_extract_archive_ids_from_find_output(archive_manager_action))

    # Find archives after end_ts.
    if end_ts is not None:
        chunk3_action = archive_manager_find_clp_text(
            clp_package=clp_package,
            begin_ts=end_ts,
        )
        assert chunk3_action.completed_proc.returncode == 0
        current_archive_id_list.extend(_extract_archive_ids_from_find_output(chunk3_action))

    # Find all.
    find_all_action = archive_manager_find_clp_text(clp_package)
    assert find_all_action.completed_proc.returncode == 0
    directories_in_package_archives = _extract_archive_ids_from_find_output(find_all_action)

    # Compare.
    if current_archive_id_list != directories_in_package_archives:
        fail_msg = (
            f"Mismatch between output archive ID list '{current_archive_id_list}' and directories"
            f" in var/archives '{directories_in_package_archives}'"
        )
        return False, fail_msg

    return True, ""


def verify_archive_manager_del_action_clp_text(
    archive_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying archive-manager del action.")
    if archive_manager_action.completed_proc.returncode != 0:
        return False, "The archive-manager.sh del subprocess returned a non-zero exit code."

    parsed_args = archive_manager_action.parsed_args
    match parsed_args.del_subcommand:
        case DelSubcommand.BY_IDS:
            # Find all, and then confirm the deleted IDs are gone.
            find_all_action = archive_manager_find_clp_text(clp_package)
            verified, failure_message = verify_archive_manager_find_action_clp_text(
                find_all_action, clp_package
            )
            assert verified, failure_message

            current_ids = _extract_archive_ids_from_find_output(find_all_action)
            if any(item in current_ids for item in parsed_args.ids):
                return False, (
                    "archive-manager del by-ids failed: Some archives that were specified for"
                    " deletion are still present in the metadata database."
                )
        case DelSubcommand.BY_FILTER:
            # Find over the same window as the current command and confirm it's empty.
            begin_ts = parsed_args.begin_ts
            end_ts = parsed_args.end_ts
            find_action = archive_manager_find_clp_text(
                clp_package=clp_package,
                begin_ts=begin_ts,
                end_ts=end_ts,
            )
            verified, failure_message = verify_archive_manager_find_action_clp_text(
                find_action, clp_package
            )
            assert verified, failure_message

            current_ids = _extract_archive_ids_from_find_output(find_action)
            if len(current_ids) > 0:
                return False, (
                    "archive-manager del by-filter failed: Some archives that should have been"
                    " deleted were not deleted."
                )
        case _:
            return False, (
                "archive-manager del failed: del needs a subcommand ('by-ids' or 'by-filter')"
            )

    return True, ""


def _extract_archive_ids_from_find_output(
    archive_manager_action: ClpPackageExternalAction,
) -> list[str]:
    output_archive_id_list: list[str] = []
    output_lines = _get_action_output(archive_manager_action).splitlines()

    num_archive_ids = 0
    filtered_lines = []
    for line in output_lines:
        match = re.search(r"Found (\d+) archives within the specified time range", line)
        if match:
            num_archive_ids = int(match.group(1))
        else:
            filtered_lines.append(line)

    if num_archive_ids == 0:
        return output_archive_id_list

    uuid_pattern = re.compile(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    )
    for line in filtered_lines:
        match = uuid_pattern.search(line)
        if match:
            output_archive_id_list.append(match.group(0))

    return sorted(output_archive_id_list)


def _get_base_archive_manager_cmd(
    clp_package: ClpPackage,
) -> list[str]:
    """Build the common prefix shared by all archive-manager commands."""
    return [
        str(clp_package.path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
    ]


def _run_archive_manager_action(cmd: list[str]) -> ClpPackageExternalAction:
    """Construct and immediately execute a ClpPackageExternalAction."""
    action = ClpPackageExternalAction(
        cmd=cmd,
        args_parser=get_archive_manager_parser(),
    )
    execute_external_action(action)
    return action


def _get_action_output(action: ClpPackageExternalAction) -> str:
    """Return the combined stdout + stderr from a completed action."""
    return action.completed_proc.stdout + action.completed_proc.stderr
