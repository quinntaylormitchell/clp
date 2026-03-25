"""Docstring."""

import logging
import re
from typing import Any

import pytest

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.parsers import (
    ClpPackageArchiveManagerDelSubcommand,
    ClpPackageArchiveManagerType,
    construct_archive_manager_arg_dict,
    construct_archive_manager_cmd,
)
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.logging_utils import format_action_failure_msg
from tests.utils.subprocess_utils import execute_external_action

logger = logging.getLogger(__name__)


def clear_package_archives_clp_text(clp_package: ClpPackage) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")

    # Find all.
    find_archive_manager_action = archive_manager_clp_text(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
    )
    find_archive_manager_action_verified, failure_message = (
        verify_archive_manager_find_action_clp_text(find_archive_manager_action, clp_package)
    )
    if not find_archive_manager_action_verified:
        pytest.fail(
            f"When clearing package archives, the call to archive-manager 'find' could not be"
            f" verified: '{failure_message}' Subprocess log:"
            f" '{find_archive_manager_action.log_file_path}'"
        )

    # Delete.
    archive_ids_to_delete = _extract_archive_ids_from_find_output(find_archive_manager_action)
    if len(archive_ids_to_delete) > 0:
        del_by_ids_action = archive_manager_clp_text(
            clp_package=clp_package,
            archive_manager_type=ClpPackageArchiveManagerType.DEL_BY_IDS,
            ids_to_del=archive_ids_to_delete,
        )
        archive_manager_action_verified, failure_message = (
            verify_archive_manager_del_action_clp_text(del_by_ids_action, clp_package)
        )
        if not archive_manager_action_verified:
            pytest.fail(
                f"When clearing package archives, the call to archive-manager 'del' could not be"
                f" verified: '{failure_message}' Subprocess log:"
                f" '{del_by_ids_action.log_file_path}'"
            )


def archive_manager_clp_text(  # noqa: PLR0913
    clp_package: ClpPackage,
    archive_manager_type: ClpPackageArchiveManagerType,
    dataset: IntegrationTestDataset | None = None,
    begin_ts: int | None = None,
    end_ts: int | None = None,
    ids_to_del: list[str] | None = None,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = f"Performing '{archive_manager_type.name}' operation with archive-manager."
    logger.info(log_msg)

    arg_dict: dict[str, Any] = construct_archive_manager_arg_dict(
        clp_package,
        archive_manager_type,
        dataset,
        begin_ts,
        end_ts,
        ids_to_del,
    )
    archive_manager_action = ClpPackageExternalAction(
        cmd=construct_archive_manager_cmd(arg_dict), arg_dict=arg_dict
    )
    execute_external_action(archive_manager_action)

    return archive_manager_action


def verify_archive_manager_find_action_clp_text(
    archive_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying archive-manager 'find'.")
    if archive_manager_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The 'archive-manager.sh find' subprocess returned a non-zero exit code.",
            archive_manager_action,
        )

    arg_dict = archive_manager_action.arg_dict
    begin_ts: int = arg_dict.get("begin_ts", 0)
    end_ts: int | None = arg_dict.get("end_ts")

    current_archive_id_list: list[str] = []

    # Find archives before begin_ts.
    if begin_ts > 0:
        chunk1_action = archive_manager_clp_text(
            clp_package=clp_package,
            archive_manager_type=ClpPackageArchiveManagerType.FIND,
            begin_ts=0,
            end_ts=begin_ts,
        )
        if chunk1_action.completed_proc.returncode != 0:
            pytest.fail(
                "During archive-manager 'find' verification, supporting call to archive-manager"
                " 'find' returned a non-zero exit code. Subprocess log:"
                f" '{chunk1_action.log_file_path}'"
            )
        current_archive_id_list.extend(_extract_archive_ids_from_find_output(chunk1_action))

    # Add the archives from the original command.
    current_archive_id_list.extend(_extract_archive_ids_from_find_output(archive_manager_action))

    # Find archives after end_ts.
    if end_ts is not None:
        chunk3_action = archive_manager_clp_text(
            clp_package=clp_package,
            archive_manager_type=ClpPackageArchiveManagerType.FIND,
            begin_ts=end_ts,
        )
        if chunk3_action.completed_proc.returncode != 0:
            pytest.fail(
                "During archive-manager 'find' verification, supporting call to archive-manager"
                " 'find' returned a non-zero exit code. Subprocess log:"
                f" '{chunk3_action.log_file_path}'"
            )
        current_archive_id_list.extend(_extract_archive_ids_from_find_output(chunk3_action))

    # Find all.
    find_all_action = archive_manager_clp_text(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
    )
    if find_all_action.completed_proc.returncode != 0:
        pytest.fail(
            "During archive-manager 'find' verification, supporting call to archive-manager 'find'"
            f" returned a non-zero exit code. Subprocess log: '{find_all_action.log_file_path}'"
        )
    directories_in_package_archives = _extract_archive_ids_from_find_output(find_all_action)

    # Compare.
    if current_archive_id_list != directories_in_package_archives:
        return format_action_failure_msg(
            "Archive-manager 'find' verification failure: mismatch between current archive ID list"
            f" '{current_archive_id_list}' and list of directories present in var/archives"
            f" directory '{directories_in_package_archives}'",
            archive_manager_action,
            find_all_action,
        )

    return True, ""


def verify_archive_manager_del_action_clp_text(
    archive_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying archive-manager 'del'.")
    if archive_manager_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The 'archive-manager.sh del' subprocess returned a non-zero exit code.",
            archive_manager_action,
        )

    arg_dict = archive_manager_action.arg_dict
    match arg_dict["del_subcommand"]:
        case ClpPackageArchiveManagerDelSubcommand.BY_IDS_COMMAND:
            find_all_action = archive_manager_clp_text(
                clp_package=clp_package,
                archive_manager_type=ClpPackageArchiveManagerType.FIND,
            )
            verified, failure_message = verify_archive_manager_find_action_clp_text(
                find_all_action, clp_package
            )
            if not verified:
                pytest.fail(
                    "During archive-manager 'del' verification, supporting call to archive-manager"
                    f" 'find' could not be verified: '{failure_message}' Subprocess log:"
                    f" '{find_all_action.log_file_path}'"
                )

            current_ids = _extract_archive_ids_from_find_output(find_all_action)
            if any(item in current_ids for item in arg_dict["ids"]):
                return format_action_failure_msg(
                    "Archive-manager 'del by-ids' verification failure: Some archives that were"
                    " specified for deletion are still present in the metadata database.",
                    archive_manager_action,
                    find_all_action,
                )
        case ClpPackageArchiveManagerDelSubcommand.BY_FILTER_COMMAND:
            begin_ts = arg_dict.get("begin_ts")
            end_ts = arg_dict.get("end_ts")
            find_action = archive_manager_clp_text(
                clp_package=clp_package,
                archive_manager_type=ClpPackageArchiveManagerType.FIND,
                begin_ts=begin_ts,
                end_ts=end_ts,
            )
            verified, failure_message = verify_archive_manager_find_action_clp_text(
                find_action, clp_package
            )
            if not verified:
                pytest.fail(
                    "During archive-manager 'del' verification, supporting call to archive-manager"
                    f" 'find' could not be verified. Subprocess log: '{find_action.log_file_path}'"
                )

            current_ids = _extract_archive_ids_from_find_output(find_action)
            if len(current_ids) > 0:
                return format_action_failure_msg(
                    "Archive-manager 'del by-filter' verification failure: Some archives that"
                    " should have been deleted were not deleted.",
                    archive_manager_action,
                    find_action,
                )
        case _:
            pytest.fail(
                "'archive-manager.sh del' needs a subcommand ('by-ids' or 'by-filter') but received"
                " neither."
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


def _get_action_output(action: ClpPackageExternalAction) -> str:
    """Return the combined stdout + stderr from a completed action."""
    return action.completed_proc.stdout + action.completed_proc.stderr
