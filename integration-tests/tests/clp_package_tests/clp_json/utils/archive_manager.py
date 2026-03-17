"""archive_manager_json.py"""

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
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.logging_utils import format_action_failure_msg
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import get_rand_subdirectory_name

logger = logging.getLogger(__name__)


class DelSubcommand(StrEnum):
    """Docstring."""

    BY_IDS = "by-ids"
    BY_FILTER = "by-filter"


def archive_manager_find_clp_json(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
    begin_ts: int | None = None,
    end_ts: int | None = None,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info("Performing 'find' operation with archive-manager.")

    cmd = _get_base_archive_manager_cmd(clp_package, dataset)
    cmd.append("find")
    if begin_ts is not None:
        cmd.append("--begin-ts")
        cmd.append(str(begin_ts))
    if end_ts is not None:
        cmd.append("--end-ts")
        cmd.append(str(end_ts))

    return _run_archive_manager_action(cmd)


def archive_manager_del_by_ids_clp_json(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
    ids_to_del: list[str] | None = None,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info("Performing 'del by-ids' operation with archive-manager.")

    cmd = _get_base_archive_manager_cmd(clp_package, dataset)
    cmd.append("del")
    cmd.append(DelSubcommand.BY_IDS)

    if ids_to_del is not None:
        cmd.extend(ids_to_del)
    else:
        sample_id = get_rand_subdirectory_name(clp_package.path_config.package_archives_path)
        cmd.append(sample_id)

    return _run_archive_manager_action(cmd)


def archive_manager_del_by_filter_clp_json(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
    begin_ts: int | None,
    end_ts: int,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info("Performing 'del by-filter' operation with archive-manager.")

    cmd = _get_base_archive_manager_cmd(clp_package, dataset)
    cmd.append("del")
    cmd.append(DelSubcommand.BY_FILTER)

    if begin_ts is not None:
        cmd.append("--begin-ts")
        cmd.append(str(begin_ts))

    cmd.append("--end-ts")
    cmd.append(str(end_ts))

    return _run_archive_manager_action(cmd)


def verify_archive_manager_find_action_clp_json(
    archive_manager_action: ClpPackageExternalAction,
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying archive-manager 'find'.")
    if archive_manager_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The 'archive-manager.sh find' subprocess returned a non-zero exit code.",
            archive_manager_action,
        )

    parsed_args = archive_manager_action.parsed_args
    begin_ts: int = parsed_args.begin_ts
    end_ts: int | None = parsed_args.end_ts

    current_archive_id_list: list[str] = []

    # Find archives before begin_ts.
    if begin_ts > 0:
        chunk1_action = archive_manager_find_clp_json(
            clp_package=clp_package,
            dataset=dataset,
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
        chunk3_action = archive_manager_find_clp_json(
            clp_package=clp_package,
            dataset=dataset,
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
    find_all_action = archive_manager_find_clp_json(
        clp_package=clp_package,
        dataset=dataset,
    )
    if find_all_action.completed_proc.returncode != 0:
        pytest.fail(
            "During archive-manager 'find' verification, supporting call to archive-manager 'find'"
            f" returned a non-zero exit code. Subprocess log: '{find_all_action.log_file_path}'"
        )
    directories_in_dataset_archive_dir = _extract_archive_ids_from_find_output(find_all_action)

    # Compare.
    if current_archive_id_list != directories_in_dataset_archive_dir:
        return format_action_failure_msg(
            "Archive-manager 'find' verification failure: mismatch between current archive ID list"
            f" '{current_archive_id_list}' and list of directories present in var/archives dataset"
            f" directory '{directories_in_dataset_archive_dir}'",
            archive_manager_action,
            find_all_action,
        )

    return True, ""


def verify_archive_manager_del_action_clp_json(
    archive_manager_action: ClpPackageExternalAction,
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying archive-manager 'del'.")
    if archive_manager_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The 'archive-manager.sh del' subprocess returned a non-zero exit code.",
            archive_manager_action,
        )

    parsed_args = archive_manager_action.parsed_args
    match parsed_args.del_subcommand:
        case DelSubcommand.BY_IDS:
            find_all_action = archive_manager_find_clp_json(
                clp_package=clp_package,
                dataset=dataset,
            )
            verified, failure_message = verify_archive_manager_find_action_clp_json(
                find_all_action, clp_package, dataset
            )
            if not verified:
                pytest.fail(
                    "During archive-manager 'del' verification, supporting call to archive-manager"
                    f" 'find' could not be verified: '{failure_message}' Subprocess log:"
                    f" '{find_all_action.log_file_path}'"
                )

            current_ids = _extract_archive_ids_from_find_output(find_all_action)
            if any(item in current_ids for item in parsed_args.ids):
                return format_action_failure_msg(
                    "Archive-manager 'del by-ids' verification failure: Some archives that were"
                    " specified for deletion are still present in the metadata database.",
                    archive_manager_action,
                    find_all_action,
                )
        case DelSubcommand.BY_FILTER:
            begin_ts = parsed_args.begin_ts
            end_ts = parsed_args.end_ts
            find_action = archive_manager_find_clp_json(
                clp_package=clp_package,
                dataset=dataset,
                begin_ts=begin_ts,
                end_ts=end_ts,
            )
            verified, failure_message = verify_archive_manager_find_action_clp_json(
                find_action, clp_package, dataset
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


def _get_base_archive_manager_cmd(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
) -> list[str]:
    """Build the common prefix shared by all archive-manager commands."""
    return [
        str(clp_package.path_config.archive_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        dataset.metadata_dict["dataset_name"],
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
