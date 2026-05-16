"""Functions and classes to facilitate CLP package archive-manager."""

import logging
import re
from enum import auto, Enum
from pathlib import Path

import pytest
from strenum import StrEnum

from tests.package_tests.classes import ClpPackage
from tests.utils.classes import ClpAction, ClpVerificationResult, CmdArgs, SampleDataset
from tests.utils.utils import get_rand_subdirectory_name

logger = logging.getLogger(__name__)


class ArchiveManagerArgs(CmdArgs):
    """Docstring."""

    script_path: Path
    config: Path
    dataset: str | None = None
    subcommand: str
    del_subcommand: str | None = None
    ids: list[str] | None = None
    begin_ts: int | None = None
    end_ts: int | None = None

    def to_cmd(self) -> list[str]:
        """Docstring."""
        cmd: list[str] = [
            str(self.script_path),
            "--config",
            str(self.config),
        ]

        if self.dataset:
            cmd.append("--dataset")
            cmd.append(self.dataset)

        cmd.append(self.subcommand)

        if self.del_subcommand:
            cmd.append(self.del_subcommand)
        if self.ids:
            cmd.extend(self.ids)
        if self.begin_ts is not None:
            cmd.append("--begin-ts")
            cmd.append(str(self.begin_ts))
        if self.end_ts is not None:
            cmd.append("--end-ts")
            cmd.append(str(self.end_ts))

        return cmd


class ClpPackageArchiveManagerType(Enum):
    """Docstring."""

    FIND = auto()
    DEL_BY_IDS = auto()
    DEL_BY_FILTER = auto()


class ClpPackageArchiveManagerSubcommand(StrEnum):
    """Docstring."""

    FIND_COMMAND = "find"
    DEL_COMMAND = "del"


class ClpPackageArchiveManagerDelSubcommand(StrEnum):
    """Docstring."""

    BY_IDS_COMMAND = "by-ids"
    BY_FILTER_COMMAND = "by-filter"


def archive_manager_clp_package(  # noqa: PLR0913
    clp_package: ClpPackage,
    archive_manager_type: ClpPackageArchiveManagerType,
    dataset: SampleDataset | None = None,
    begin_ts: int | None = None,
    end_ts: int | None = None,
    ids_to_del: list[str] | None = None,
) -> ClpAction:
    """Docstring."""
    log_msg = f"Performing '{archive_manager_type.name}' operation with archive-manager."
    logger.info(log_msg)

    args: ArchiveManagerArgs = _construct_archive_manager_args(
        clp_package,
        archive_manager_type,
        dataset,
        begin_ts,
        end_ts,
        ids_to_del,
    )
    return ClpAction.from_args(args)


def _construct_archive_manager_args(  # noqa: PLR0913
    clp_package: ClpPackage,
    archive_manager_type: ClpPackageArchiveManagerType,
    dataset: SampleDataset | None,
    begin_ts: int | None = None,
    end_ts: int | None = None,
    ids_to_del: list[str] | None = None,
) -> ArchiveManagerArgs:
    """Docstring."""
    path_config = clp_package.path_config
    args = ArchiveManagerArgs(
        script_path=path_config.archive_manager_path,
        config=clp_package.temp_config_file_path,
        subcommand=_get_subcommand(archive_manager_type),
    )

    if dataset is not None:
        args.dataset = dataset.dataset_name

    match archive_manager_type:
        case ClpPackageArchiveManagerType.FIND:
            if begin_ts is not None:
                args.begin_ts = begin_ts
            if end_ts is not None:
                args.end_ts = end_ts
        case ClpPackageArchiveManagerType.DEL_BY_IDS:
            args.del_subcommand = ClpPackageArchiveManagerDelSubcommand.BY_IDS_COMMAND
            if ids_to_del is not None:
                args.ids = ids_to_del
            else:
                sample_id = get_rand_subdirectory_name(path_config.package_archives_path)
                args.ids = [sample_id]
        case ClpPackageArchiveManagerType.DEL_BY_FILTER:
            args.del_subcommand = ClpPackageArchiveManagerDelSubcommand.BY_FILTER_COMMAND
            if begin_ts is not None:
                args.begin_ts = begin_ts
            if end_ts is None:
                pytest.fail(
                    "`end_ts` parameter cannot be 'None' when using archive-manager"
                    " 'del by-filter'."
                )
            args.end_ts = end_ts
        case _:
            pytest.fail(
                "Unsupported archive_management task type for CLP package:"
                f" '{archive_manager_type}'"
            )

    return args


def verify_archive_manager_find_action(
    action: ClpAction,
    clp_package: ClpPackage,
    dataset: SampleDataset | None = None,
) -> ClpVerificationResult:
    """Docstring."""
    logger.info("Verifying archive-manager 'find'.")
    returncode_result = action.verify_returncode()
    if not returncode_result:
        return returncode_result

    args = action.args
    assert isinstance(args, ArchiveManagerArgs)
    begin_ts = args.begin_ts if args.begin_ts is not None else 0
    end_ts = args.end_ts
    current_archive_id_list: list[str] = []

    # Find archives before begin_ts.
    if begin_ts > 0:
        find_before_action = archive_manager_clp_package(
            clp_package=clp_package,
            archive_manager_type=ClpPackageArchiveManagerType.FIND,
            dataset=dataset,
            begin_ts=0,
            end_ts=begin_ts,
        )
        find_before_result = find_before_action.verify_returncode()
        if not find_before_result:
            return ClpVerificationResult.fail(
                action,
                "Supporting 'find' call failed during archive-manager 'find' verification.",
                supporting_action=find_before_action,
            )
        current_archive_id_list.extend(_extract_archive_ids_from_find_output(find_before_action))

    # Add the archives from the original command.
    current_archive_id_list.extend(_extract_archive_ids_from_find_output(action))

    # Find archives after end_ts.
    if end_ts is not None:
        find_after_action = archive_manager_clp_package(
            clp_package=clp_package,
            archive_manager_type=ClpPackageArchiveManagerType.FIND,
            dataset=dataset,
            begin_ts=end_ts,
        )
        find_after_result = find_after_action.verify_returncode()
        if not find_after_result:
            return ClpVerificationResult.fail(
                action,
                "Supporting 'find' call failed during archive-manager 'find' verification.",
                supporting_action=find_after_action,
            )
        current_archive_id_list.extend(_extract_archive_ids_from_find_output(find_after_action))

    # Find all.
    find_all_action = archive_manager_clp_package(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
        dataset=dataset,
    )
    find_all_result = find_all_action.verify_returncode()
    if not find_all_result:
        return ClpVerificationResult.fail(
            action,
            "Supporting 'find' call failed during archive-manager 'find' verification.",
            supporting_action=find_all_action,
        )
    directories_in_archive_dir = _extract_archive_ids_from_find_output(find_all_action)

    # Compare.
    if current_archive_id_list == directories_in_archive_dir:
        return ClpVerificationResult.ok()

    return ClpVerificationResult.fail(
        action,
        "Archive-manager 'find' verification failure: mismatch between current archive ID list"
        f" '{current_archive_id_list}' and list of directories present in var/archives"
        f" directory '{directories_in_archive_dir}'."
        f" See supporting 'find' subprocess log at: '{find_all_action.log_file_path}'.",
    )


def verify_archive_manager_del_action(
    action: ClpAction,
    clp_package: ClpPackage,
    dataset: SampleDataset | None = None,
) -> ClpVerificationResult:
    """Docstring."""
    logger.info("Verifying archive-manager 'del'.")
    returncode_result = action.verify_returncode()
    if not returncode_result:
        return returncode_result

    args = action.args
    assert isinstance(args, ArchiveManagerArgs)
    match args.del_subcommand:
        case ClpPackageArchiveManagerDelSubcommand.BY_IDS_COMMAND:
            find_all_action = archive_manager_clp_package(
                clp_package=clp_package,
                archive_manager_type=ClpPackageArchiveManagerType.FIND,
                dataset=dataset,
            )
            find_result = verify_archive_manager_find_action(find_all_action, clp_package, dataset)
            if not find_result:
                return ClpVerificationResult.fail(
                    action,
                    "Supporting call to archive-manager 'find' could not be verified during"
                    f" archive-manager 'del' verification: '{find_result.failure_message}'",
                    supporting_action=find_all_action,
                )

            current_ids = _extract_archive_ids_from_find_output(find_all_action)
            if args.ids and any(item in current_ids for item in args.ids):
                return ClpVerificationResult.fail(
                    action,
                    "Archive-manager 'del by-ids' verification failure: Some archives that were"
                    " specified for deletion are still present in the metadata database."
                    f" See supporting 'find' subprocess log at: '{find_all_action.log_file_path}'.",
                )
        case ClpPackageArchiveManagerDelSubcommand.BY_FILTER_COMMAND:
            begin_ts = args.begin_ts
            end_ts = args.end_ts
            find_action = archive_manager_clp_package(
                clp_package=clp_package,
                archive_manager_type=ClpPackageArchiveManagerType.FIND,
                dataset=dataset,
                begin_ts=begin_ts,
                end_ts=end_ts,
            )
            find_result = verify_archive_manager_find_action(find_action, clp_package, dataset)
            if not find_result:
                return ClpVerificationResult.fail(
                    action,
                    "Supporting call to archive-manager 'find' could not be verified during"
                    f" archive-manager 'del' verification: '{find_result.failure_message}'",
                    supporting_action=find_action,
                )

            current_ids = _extract_archive_ids_from_find_output(find_action)
            if len(current_ids) > 0:
                return ClpVerificationResult.fail(
                    action,
                    "Archive-manager 'del by-filter' verification failure: Some archives that"
                    " should have been deleted were not deleted."
                    f" See supporting 'find' subprocess log at: '{find_action.log_file_path}'.",
                )
        case _:
            pytest.fail(
                "'archive-manager.sh del' needs a subcommand ('by-ids' or 'by-filter') but received"
                " neither."
            )

    return ClpVerificationResult.ok()


def _extract_archive_ids_from_find_output(
    action: ClpAction,
) -> list[str]:
    output_archive_id_list: list[str] = []
    output_lines = action.get_output().splitlines()

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


def _get_subcommand(archive_manager_type: ClpPackageArchiveManagerType) -> str:
    return (
        ClpPackageArchiveManagerSubcommand.FIND_COMMAND
        if archive_manager_type == ClpPackageArchiveManagerType.FIND
        else ClpPackageArchiveManagerSubcommand.DEL_COMMAND
    )
