"""Functions and classes to facilitate clp-json dataset-manager."""

import logging
import re
from enum import auto, Enum
from pathlib import Path

import pytest
from strenum import StrEnum

from tests.package_tests.classes import ClpPackage
from tests.utils.classes import CmdArgs, ExternalAction, IntegrationTestDataset, VerificationResult
from tests.utils.logging_utils import format_action_failure_msg

logger = logging.getLogger(__name__)


class DatasetManagerArgs(CmdArgs):
    """Docstring."""

    script_path: Path
    config: Path
    subcommand: str
    del_all: bool = False
    datasets: list[str] | None = None

    def to_cmd(self) -> list[str]:
        """Docstring."""
        cmd = [str(self.script_path), "--config", str(self.config)]

        cmd.append(self.subcommand)

        if self.del_all:
            cmd.append("--all")
        if self.datasets:
            cmd.extend(self.datasets)

        return cmd


class ClpPackageDatasetManagerType(Enum):
    """Docstring."""

    LIST = auto()
    DEL = auto()


class ClpPackageDatasetManagerSubcommand(StrEnum):
    """Docstring."""

    LIST_COMMAND = "list"
    DEL_COMMAND = "del"


def dataset_manager_clp_json(
    clp_package: ClpPackage,
    dataset_manager_type: ClpPackageDatasetManagerType,
    datasets_to_del: list[IntegrationTestDataset] | None = None,
    del_all: bool = False,
) -> ExternalAction:
    """Docstring."""
    log_msg = f"Performing '{dataset_manager_type.name}' operation with dataset-manager."
    logger.info(log_msg)

    args: DatasetManagerArgs = _construct_dataset_manager_args(
        clp_package,
        dataset_manager_type,
        datasets_to_del,
        del_all,
    )
    return ExternalAction(cmd=args.to_cmd(), args=args)


def _construct_dataset_manager_args(
    clp_package: ClpPackage,
    dataset_manager_type: ClpPackageDatasetManagerType,
    datasets_to_del: list[IntegrationTestDataset] | None = None,
    del_all: bool = False,
) -> DatasetManagerArgs:
    """Docstring."""
    path_config = clp_package.path_config
    args = DatasetManagerArgs(
        script_path=path_config.dataset_manager_path,
        config=clp_package.temp_config_file_path,
        subcommand=_get_subcommand(dataset_manager_type),
    )

    match dataset_manager_type:
        case ClpPackageDatasetManagerType.LIST:
            pass
        case ClpPackageDatasetManagerType.DEL:
            args.del_all = del_all
            if datasets_to_del is not None:
                dataset_names: list[str] = []
                for dataset in datasets_to_del:
                    dataset_names.append(dataset.dataset_name)
                args.datasets = dataset_names
            elif not del_all:
                pytest.fail(
                    "You must specify either `datasets_to_del` or `del_all` arguments for"
                    " dataset-manager."
                )
        case _:
            pytest.fail(
                "Unsupported archive_management task type for CLP package:"
                f" '{dataset_manager_type}'"
            )

    return args


def verify_dataset_manager_list_action_clp_json(
    action: ExternalAction,
    clp_package: ClpPackage,
) -> VerificationResult:
    """Docstring."""
    logger.info("Verifying dataset-manager 'list'.")
    if action.completed_proc.returncode != 0:
        return VerificationResult.fail(
            format_action_failure_msg(
                "The 'dataset-manager.sh list' subprocess returned a non-zero exit code.",
                action,
            )
        )

    dataset_list = _extract_dataset_names_from_output(action)
    directories_in_package_archives = _get_names_of_directories_in_package_archives(clp_package)

    if dataset_list != directories_in_package_archives:
        return VerificationResult.fail(
            format_action_failure_msg(
                "Dataset-manager 'list' verification failure: mismatch between output dataset list"
                f" '{dataset_list}' and directories in var/archives"
                f" '{directories_in_package_archives}'",
                action,
            )
        )

    return VerificationResult.ok()


def verify_dataset_manager_del_action_clp_json(
    action: ExternalAction,
    clp_package: ClpPackage,
) -> VerificationResult:
    """Docstring."""
    logger.info("Verifying dataset-manager 'del'.")
    if action.completed_proc.returncode != 0:
        return VerificationResult.fail(
            format_action_failure_msg(
                "The 'dataset-manager.sh del' subprocess returned a non-zero exit code.",
                action,
            )
        )

    # Get list of all datasets currently in archives.
    list_action = dataset_manager_clp_json(
        clp_package=clp_package,
        dataset_manager_type=ClpPackageDatasetManagerType.LIST,
    )
    list_result = verify_dataset_manager_list_action_clp_json(list_action, clp_package)
    if not list_result:
        pytest.fail(
            "During dataset-manager 'del' verification, supporting call to dataset-manager 'list'"
            f" could not be verified: '{list_result.failure_message}' Subprocess log:"
            f" '{list_action.log_file_path}'"
        )

    current_datasets = _extract_dataset_names_from_output(list_action)

    args = action.args
    assert isinstance(args, DatasetManagerArgs)
    datasets_specified_for_deletion = args.datasets or []
    del_all_flag = args.del_all
    if del_all_flag:
        if len(current_datasets) > 0:
            return VerificationResult.fail(
                format_action_failure_msg(
                    f"Dataset-manager 'del --all' verification failure: There are datasets still"
                    f" present in the database: '{current_datasets}'.",
                    action,
                )
            )
    elif any(item in current_datasets for item in datasets_specified_for_deletion):
        return VerificationResult.fail(
            format_action_failure_msg(
                "Dataset-manager 'del' verification failure: Some datasets that were specified for"
                " deletion are still present in the database.",
                action,
            )
        )

    return VerificationResult.ok()


def _extract_dataset_names_from_output(
    action: ExternalAction,
) -> list[str]:
    dataset_list: list[str] = []
    output = _get_action_output(action)
    output_lines = output.splitlines()
    num_datasets = 0
    for line in output_lines:
        match = re.search(r"Found (\d+) datasets", line)
        if match:
            num_datasets = int(match.group(1))
            output_lines.remove(line)
            break

    if num_datasets == 0:
        return dataset_list

    for line in output_lines:
        match = re.search(r"INFO \[dataset_manager\] (.+)", line)
        if match:
            dataset_list.append(match.group(1))

    return sorted(dataset_list)


def _get_action_output(action: ExternalAction) -> str:
    """Return the combined stdout + stderr from a completed action."""
    return action.completed_proc.stdout + action.completed_proc.stderr


def _get_names_of_directories_in_package_archives(clp_package: ClpPackage) -> list[str]:
    directories_in_package_archives = []
    archives_dir = clp_package.path_config.package_archives_path
    if not archives_dir.exists():
        return []
    for item in archives_dir.iterdir():
        if item.is_dir():
            directories_in_package_archives.append(item.name)
    return sorted(directories_in_package_archives)


def _get_subcommand(dataset_manager_type: ClpPackageDatasetManagerType) -> str:
    return (
        ClpPackageDatasetManagerSubcommand.LIST_COMMAND
        if dataset_manager_type == ClpPackageDatasetManagerType.LIST
        else ClpPackageDatasetManagerSubcommand.DEL_COMMAND
    )
