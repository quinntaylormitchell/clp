"""Functions and classes to facilitate clp-json dataset-manager."""

import logging
import re
from enum import auto, Enum
from pathlib import Path

import pytest
from strenum import StrEnum

from tests.package_tests.classes import ClpPackage
from tests.utils.classes import ClpAction, ClpVerificationResult, CmdArgs, SampleDataset

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
    datasets_to_del: list[SampleDataset] | None = None,
    del_all: bool = False,
) -> ClpAction:
    """Docstring."""
    log_msg = f"Performing '{dataset_manager_type.name}' operation with dataset-manager."
    logger.info(log_msg)

    args: DatasetManagerArgs = _construct_dataset_manager_args(
        clp_package,
        dataset_manager_type,
        datasets_to_del,
        del_all,
    )
    return ClpAction.from_args(args)


def _construct_dataset_manager_args(
    clp_package: ClpPackage,
    dataset_manager_type: ClpPackageDatasetManagerType,
    datasets_to_del: list[SampleDataset] | None = None,
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
    action: ClpAction,
    clp_package: ClpPackage,
) -> ClpVerificationResult:
    """Docstring."""
    logger.info("Verifying dataset-manager 'list'.")
    returncode_result = action.verify_returncode()
    if not returncode_result:
        return returncode_result

    dataset_list = _extract_dataset_names_from_output(action)
    directories_in_package_archives = _get_names_of_directories_in_package_archives(clp_package)

    if dataset_list == directories_in_package_archives:
        return action.pass_verification()

    return action.fail_verification(
        reason=(
            "Dataset-manager 'list' verification failure: mismatch between output dataset list"
            f" '{dataset_list}' and directories in var/archives"
            f" '{directories_in_package_archives}'"
        ),
    )


def verify_dataset_manager_del_action_clp_json(
    action: ClpAction,
    clp_package: ClpPackage,
) -> ClpVerificationResult:
    """Docstring."""
    logger.info("Verifying dataset-manager 'del'.")
    returncode_result = action.verify_returncode()
    if not returncode_result:
        return returncode_result

    # Get list of all datasets currently in archives.
    list_action = dataset_manager_clp_json(
        clp_package=clp_package,
        dataset_manager_type=ClpPackageDatasetManagerType.LIST,
    )
    list_result = verify_dataset_manager_list_action_clp_json(list_action, clp_package)
    if not list_result:
        return action.fail_verification(
            reason=(
                "Supporting call to dataset-manager 'list' failed during dataset-manager 'del'"
                " verification"
            ),
            supporting_action=list_action,
        )

    current_datasets = _extract_dataset_names_from_output(list_action)

    args = action.args
    assert isinstance(args, DatasetManagerArgs)

    datasets_specified_for_deletion = args.datasets or []
    del_all_flag = args.del_all
    if del_all_flag and len(current_datasets) > 0:
        return action.fail_verification(
            reason=(
                f"Dataset-manager 'del --all' verification failure: There are datasets still"
                f" present in the database: '{current_datasets}'."
            ),
        )

    if any(item in current_datasets for item in datasets_specified_for_deletion):
        return action.fail_verification(
            reason=(
                "Dataset-manager 'del' verification failure: Some datasets that were specified for"
                " deletion are still present in the database."
            ),
        )

    return action.pass_verification()


def _extract_dataset_names_from_output(
    action: ClpAction,
) -> list[str]:
    dataset_list: list[str] = []
    output = action.get_output()
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
