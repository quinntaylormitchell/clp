"""Docstring."""

import logging
import re
from contextlib import closing

import pytest
from clp_py_utils.clp_config import (
    ClpConfig,
)
from clp_py_utils.clp_metadata_db_utils import (
    get_datasets_table_name,
)
from clp_py_utils.sql_adapter import SqlAdapter
from dotenv import load_dotenv

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
    ClpPackageTestPathConfig,
)
from tests.clp_package_tests.utils.parsers import (
    get_dataset_manager_parser,
)
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.subprocess_utils import execute_external_action

logger = logging.getLogger(__name__)


def dataset_manager_list_clp_json(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = "Performing 'list' operation with dataset manager."
    logger.info(log_msg)

    dataset_manager_cmd: list[str] = [
        str(clp_package_test_path_config.dataset_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "list",
    ]

    dataset_manager_action = ClpPackageExternalAction(
        cmd=dataset_manager_cmd,
        args_parser=get_dataset_manager_parser(),
    )
    execute_external_action(dataset_manager_action)
    return dataset_manager_action


def dataset_manager_del_clp_json(
    clp_package_test_path_config: ClpPackageTestPathConfig,
    clp_package: ClpPackage,
    datasets_to_del: list[IntegrationTestDataset] | None = None,
    del_all: bool = False,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = "Performing 'del' operation with dataset manager."
    logger.info(log_msg)

    dataset_manager_cmd: list[str] = [
        str(clp_package_test_path_config.dataset_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "del",
    ]
    if del_all:
        dataset_manager_cmd.append("--all")
    elif datasets_to_del is not None:
        for dataset in datasets_to_del:
            dataset_manager_cmd.append(dataset.dataset_name)
    else:
        pytest.fail("You must specify either `datasets_to_del` or `del_all` arguments.")

    dataset_manager_action = ClpPackageExternalAction(
        cmd=dataset_manager_cmd,
        args_parser=get_dataset_manager_parser(),
    )
    execute_external_action(dataset_manager_action)
    return dataset_manager_action


def verify_dataset_manager_list_action_clp_json(
    dataset_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying dataset-manager list action.")
    if dataset_manager_action.completed_proc.returncode != 0:
        return (
            False,
            "The dataset-manager.sh list subprocess returned a non-zero exit code.",
        )

    output_dataset_list = _extract_dataset_names_from_output(dataset_manager_action)
    current_datasets_in_db = _get_dataset_names(clp_package.clp_config)

    if output_dataset_list != current_datasets_in_db:
        fail_msg = (
            f"Mismatch between output dataset list '{output_dataset_list}' and list of datasets"
            f" currently in the metadata database: '{current_datasets_in_db}'"
        )
        return False, fail_msg

    return True, ""


def verify_dataset_manager_del_action_clp_json(
    dataset_manager_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info("Verifying dataset-manager del action.")
    if dataset_manager_action.completed_proc.returncode != 0:
        return (
            False,
            "The dataset-manager.sh del subprocess returned a non-zero exit code.",
        )

    current_datasets_in_db = _get_dataset_names(clp_package.clp_config)
    parsed_args = dataset_manager_action.parsed_args
    del_all_flag = parsed_args.del_all
    if del_all_flag:
        # Verify that there are no datasets left.
        if len(current_datasets_in_db) > 0:
            fail_msg = (
                f"dataset-manager del --all failed: There are datasets still present in the"
                f" metadata database: {current_datasets_in_db}"
            )
            return False, fail_msg
    else:
        datasets_specified_for_deletion = parsed_args.datasets
        if len(datasets_specified_for_deletion) == 0:
            # No datasets were specified for deletion.
            return True, ""

        # Verify that the datasets specified for deletion are not present.
        if any(item in current_datasets_in_db for item in datasets_specified_for_deletion):
            fail_msg = (
                "dataset-manager del failed: Some datasets that were specified for deletion"
                " are still present in the metadata database."
            )
            return False, fail_msg

    return True, ""


def _extract_dataset_names_from_output(
    dataset_manager_action: ClpPackageExternalAction,
) -> list[str]:
    dataset_list: list[str] = []
    output = (
        dataset_manager_action.completed_proc.stdout + dataset_manager_action.completed_proc.stderr
    )
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


def clear_package_archives_clp_json(clp_package: ClpPackage) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")
    path_config = clp_package.path_config
    dataset_manager_cmd = [
        str(path_config.dataset_manager_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "del",
        "--all",
    ]

    dataset_manager_action = ClpPackageExternalAction(
        cmd=dataset_manager_cmd,
        args_parser=get_dataset_manager_parser(),
    )
    execute_external_action(dataset_manager_action)

    dataset_manager_action_verified, failure_message = verify_dataset_manager_del_action_clp_json(
        dataset_manager_action, clp_package
    )
    assert dataset_manager_action_verified, failure_message


def _get_dataset_names(clp_config: ClpConfig) -> list[str]:
    """
    :param clp_config:
    :return: A list of all datasets that currently exist in the metadata database.
    """
    load_dotenv(dotenv_path="/home/quinnmitchell/clp/build/clp-package/.env")
    database = clp_config.database
    database.load_credentials_from_env()

    sql_adapter = SqlAdapter(database)
    with (
        closing(sql_adapter.create_connection(True)) as db_conn,
        closing(db_conn.cursor(dictionary=True)) as db_cursor,
    ):
        clp_db_connection_params = database.get_clp_connection_params_and_type(True)
        table_prefix = clp_db_connection_params["table_prefix"]
        db_cursor.execute(f"SELECT name FROM `{get_datasets_table_name(table_prefix)}`")  # noqa: S608
        rows = db_cursor.fetchall()
        return [row["name"] for row in rows]  # type: ignore[call-overload, misc]
