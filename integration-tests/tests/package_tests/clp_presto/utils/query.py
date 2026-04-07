"""Functions and classes to facilitate clp-presto queries."""

import json
import logging
from typing import Any

import pytest
from clp_py_utils.clp_config import (
    PRESTO_COORDINATOR_COMPONENT_NAME,
)

from tests.package_tests.clp_presto.utils.classes import (
    PrestoCluster,
)
from tests.utils.classes import (
    IntegrationTestDataset,
    IntegrationTestExternalAction,
)
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import (
    get_binary_path,
)

logger = logging.getLogger(__name__)


def query_clp_presto(
    presto_cluster: PrestoCluster,
    query: str,
) -> IntegrationTestExternalAction:
    """Docstring."""
    logger.info(f"Running Presto query: '{query}'")

    docker_compose_file_path = presto_cluster.path_config.docker_compose_file_path
    cmd: list[str] = [
        get_binary_path("docker"),
        "compose",
        "--file",
        str(docker_compose_file_path),
        "exec",
        PRESTO_COORDINATOR_COMPONENT_NAME,
        "presto-cli",
        "--catalog",
        "clp",
        "--schema",
        "default",
        "--output-format",
        "JSON",
        "--execute",
        query,
    ]

    action = IntegrationTestExternalAction(cmd=cmd)
    execute_external_action(action)

    return action


def verify_show_tables_action_clp_presto(
    show_tables_action: IntegrationTestExternalAction,
    current_datasets: list[IntegrationTestDataset],
) -> tuple[bool, str]:
    """Verify that `SHOW TABLES;` output is accurate w.r.t. current datasets."""
    logger.info("Verifying `SHOW TABLES;` Presto query.")

    output_lines = show_tables_action.completed_proc.stdout.splitlines()
    try:
        actual: set[str] = {json.loads(line)["Table"] for line in output_lines if line.strip()}
    except (json.JSONDecodeError, KeyError) as e:
        return False, f"Failed to parse output of `SHOW TABLES;` as JSON: {e}"

    expected: set[str] = {ds.dataset_name for ds in current_datasets}

    if actual != expected:
        return (
            False,
            f"Mismatch between set of tables from `SHOW TABLES;` query: '{actual}' and expected"
            f" set: '{expected}'",
        )

    return True, ""


def verify_describe_dataset_action_clp_presto(
    describe_dataset_action: IntegrationTestExternalAction,
    dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """
    Verify that `DESCRIBE <dataset_name>;` output is accurate w.r.t. "columns" field from dataset
    metadata.
    """
    logger.info("Verifying `DESCRIBE <dataset_name>;` Presto query.")

    output_lines = describe_dataset_action.completed_proc.stdout.splitlines()
    try:
        actual: list[dict[str, Any]] = [json.loads(line) for line in output_lines if line.strip()]
    except (json.JSONDecodeError, KeyError) as e:
        return False, f"Failed to parse output of `DESCRIBE <dataset_name>;` as JSON: {e}"

    expected: list[dict[str, Any]] = dataset.metadata_dict["columns"]

    if actual != expected:
        return (
            False,
            f"Mismatch between dataset column description from `DESCRIBE <dataset_name>;` query:"
            f" '{actual}' and expected column description: '{expected}'",
        )

    return True, ""


def verify_select_logs_action_clp_presto(
    select_logs_action: IntegrationTestExternalAction,
    dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """
    Verify that `SELECT * FROM <dataset_name>;` output is accurate w.r.t. grep -r ".*" output for
    dataset logs.
    """
    logger.info("Verifying `SELECT * FROM <dataset_name>;` Presto query.")

    actual = select_logs_action.completed_proc.stdout

    cmd = [
        get_binary_path("grep"),
        "--recursive",
        "--no-filename",
        "--color=never",
        ".*",
        str(dataset.path_to_dataset_logs),
    ]
    grep_action = IntegrationTestExternalAction(cmd)
    execute_external_action(grep_action)
    if grep_action.completed_proc.returncode != 0:
        pytest.fail(
            "During `SELECT * FROM <dataset_name>;` verification, supporting `grep` call returned a"
            f" non-zero exit code. Subprocess log: {grep_action.log_file_path}"
        )
    expected = grep_action.completed_proc.stdout

    if actual != expected:
        return (
            False,
            f"Mismatch between output logs from `SELECT * FROM <dataset_name>;` query: '{actual}'"
            f" and expected logs: '{expected}'",
        )

    return True, ""
