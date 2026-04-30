"""Functions and classes to facilitate clp-presto queries."""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pytest
from clp_py_utils.clp_config import PRESTO_COORDINATOR_COMPONENT_NAME
from pydantic import ValidationError

from tests.package_tests.clp_presto.utils.classes import PrestoCluster
from tests.utils.classes import (
    DatasetColumn,
    ExternalAction,
    IntegrationTestDataset,
    VerificationResult,
)
from tests.utils.utils import get_binary_path

logger = logging.getLogger(__name__)


PRESTO_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def query_clp_presto(
    presto_cluster: PrestoCluster,
    query: str,
) -> ExternalAction:
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

    return ExternalAction(cmd=cmd)


def verify_show_tables_action_clp_presto(
    show_tables_action: ExternalAction,
    current_datasets: list[IntegrationTestDataset],
) -> VerificationResult:
    """Verify that `SHOW TABLES;` output is accurate w.r.t. current datasets."""
    logger.info("Verifying 'SHOW TABLES;' Presto query.")

    output_lines = show_tables_action.completed_proc.stdout.splitlines()
    try:
        actual: set[str] = {json.loads(line)["Table"] for line in output_lines if line.strip()}
    except (json.JSONDecodeError, KeyError) as e:
        return VerificationResult.fail(f"Failed to parse output of `SHOW TABLES;` as JSON: {e}")

    expected: set[str] = {ds.dataset_name for ds in current_datasets}

    if actual == expected:
        return VerificationResult.ok()

    return VerificationResult.fail(
        f"Mismatch between set of tables from `SHOW TABLES;` query: '{actual}' and expected set:"
        f" '{expected}'"
    )


def verify_describe_dataset_action_clp_presto(
    describe_dataset_action: ExternalAction,
    dataset: IntegrationTestDataset,
) -> VerificationResult:
    """
    Verify that `DESCRIBE <dataset_name>;` output is accurate w.r.t. "columns" field from dataset
    metadata.
    """
    logger.info("Verifying 'DESCRIBE <dataset_name>;' Presto query.")

    if dataset.columns is None:
        pytest.fail(
            f"The '{dataset}' dataset doesn't have a file that describes the column structure"
            " (should be `columns.json`)."
        )
    expected: list[DatasetColumn] = dataset.columns.columns

    output_lines = describe_dataset_action.completed_proc.stdout.splitlines()
    try:
        actual: list[DatasetColumn] = [
            DatasetColumn.model_validate_json(line) for line in output_lines if line.strip()
        ]
    except ValidationError as e:
        return VerificationResult.fail(
            f"Failed to parse output of `DESCRIBE <dataset_name>;` as `DatasetColumn`: {e}"
        )

    if actual == expected:
        return VerificationResult.ok()

    return VerificationResult.fail(
        f"Mismatch between dataset column description from `DESCRIBE <dataset_name>;` query:"
        f" '{actual}' and expected column description: '{expected}'"
    )


def verify_select_logs_action_clp_presto(
    select_logs_action: ExternalAction,
    dataset: IntegrationTestDataset,
) -> VerificationResult:
    """
    Verify that `SELECT * FROM <dataset_name>;` output is accurate w.r.t. grep -r ".*" output for
    dataset logs.
    """
    logger.info("Verifying 'SELECT * FROM <dataset_name>;' Presto query.")

    actual: list[dict[str, Any]] = _load_str_to_json_list(select_logs_action.completed_proc.stdout)

    cmd = [
        get_binary_path("grep"),
        "--recursive",
        "--no-filename",
        "--color=never",
        ".*",
        str(dataset.logs_path),
    ]
    grep_action = ExternalAction(cmd)
    if grep_action.completed_proc.returncode != 0:
        pytest.fail(
            "During `SELECT * FROM <dataset_name>;` verification, supporting `grep` call returned a"
            f" non-zero exit code. Subprocess log: {grep_action.log_file_path}"
        )
    expected: list[dict[str, Any]] = _format_grep_output(grep_action.completed_proc.stdout)

    if _as_multiset(actual) == _as_multiset(expected):
        return VerificationResult.ok()

    return VerificationResult.fail(
        f"Mismatch between output logs from `SELECT * FROM <dataset_name>;` query: '{actual}'"
        f" and expected logs: '{expected}'"
    )


def _load_str_to_json_list(raw_str: str) -> list[dict[str, Any]]:
    """Load a string containing multiple JSON objects (one per line) into a list of dicts."""
    try:
        return [json.loads(line) for line in raw_str.splitlines() if line.strip()]
    except json.JSONDecodeError as e:
        pytest.fail(f"Failed to parse string as JSON: {e}")


def _format_grep_output(raw_output: str) -> list[dict[str, Any]]:
    """
    Formats raw grep output into a list of JSON lines, then normalizes the timestamp field of each
    log line from POSIX ms to 'YYYY-MM-DD HH:MM:SS.mmm' format to match Presto's `timestamp` column
    JSON output.
    """
    lines: list[dict[str, Any]] = _load_str_to_json_list(raw_output)
    for line in lines:
        timestamp_ms = line.get("timestamp")
        if timestamp_ms is not None:
            line["timestamp"] = _format_posix_ms(timestamp_ms)
    return lines


def _format_posix_ms(timestamp_ms: int) -> str:
    """Convert a POSIX ms timestamp to 'YYYY-MM-DD HH:MM:SS.mmm'."""
    date = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    return date.strftime(PRESTO_TIMESTAMP_FORMAT)[:-3]


def _as_multiset(records: list[dict[str, Any]]) -> list[str]:
    """Serialize each dict to a canonical JSON string for comparison as a sorted list."""
    return sorted(json.dumps(r, sort_keys=True) for r in records)
