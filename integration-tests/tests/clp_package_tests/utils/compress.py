"""Functions to facilitate CLP package compression."""

import logging
from typing import Any

import pytest
from clp_package_utils.general import EXTRACT_FILE_CMD
from clp_py_utils.clp_config import StorageEngine

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.logging_utils import format_action_failure_msg
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import (
    clear_directory,
    is_dir_tree_content_equal,
)

logger = logging.getLogger(__name__)


def compress_clp_json(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = f"Compressing the '{dataset.dataset_name}' dataset."
    logger.info(log_msg)

    arg_dict: dict[str, Any] = construct_compress_arg_dict(clp_package, dataset)
    compress_action = ClpPackageExternalAction(
        cmd=construct_compress_cmd(arg_dict), arg_dict=arg_dict
    )
    execute_external_action(compress_action)

    return compress_action


def compress_clp_text(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = f"Compressing the '{dataset.dataset_name}' dataset."
    logger.info(log_msg)

    arg_dict: dict[str, Any] = construct_compress_arg_dict(clp_package, dataset)
    compress_action = ClpPackageExternalAction(
        cmd=construct_compress_cmd(arg_dict), arg_dict=arg_dict
    )
    execute_external_action(compress_action)

    return compress_action


def construct_compress_arg_dict(
    clp_package: ClpPackage, dataset: IntegrationTestDataset
) -> dict[str, Any]:
    """Docstring."""
    path_config = clp_package.path_config

    arg_dict: dict[str, Any] = {
        "script_path": path_config.compress_path,
        "config": clp_package.temp_config_file_path,
    }

    if clp_package.clp_config.package.storage_engine == StorageEngine.CLP_S:
        arg_dict["dataset"] = dataset.metadata_dict["dataset"]
        arg_dict["timestamp_key"] = dataset.metadata_dict["timestamp_key"]
        arg_dict["unstructured"] = dataset.metadata_dict["data"]["unstructured"]

    arg_dict["paths"] = [dataset.path_to_dataset_logs]

    return arg_dict


def construct_compress_cmd(arg_dict: dict[str, Any]) -> list[str]:
    """Docstring."""
    compress_cmd: list[str] = [
        str(arg_dict["script_path"]),
        "--config",
        str(arg_dict["config"]),
    ]
    if "dataset" in arg_dict:
        compress_cmd.append("--dataset")
        compress_cmd.append(arg_dict["dataset"])
    if "timestamp_key" in arg_dict:
        compress_cmd.append("--timestamp-key")
        compress_cmd.append(arg_dict["timestamp_key"])
    if arg_dict.get("unstructured"):
        compress_cmd.append("--unstructured")
    if "paths" in arg_dict:
        compress_cmd.extend(arg_dict["paths"])

    return compress_cmd


def verify_compress_action(
    compress_action: ClpPackageExternalAction,
    clp_package: ClpPackage,
    original_dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    if compress_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The compress.sh subprocess returned a non-zero exit code.",
            compress_action,
        )

    mode = clp_package.mode_name
    match mode:
        case "clp-json" | "clp-presto":
            result = _verify_compress_action_clp_json(compress_action, clp_package)
        case "clp-text":
            result = _verify_compress_action_clp_text(
                compress_action, clp_package, original_dataset
            )
        case _:
            pytest.fail(f"Compression verification for the '{mode}' mode is not supported.")

    return result


def _verify_compress_action_clp_json(
    compress_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    if compress_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The compress.sh subprocess returned a non-zero exit code.",
            compress_action,
        )

    # TODO: Waiting for PR 1299 (clp-json decompression) to be merged.
    return True, ""


def _verify_compress_action_clp_text(
    compress_action: ClpPackageExternalAction,
    clp_package: ClpPackage,
    original_dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    if compress_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The 'compress.sh' subprocess returned a non-zero exit code.",
            compress_action,
        )

    # Decompress the contents of `clp-package/var/data/archives`.
    path_config = clp_package.path_config
    clear_directory(path_config.package_decompression_dir)

    decompress_cmd = [
        str(path_config.decompress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        EXTRACT_FILE_CMD,
        "--extraction-dir",
        str(path_config.package_decompression_dir),
    ]
    decompress_action = ClpPackageExternalAction(
        cmd=decompress_cmd,
        arg_dict={},
    )
    execute_external_action(decompress_action)
    if decompress_action.completed_proc.returncode != 0:
        pytest.fail(
            f"During compress action verification, internal decompress.sh command returned a"
            f" non-zero exit code. Subprocess log: {decompress_action.log_file_path}"
        )

    # Verify equality between original logs and decompressed logs.
    original_logs_path = original_dataset.path_to_dataset_logs
    decompressed_logs_path = path_config.package_decompression_dir / original_logs_path.relative_to(
        original_logs_path.anchor
    )

    equal = is_dir_tree_content_equal(original_logs_path, decompressed_logs_path)
    clear_directory(path_config.package_decompression_dir)
    if equal:
        return True, ""
    return format_action_failure_msg(
        f"Compress verification failure: mismatch between original logs at '{original_logs_path}'"
        f" and decompressed logs at '{decompressed_logs_path}'.",
        compress_action,
    )
