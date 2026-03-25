"""Docstring."""

import logging
from typing import Any

import pytest
from clp_package_utils.general import EXTRACT_FILE_CMD

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.parsers import (
    construct_compress_arg_dict,
    construct_compress_cmd,
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


def verify_compress_action_clp_text(
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
