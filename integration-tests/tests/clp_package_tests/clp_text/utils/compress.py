"""Docstring."""

import logging

from clp_package_utils.general import EXTRACT_FILE_CMD

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.parsers import get_compress_parser, get_decompress_parser
from tests.utils.classes import (
    IntegrationTestDataset,
)
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

    compress_cmd: list[str] = [
        str(clp_package.path_config.compress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        str(dataset.path_to_dataset_logs),
    ]
    compress_action = ClpPackageExternalAction(cmd=compress_cmd, args_parser=get_compress_parser())
    execute_external_action(compress_action)
    return compress_action


def verify_compress_action_clp_text(
    compress_action: ClpPackageExternalAction,
    clp_package: ClpPackage,
    original_dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    if compress_action.completed_proc.returncode != 0:
        return False, "The compress.sh subprocess returned a non-zero exit code."

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
        args_parser=get_decompress_parser(),
    )
    execute_external_action(decompress_action)
    assert decompress_action.completed_proc.returncode == 0, (
        "decompress.sh command returned non-zero exit code."
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
    fail_msg = (
        f"Mismatch between original logs at '{original_logs_path}' and decompressed logs at "
        f"'{decompressed_logs_path}'"
    )
    return False, fail_msg
