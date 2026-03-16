"""Docstring."""

import logging

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.parsers import get_compress_parser
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.subprocess_utils import execute_external_action

logger = logging.getLogger(__name__)


def compress_clp_json(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
) -> ClpPackageExternalAction:
    """Docstring."""
    log_msg = f"Compressing the '{dataset.dataset_name}' dataset."
    logger.info(log_msg)

    clp_package_test_path_config = clp_package.path_config
    compress_cmd: list[str] = [
        str(clp_package_test_path_config.compress_path),
        "--config",
        str(clp_package.temp_config_file_path),
        "--dataset",
        dataset.metadata_dict["dataset_name"],
        "--timestamp-key",
        dataset.metadata_dict["timestamp_key"],
        str(dataset.path_to_dataset_logs),
    ]
    compress_action = ClpPackageExternalAction(cmd=compress_cmd, args_parser=get_compress_parser())
    execute_external_action(compress_action)
    return compress_action


def verify_compress_action_clp_json(
    compress_action: ClpPackageExternalAction, clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    if compress_action.completed_proc.returncode != 0:
        return False, "The compress.sh subprocess returned a non-zero exit code."

    # TODO: Waiting for PR 1299 (clp-json decompression) to be merged.
    assert clp_package
    return True, ""
