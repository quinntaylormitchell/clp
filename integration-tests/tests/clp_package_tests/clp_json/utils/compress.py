"""Docstring."""

import logging
from typing import Any

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


def verify_compress_action_clp_json(
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
