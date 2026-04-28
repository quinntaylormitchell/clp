"""Docstring."""

import logging

import pytest

from tests.package_tests.clp_json.utils.dataset_manager import (
    ClpPackageDatasetManagerType,
    dataset_manager_clp_json,
    verify_dataset_manager_del_action_clp_json,
)
from tests.package_tests.utils.classes import (
    ClpPackage,
)

logger = logging.getLogger(__name__)


def clear_package_archives_clp_json(
    clp_package: ClpPackage,
) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")
    del_action, args = dataset_manager_clp_json(
        clp_package=clp_package,
        dataset_manager_type=ClpPackageDatasetManagerType.DEL,
        del_all=True,
    )
    verified, failure_message = verify_dataset_manager_del_action_clp_json(
        del_action, args, clp_package
    )
    if not verified:
        pytest.fail(
            f"When clearing package archives, the call to dataset-manager 'del' could not be"
            f" verified: '{failure_message}' Subprocess log: '{del_action.log_file_path}'"
        )
