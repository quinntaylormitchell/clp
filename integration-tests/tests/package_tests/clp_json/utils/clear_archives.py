"""Docstring."""

import logging

import pytest

from tests.package_tests.classes import (
    ClpPackage,
)
from tests.package_tests.clp_json.utils.dataset_manager import (
    ClpPackageDatasetManagerType,
    dataset_manager_clp_json,
    verify_dataset_manager_del_action_clp_json,
)

logger = logging.getLogger(__name__)


def clear_package_archives_clp_json(
    clp_package: ClpPackage,
) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")
    del_action = dataset_manager_clp_json(
        clp_package=clp_package,
        dataset_manager_type=ClpPackageDatasetManagerType.DEL,
        del_all=True,
    )
    del_result = verify_dataset_manager_del_action_clp_json(del_action, clp_package)
    if not del_result:
        pytest.fail(
            f"When clearing package archives, the call to dataset-manager 'del' could not be"
            f" verified: '{del_result.failure_message}' Subprocess log:"
            f" '{del_action.log_file_path}'"
        )
