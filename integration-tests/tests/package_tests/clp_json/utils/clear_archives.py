"""Helpers to clear all archives from a clp-json package between tests."""

import logging

import pytest

from tests.package_tests.classes import (
    ClpPackage,
)
from tests.package_tests.clp_json.utils.dataset_manager import (
    dataset_manager_del,
    verify_dataset_manager_del_action_clp_json,
)

logger = logging.getLogger(__name__)


def clear_package_archives_clp_json(
    clp_package: ClpPackage,
) -> None:
    """
    Deletes all datasets (and their archives) from the given clp-json package via the
    dataset-manager.

    :param clp_package:
    """
    logger.info("Clearing the %s archives.", clp_package.mode_name)
    del_action = dataset_manager_del(
        clp_package=clp_package,
        del_all=True,
    )
    del_result = verify_dataset_manager_del_action_clp_json(del_action, clp_package)
    if not del_result:
        pytest.fail(
            f"When clearing package archives, the call to dataset-manager 'del' could not be"
            f" verified: '{del_result.failure_message}' Subprocess log:"
            f" '{del_action.log_file_path}'"
        )
