"""Docstring."""

import logging

import pytest

from tests.clp_package_tests.utils.archive_manager import (
    _extract_archive_ids_from_find_output,
    archive_manager_clp_text,
    ClpPackageArchiveManagerType,
    verify_archive_manager_del_action,
    verify_archive_manager_find_action,
)
from tests.clp_package_tests.utils.classes import (
    ClpPackage,
)

logger = logging.getLogger(__name__)


def clear_package_archives_clp_text(clp_package: ClpPackage) -> None:
    """Docstring."""
    logger.info(f"Clearing the {clp_package.mode_name} archives.")

    # Find all.
    find_archive_manager_action = archive_manager_clp_text(
        clp_package=clp_package,
        archive_manager_type=ClpPackageArchiveManagerType.FIND,
    )
    find_archive_manager_action_verified, failure_message = verify_archive_manager_find_action(
        find_archive_manager_action, clp_package
    )
    if not find_archive_manager_action_verified:
        pytest.fail(
            f"When clearing package archives, the call to archive-manager 'find' could not be"
            f" verified: '{failure_message}' Subprocess log:"
            f" '{find_archive_manager_action.log_file_path}'"
        )

    # Delete.
    archive_ids_to_delete = _extract_archive_ids_from_find_output(find_archive_manager_action)
    if len(archive_ids_to_delete) > 0:
        del_by_ids_action = archive_manager_clp_text(
            clp_package=clp_package,
            archive_manager_type=ClpPackageArchiveManagerType.DEL_BY_IDS,
            ids_to_del=archive_ids_to_delete,
        )
        archive_manager_action_verified, failure_message = verify_archive_manager_del_action(
            del_by_ids_action, clp_package
        )
        if not archive_manager_action_verified:
            pytest.fail(
                f"When clearing package archives, the call to archive-manager 'del' could not be"
                f" verified: '{failure_message}' Subprocess log:"
                f" '{del_by_ids_action.log_file_path}'"
            )
