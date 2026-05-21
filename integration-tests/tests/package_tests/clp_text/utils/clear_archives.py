"""Helpers to clear all archives from a clp-text package between tests."""

import logging

from tests.package_tests.classes import (
    ClpPackage,
)
from tests.package_tests.utils.archive_manager import (
    archive_manager_del_by_ids,
    archive_manager_find,
    extract_archive_ids_from_find_output,
    verify_archive_manager_del_by_ids_action,
    verify_archive_manager_find_action,
)

logger = logging.getLogger(__name__)


def clear_package_archives_clp_text(clp_package: ClpPackage) -> None:
    """
    Deletes every archive in the given clp-text package by listing them via the archive-manager
    `find` action and then removing them by ID.

    :param clp_package:
    """
    logger.info("Clearing the %s archives.", clp_package.mode_name)

    # Find all.
    find_archive_manager_action = archive_manager_find(clp_package=clp_package)
    find_result = verify_archive_manager_find_action(find_archive_manager_action, clp_package)
    assert find_result, find_result.failure_message

    # Delete.
    archive_ids_to_delete = extract_archive_ids_from_find_output(find_archive_manager_action)
    if len(archive_ids_to_delete) > 0:
        del_by_ids_action = archive_manager_del_by_ids(
            clp_package=clp_package,
            ids=archive_ids_to_delete,
        )
        del_result = verify_archive_manager_del_by_ids_action(del_by_ids_action, clp_package)
        assert del_result, del_result.failure_message
