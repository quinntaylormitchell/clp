"""Tests for the clp-presto package."""

import logging

import pytest

from tests.clp_package_tests.clp_json.utils.clear_archives import clear_package_archives_clp_json
from tests.clp_package_tests.clp_presto.utils.classes import (
    PrestoCluster,
)
from tests.clp_package_tests.clp_presto.utils.mode import (
    CLP_PRESTO_MODE,
)
from tests.clp_package_tests.clp_presto.utils.split_filters import (
    add_split_filter_for_dataset,
    clear_split_filter_file,
)
from tests.clp_package_tests.utils.classes import (
    ClpPackage,
)
from tests.clp_package_tests.utils.compress import compress_clp_package, verify_compress_action
from tests.utils.classes import (
    IntegrationTestDataset,
)

logger = logging.getLogger(__name__)

# Pytest markers for this module.
pytestmark = [
    pytest.mark.package,
    pytest.mark.clp_presto,
    pytest.mark.parametrize(
        "clp_package", [CLP_PRESTO_MODE], indirect=True, ids=[CLP_PRESTO_MODE.mode_name]
    ),
]


@pytest.mark.startup
def test_clp_presto_startup(clp_package: ClpPackage, presto_cluster: PrestoCluster) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_presto_startup'")

    assert clp_package
    assert presto_cluster

    logger.info("Test complete: 'test_clp_presto_startup'")


@pytest.mark.compression
def test_clp_presto_compression_json_multifile(
    clp_package: ClpPackage,
    presto_cluster: PrestoCluster,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_presto_compression_json_multifile'")

    clear_package_archives_clp_json(clp_package)
    clear_split_filter_file(presto_cluster)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message
    add_split_filter_for_dataset(json_multifile, presto_cluster)

    # TODO: some sort of split filter verification?

    clear_split_filter_file(presto_cluster)
    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_presto_compression_json_multifile'")


@pytest.mark.search
def test_clp_presto_query_json_multifile_basic(
    clp_package: ClpPackage,
    presto_cluster: PrestoCluster,
    json_multifile: IntegrationTestDataset,
) -> None:
    """Docstring."""
    logger.info("Starting test: 'test_clp_presto_query_json_multifile_basic'")

    clear_package_archives_clp_json(clp_package)
    clear_split_filter_file(presto_cluster)

    compress_action = compress_clp_package(clp_package, json_multifile)
    verified, failure_message = verify_compress_action(compress_action, clp_package, json_multifile)
    assert verified, failure_message
    add_split_filter_for_dataset(json_multifile, presto_cluster)

    # TODO: some sort of split filter verification?

    # TODO: presto queries

    clear_split_filter_file(presto_cluster)
    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_presto_query_json_multifile_basic'")
