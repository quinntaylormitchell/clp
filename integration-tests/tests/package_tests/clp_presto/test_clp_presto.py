"""Tests for the clp-presto package."""

import logging

import pytest

from tests.package_tests.classes import (
    ClpPackage,
)
from tests.package_tests.clp_json.utils.clear_archives import (
    clear_package_archives_clp_json,
)
from tests.package_tests.clp_presto.utils.classes import (
    PrestoCluster,
)
from tests.package_tests.clp_presto.utils.mode import (
    CLP_PRESTO_MODE,
)
from tests.package_tests.clp_presto.utils.query import (
    query_clp_presto,
    verify_describe_dataset_action_clp_presto,
    verify_select_logs_action_clp_presto,
    verify_show_tables_action_clp_presto,
)
from tests.package_tests.clp_presto.utils.split_filters import (
    add_split_filter_for_dataset,
    clear_split_filter_file,
)
from tests.package_tests.utils.compress import (
    compress_clp_package,
    verify_compress_action,
)
from tests.utils.classes import (
    SampleDataset,
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
    """
    Verifies that both the clp-presto package and its companion Presto cluster start up
    successfully.

    :param clp_package:
    :param presto_cluster:
    """
    logger.info("Starting test: 'test_clp_presto_startup'")

    assert clp_package
    assert presto_cluster

    logger.info("Test complete: 'test_clp_presto_startup'")


@pytest.mark.compression
@pytest.mark.presto_query
def test_clp_presto_json_multifile(
    clp_package: ClpPackage,
    presto_cluster: PrestoCluster,
    json_multifile: SampleDataset,
) -> None:
    """
    Verifies that, after compressing the `json_multifile` dataset, the Presto cluster correctly
    answers `SHOW TABLES`, `DESCRIBE`, and `SELECT *` queries against it.

    :param clp_package:
    :param presto_cluster:
    :param json_multifile:
    """
    logger.info("Starting test: 'test_clp_presto_json_multifile'")

    clear_package_archives_clp_json(clp_package)
    clear_split_filter_file(presto_cluster)

    compress_action = compress_clp_package(clp_package, json_multifile)
    compress_result = verify_compress_action(compress_action, clp_package, json_multifile)
    assert compress_result, compress_result.failure_message

    add_split_filter_for_dataset(json_multifile, presto_cluster)

    show_tables_action = query_clp_presto(
        presto_cluster=presto_cluster,
        query="SHOW TABLES;",
    )
    show_tables_result = verify_show_tables_action_clp_presto(show_tables_action, [json_multifile])
    assert show_tables_result, show_tables_result.failure_message

    describe_dataset_action = query_clp_presto(
        presto_cluster=presto_cluster,
        query=f"DESCRIBE {json_multifile.dataset_name};",
    )
    describe_dataset_result = verify_describe_dataset_action_clp_presto(
        describe_dataset_action, json_multifile
    )
    assert describe_dataset_result, describe_dataset_result.failure_message

    select_logs_action = query_clp_presto(
        presto_cluster=presto_cluster,
        query=f"SELECT * FROM {json_multifile.dataset_name};",  # noqa: S608
    )
    select_logs_result = verify_select_logs_action_clp_presto(select_logs_action, json_multifile)
    assert select_logs_result, select_logs_result.failure_message

    clear_split_filter_file(presto_cluster)
    clear_package_archives_clp_json(clp_package)

    logger.info("Test complete: 'test_clp_presto_json_multifile'")
