"""Session-scoped path configuration fixtures used in clp-presto integration tests."""

import logging
from collections.abc import Iterator

import pytest

from tests.package_tests.clp_presto.utils.classes import (
    PrestoCluster,
    PrestoClusterTestPathConfig,
)
from tests.package_tests.clp_presto.utils.start_stop import (
    start_presto_cluster,
    stop_presto_cluster,
    verify_start_presto_action,
    verify_stop_presto_action,
)
from tests.utils.classes import ExternalAction
from tests.utils.utils import resolve_path_env_var

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def presto_cluster_test_path_config() -> PrestoClusterTestPathConfig:
    """Provides paths relevant to all clp-presto integration tests."""
    return PrestoClusterTestPathConfig(
        clp_build_dir=resolve_path_env_var("CLP_BUILD_DIR"),
        integration_tests_project_root=resolve_path_env_var("INTEGRATION_TESTS_PROJECT_ROOT"),
        clp_package_dir=resolve_path_env_var("CLP_PACKAGE_DIR"),
        presto_deployment_dir=resolve_path_env_var("PRESTO_DEPLOYMENT_DIR"),
    )


@pytest.fixture(scope="module")
def presto_cluster(
    presto_cluster_test_path_config: PrestoClusterTestPathConfig,
) -> Iterator[PrestoCluster]:
    """Docstring for presto_cluster fixture."""
    setup_presto_cmd: list[str] = [
        str(presto_cluster_test_path_config.set_up_config_path),
        str(presto_cluster_test_path_config.clp_package_dir),
    ]
    setup_presto_action = ExternalAction(cmd=setup_presto_cmd)
    if setup_presto_action.completed_proc.returncode != 0:
        pytest.fail(
            "During Presto cluster setup, supporting call to set-up-config.sh returned a non-zero"
            f" exit code. Subprocess log: '{setup_presto_action.log_file_path}'"
        )

    # Construct `PrestoCluster` object.
    presto_cluster = PrestoCluster(path_config=presto_cluster_test_path_config)

    try:
        start_presto_action = start_presto_cluster(presto_cluster)
        start_presto_action_verified, failure_message = verify_start_presto_action(
            start_presto_action
        )
        assert start_presto_action_verified, failure_message
        yield presto_cluster
    finally:
        stop_presto_action = stop_presto_cluster(presto_cluster)
        stop_presto_action_verified, failure_message = verify_stop_presto_action(stop_presto_action)
        assert stop_presto_action_verified, failure_message
