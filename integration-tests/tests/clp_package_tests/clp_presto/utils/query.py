"""Functions and classes to facilitate clp-presto queries."""

from clp_py_utils.clp_config import (
    PRESTO_COORDINATOR_COMPONENT_NAME,
)

from tests.clp_package_tests.clp_presto.utils.classes import PrestoCluster
from tests.utils.classes import (
    IntegrationTestExternalAction,
)
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import (
    get_binary_path,
)


def query_clp_presto(
    presto_cluster: PrestoCluster,
    query: str,
) -> IntegrationTestExternalAction:
    """Docstring."""
    docker_compose_file_path = presto_cluster.path_config.docker_compose_file_path
    cmd: list[str] = [
        get_binary_path("docker"),
        "compose",
        "--file",
        str(docker_compose_file_path),
        "exec",
        PRESTO_COORDINATOR_COMPONENT_NAME,
        "presto-cli",
        "--catalog",
        "clp",
        "--schema",
        "default",
        "--output-format",
        "JSON",
        "--execute",
        query,
    ]

    action = IntegrationTestExternalAction(cmd=cmd)
    execute_external_action(action)

    return action
