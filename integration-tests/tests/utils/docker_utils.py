"""Provide utility functions related to the use of Docker during integration tests."""

import pytest

from tests.utils.classes import IntegrationTestExternalAction
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import get_binary_path


def list_running_services_in_compose_project(project_name: str) -> list[str]:
    """
    Lists running Docker services that belong to the given Docker Compose project.

    :param project_name:
    :return: List of the running services that belong to the specified Docker Compose project.
    """
    docker_bin = get_binary_path("docker")

    # fmt: off
    compose_ps_cmd = [
        docker_bin,
        "compose",
        "--project-name", project_name,
        "ps",
        "--format", "{{.Service}}",
    ]
    # fmt: on
    compose_ps_action = IntegrationTestExternalAction(cmd=compose_ps_cmd)
    execute_external_action(compose_ps_action)

    if compose_ps_action.completed_proc.returncode != 0:
        pytest.fail(
            "When getting running services in docker compose project, supporting call to docker"
            f" returned a non-zero exit code. Subprocess log: '{compose_ps_action.log_file_path}'"
        )

    compose_ps_output = compose_ps_action.completed_proc.stdout

    service_names: list[str] = []
    for line in (compose_ps_output or "").splitlines():
        service_name_candidate = line.strip()
        if service_name_candidate:
            service_names.append(service_name_candidate)

    return service_names
