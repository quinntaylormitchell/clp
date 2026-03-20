"""Provide utility functions related to the use of Docker during integration tests."""

import re

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
    compose_ps_cmd = [
        get_binary_path("docker"),
        "compose",
        "--project-name",
        project_name,
        "ps",
        "--format",
        "{{.Service}}",
    ]
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


def list_running_containers_with_prefix(prefix: str) -> list[str]:
    """
    Lists running Docker containers whose names begin with `prefix` and end with one or more digits.

    :param prefix:
    :return: List of running container names that match the pattern.
    """
    docker_ps_cmd = [
        get_binary_path("docker"),
        "ps",
        "--format",
        "{{.Names}}",
        "--filter",
        f"name={prefix}",
    ]
    docker_ps_action = IntegrationTestExternalAction(cmd=docker_ps_cmd)
    execute_external_action(docker_ps_action)
    docker_ps_proc = docker_ps_action.completed_proc
    if docker_ps_proc.returncode != 0:
        pytest.fail(
            "When getting containers with prefix in docker compose project, supporting call to"
            " docker returned a non-zero exit code. Subprocess log:"
            f" '{docker_ps_action.log_file_path}'"
        )

    matches: list[str] = []
    for line in (docker_ps_proc.stdout or "").splitlines():
        name_candidate = line.strip()
        if re.fullmatch(re.escape(prefix) + r"\d+", name_candidate):
            matches.append(name_candidate)

    return matches
