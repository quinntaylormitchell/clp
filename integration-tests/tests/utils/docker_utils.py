"""Provide utility functions related to the use of Docker during integration tests."""

import re

from tests.utils.classes import NonClpAction
from tests.utils.utils import get_binary_path


def list_running_services_in_compose_project(project_name: str) -> list[str]:
    """
    Lists running Docker services that belong to the given Docker Compose project.

    :param project_name:
    :return: List of the running services that belong to the specified Docker Compose project.
    :raise RuntimeError: if `docker compose ps` returns a non-zero exit code.
    :raise RuntimeError: if `docker compose ps` returns a non-zero exit code.
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
    compose_ps_action = NonClpAction(cmd=compose_ps_cmd)
    compose_ps_action.check_returncode()

    service_names: list[str] = []
    for line in compose_ps_action.completed_proc.stdout.splitlines():
        service_name_candidate = line.strip()
        if service_name_candidate:
            service_names.append(service_name_candidate)

    return service_names


def list_running_containers_with_prefix(prefix: str) -> list[str]:
    """
    Lists running Docker containers whose names begin with `prefix` and end with one or more digits.

    :param prefix:
    :return: List of running container names that match the pattern.
    :raise RuntimeError: if `docker ps` returns a non-zero exit code.
    """
    docker_ps_cmd = [
        get_binary_path("docker"),
        "ps",
        "--format",
        "{{.Names}}",
        "--filter",
        f"name={prefix}",
    ]
    docker_ps_action = NonClpAction(cmd=docker_ps_cmd)
    docker_ps_action.check_returncode()

    matches: list[str] = []
    for line in docker_ps_action.completed_proc.stdout.splitlines():
        name_candidate = line.strip()
        if re.fullmatch(re.escape(prefix) + r"\d+", name_candidate):
            matches.append(name_candidate)

    return matches
