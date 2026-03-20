"""Classes used in clp-presto integration tests."""

import argparse
import logging
from dataclasses import dataclass, field
from pathlib import Path

from tests.clp_package_tests.utils.classes import ClpPackageTestPathConfig
from tests.utils.classes import IntegrationTestExternalAction
from tests.utils.utils import (
    validate_dir_exists,
    validate_file_exists,
)

logger = logging.getLogger(__name__)


@dataclass
class PrestoClusterTestPathConfig(ClpPackageTestPathConfig):
    """Docstring."""

    #: Default Presto deployment directory.
    presto_deployment_dir: Path

    def __post_init__(self) -> None:
        """Validate directories."""
        super().__post_init__()

        # Ensure that the Presto deployment directory exists and that it is structured correctly.
        presto_deployment_dir = self.presto_deployment_dir
        validate_dir_exists(presto_deployment_dir)
        validate_file_exists(self.set_up_config_path)
        validate_file_exists(self.split_filter_file_path)
        validate_file_exists(self.docker_compose_file_path)

    @property
    def set_up_config_path(self) -> Path:
        """:return: The absolute path to the Presto set-up-config script."""
        return self.presto_deployment_dir / "scripts" / "set-up-config.sh"

    @property
    def split_filter_file_path(self) -> Path:
        """:return: The absolute path to the Presto split-filter file."""
        return self.presto_deployment_dir / "coordinator" / "config-template" / "split-filter.json"

    @property
    def docker_compose_file_path(self) -> Path:
        """:return: The absolute path to the docker compose file for Presto deployment."""
        return self.presto_deployment_dir / "docker-compose.yaml"


@dataclass
class PrestoCluster:
    """Docstring."""

    # The `PrestoClusterTestPathConfig` object for this Presto cluster.
    path_config: PrestoClusterTestPathConfig


@dataclass
class PrestoClusterExternalAction(IntegrationTestExternalAction):
    """Docstring for PrestoClusterExternalAction."""

    #: Parser to define semantics for the content of `cmd`.
    args_parser: argparse.ArgumentParser

    #: Namespace to hold information from `cmd`.
    parsed_args: argparse.Namespace = field(init=False)

    def __post_init__(self) -> None:
        """Parse command arguments."""
        self.parsed_args = self.args_parser.parse_args(self.cmd[1:])
