"""Classes used in CLP package integration tests."""

import argparse
from dataclasses import dataclass, field
from pathlib import Path

from tests.utils.classes import IntegrationTestExternalAction
from tests.utils.utils import (
    validate_dir_exists,
)


@dataclass
class ClpPackageTestPathConfig:
    """Docstring for ClpPackageTestPathConfig."""

    #: Default CLP package directory.
    clp_package_dir: Path

    #: Directory to store temporary package config files.
    temp_config_dir: Path = field(init=False)

    #: TODO: fill out the rest of the relevant paths.

    def __post_init__(self) -> None:
        """Validate directories."""
        validate_dir_exists(self.clp_package_dir)


@dataclass
class ClpPackage:
    """Docstring for ClpPackage."""

    # TODO: Merge `PackageModeConfig`, `PackageTestConfig`, and `PackageInstance` into this class.


@dataclass
class ClpPackageExternalAction(IntegrationTestExternalAction):
    """Docstring for ClpPackageExternalAction."""

    #: Parser to define semantics for the content of `IntegrationTestExternalAction.cmd`.
    args_parser: argparse.ArgumentParser

    def parse_args_from_cmd(self) -> argparse.Namespace:
        return self.args_parser.parse_args(self.cmd[1:])
