"""Classes used in CLP binary integration tests."""

import argparse
from dataclasses import dataclass, field
from pathlib import Path

from tests.utils.classes import (
    IntegrationTestExternalAction,
    IntegrationTestPathConfig,
    static_path,
)
from tests.utils.utils import (
    validate_dir_exists,
)


@dataclass
class ClpBinaryTestPathConfig(IntegrationTestPathConfig):
    """Path configuration for CLP binary integration tests."""

    #: Default CLP binary directory.
    clp_core_bins_dir: Path

    def __post_init__(self) -> None:
        """
        Validates that the CLP core binaries directory exists and contains all required
        executables.
        """
        super().__post_init__()

        # Validate that init directory exists.
        validate_dir_exists(self.clp_core_bins_dir)

        # Validate all static path properties.
        self.validate_static_paths()

    @property
    @static_path
    def clp_binary_path(self) -> Path:
        """:return: The absolute path to the `clp` binary."""
        return self.clp_core_bins_dir / "clp"

    @property
    @static_path
    def clp_s_binary_path(self) -> Path:
        """:return: The absolute path to the `clp-s` binary."""
        return self.clp_core_bins_dir / "clp-s"


@dataclass
class ClpBinaryExternalAction(IntegrationTestExternalAction):
    """Metadata for an external action executed during a CLP binary integration test."""

    #: Parser to define semantics for the content of `cmd`.
    args_parser: argparse.ArgumentParser

    #: Namespace to hold information from `cmd`.
    parsed_args: argparse.Namespace = field(init=False)

    def __post_init__(self) -> None:
        """Parse command arguments."""
        self.parsed_args = self.args_parser.parse_args(self.cmd[1:])
