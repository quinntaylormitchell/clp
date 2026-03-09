"""Classes used in CLP package integration tests."""

import argparse
import re
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from clp_py_utils.clp_config import (
    ClpConfig,
)
from pydantic import ValidationError

from tests.utils.classes import IntegrationTestExternalAction, IntegrationTestPathConfig
from tests.utils.utils import (
    validate_dir_exists,
)


@dataclass
class ClpPackageTestPathConfig(IntegrationTestPathConfig):
    """Docstring for ClpPackageTestPathConfig."""

    #: Default CLP package directory.
    clp_package_dir: Path

    #: Directory to store temporary package config files.
    temp_config_dir: Path = field(init=False)

    #: Directory to store temporary decompressed files.
    package_decompression_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        """Validate directories."""
        # Ensure that the CLP package directory exists and that it is structured correctly.
        clp_package_dir = self.clp_package_dir
        validate_dir_exists(clp_package_dir)
        required_dirs = ["etc", "sbin"]
        missing_dirs = [d for d in required_dirs if not (clp_package_dir / d).is_dir()]
        if len(missing_dirs) > 0:
            err_msg = (
                f"CLP package at {clp_package_dir} is incomplete."
                f" Missing directories: {', '.join(missing_dirs)}"
            )
            # TODO: change this error to something else.
            # Maybe this check should go in package setup or something?
            raise RuntimeError(err_msg)

        # Create and set `temp_config_dir` within `test_cache_dir`.
        object.__setattr__(self, "temp_config_dir", self.test_cache_dir / "temp_configs")
        self.temp_config_dir.mkdir(parents=True, exist_ok=True)

        # Create and set `temp_config_dir` within `test_cache_dir`.
        object.__setattr__(
            self,
            "package_decompression_dir",
            self.test_cache_dir / "package_decompression",
        )
        self.package_decompression_dir.mkdir(parents=True, exist_ok=True)

    @property
    def archive_manager_path(self) -> Path:
        """:return: The absolute path to the package archive-manager script."""
        return self.clp_package_dir / "sbin" / "admin-tools" / "archive-manager.sh"

    @property
    def compress_from_s3_path(self) -> Path:
        """:return: The absolute path to the package compress-from-s3 script."""
        return self.clp_package_dir / "sbin" / "compress-from-s3.sh"

    @property
    def compress_path(self) -> Path:
        """:return: The absolute path to the package compress script."""
        return self.clp_package_dir / "sbin" / "compress.sh"

    @property
    def dataset_manager_path(self) -> Path:
        """:return: The absolute path to the package dataset-manager script."""
        return self.clp_package_dir / "sbin" / "admin-tools" / "dataset-manager.sh"

    @property
    def decompress_path(self) -> Path:
        """:return: The absolute path to the package decompress script."""
        return self.clp_package_dir / "sbin" / "decompress.sh"

    @property
    def search_path(self) -> Path:
        """:return: The absolute path to the package search script."""
        return self.clp_package_dir / "sbin" / "search.sh"

    @property
    def start_clp_path(self) -> Path:
        """:return: The absolute path to the package start-clp script."""
        return self.clp_package_dir / "sbin" / "start-clp.sh"

    @property
    def stop_clp_path(self) -> Path:
        """:return: The absolute path to the package stop-clp script."""
        return self.clp_package_dir / "sbin" / "stop-clp.sh"


@dataclass
class ClpPackageModeConfig:
    """Mode configuration for the CLP package."""

    #: Name of the package operation mode.
    mode_name: str

    #: The Pydantic representation of the package operation mode.
    clp_config: ClpConfig

    #: The list of CLP components that this package needs.
    component_list: tuple[str, ...]


@dataclass
class ClpPackage:
    """Docstring for ClpPackage."""

    # The `ClpPackageTestPathConfig` object for this CLP package.
    path_config: ClpPackageTestPathConfig

    #: Name of the CLP package operating mode.
    mode_name: str

    #: The Pydantic representation of this CLP package's operation mode.
    clp_config: ClpConfig

    #: The list of CLP components that this CLP package needs.
    component_list: tuple[str, ...]

    #: The instance ID of the running package.
    clp_instance_id: str = field(init=False, repr=True)

    #: The path to the .clp-config.yaml file constructed by the package during spin up.
    shared_config_file_path: Path = field(init=False, repr=True)

    def __post_init__(self) -> None:
        """Docstring."""
        # Validate clp_config.
        try:
            ClpConfig.model_validate(self.clp_config)
        except ValidationError as err:
            fail_msg = f"The `ClpConfig` pydantic object could not be validated: {err}"
            pytest.fail(fail_msg)

        # TODO: Set clp_instance_id from instance-id file.

        # TODO: Set shared_config_file_path and validate it exists.

    @property
    def temp_config_file_path(self) -> Path:
        """:return: The absolute path to the temporary configuration file for the package."""
        return self.path_config.temp_config_dir / f"clp-config-{self.mode_name}.yaml"

    @staticmethod
    def _get_clp_instance_id(clp_instance_id_file_path: Path) -> str:
        """
        Reads the CLP instance ID from the given file and validates its format.

        :param clp_instance_id_file_path:
        :return: The 4-character hexadecimal instance ID.
        :raise ValueError: If the file cannot be read or contents are not a 4-character hex string.
        """
        try:
            contents = clp_instance_id_file_path.read_text(encoding="utf-8").strip()
        except OSError as err:
            err_msg = f"Cannot read instance-id file '{clp_instance_id_file_path}'"
            raise ValueError(err_msg) from err

        if not re.fullmatch(r"[0-9a-fA-F]{4}", contents):
            err_msg = (
                f"Invalid instance ID in {clp_instance_id_file_path}: expected a 4-character"
                f" hexadecimal string, but read {contents}."
            )
            raise ValueError(err_msg)

        return contents

    @staticmethod
    def clear_archives() -> None:
        """Docstring."""
        # Use dataset-manager or archive-manager as appropriate.


@dataclass
class ClpPackageExternalAction(IntegrationTestExternalAction):
    """Docstring for ClpPackageExternalAction."""

    #: Parser to define semantics for the content of `IntegrationTestExternalAction.cmd`.
    args_parser: argparse.ArgumentParser

    #: Namespace to hold information from `IntegrationTestExternalAction.cmd`.
    parsed_args: argparse.Namespace = field(init=False)

    def __post_init__(self) -> None:
        """Parse command arguments."""
        self.parsed_args = self.args_parser.parse_args(self.cmd[1:])
