"""Classes used in CLP integration tests."""

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.utils.utils import (
    validate_dir_exists,
)


@dataclass
class NEWIntegrationTestPathConfig:
    """Path configuration for CLP integration tests."""

    #: Default CLP build directory.
    clp_build_dir: Path

    #: Default integration test project root directory.
    integration_tests_project_root: Path

    #: The cache directory for integration tests.
    test_cache_dir: Path = field(init=False)

    #: The log directory for integration tests.
    test_log_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        """Verify and validate directory structure."""
        # Validate that init directories exist.
        validate_dir_exists(self.clp_build_dir)
        validate_dir_exists(self.integration_tests_project_root)

        # Create and set `test_cache_dir` within `clp_build_dir`.
        object.__setattr__(self, "test_cache_dir", self.clp_build_dir / "integration_tests")
        self.test_cache_dir.mkdir(parents=True, exist_ok=True)

        # Create and set `test_log_dir` within `test_cache_dir`.
        object.__setattr__(self, "test_log_dir", self.test_cache_dir / "test_logs")
        self.test_log_dir.mkdir(parents=True, exist_ok=True)

    @property
    def test_data_path(self) -> Path:
        """:return: The absolute path to the sample dataset directory."""
        return self.integration_tests_project_root / "tests" / "data"


@dataclass
class IntegrationTestDataset:
    """Metadata for a sample dataset stored in `clp/integration-tests/tests/data`."""

    #: The name of the dataset (for logging purposes).
    dataset_name: str

    #: A dictionary of metadata describing the dataset.
    metadata_dict: dict[str, Any]

    def __post_init__(self) -> None:
        # TODO: load in the metadata dictionary
        # Should each dataset have a fixture maybe?
        pass


@dataclass
class IntegrationTestExternalAction:
    """Metadata for an external action executed with `subprocess.run()` during an integration test."""

    #: Command to pass to `subprocess.run()`.
    cmd: list[str]

    #: The completed process returned from `subprocess.run()`.
    completed_proc: subprocess.CompletedProcess[str] = field(init=False)
