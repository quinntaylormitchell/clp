"""Session-scoped path configuration fixtures shared across binary integration tests."""

import pytest

from tests.binary_tests.config import (
    ClpCorePathConfig,
)
from tests.utils.utils import resolve_path_env_var


@pytest.fixture(scope="session")
def clp_core_path_config() -> ClpCorePathConfig:
    """Provides paths for the CLP core binaries."""
    return ClpCorePathConfig(clp_core_bins_dir=resolve_path_env_var("CLP_CORE_BINS_DIR"))
