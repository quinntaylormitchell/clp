"""Session-scoped path configuration fixtures shared across binary integration tests."""

import pytest

from tests.binary_tests.classes import ClpBinaryTestPathConfig
from tests.utils.utils import resolve_path_env_var


@pytest.fixture(scope="session")
def clp_binary_test_path_config() -> ClpBinaryTestPathConfig:
    """Provides paths for the CLP core binaries."""
    return ClpBinaryTestPathConfig(
        clp_build_dir=resolve_path_env_var("CLP_BUILD_DIR"),
        integration_tests_project_root=resolve_path_env_var("INTEGRATION_TESTS_PROJECT_ROOT"),
        clp_core_bins_dir=resolve_path_env_var("CLP_CORE_BINS_DIR"),
    )
