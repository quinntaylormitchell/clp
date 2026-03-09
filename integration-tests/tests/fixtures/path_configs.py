"""Session-scoped path configuration fixtures shared across integration tests."""

import pytest

from tests.utils.classes import IntegrationTestPathConfig
from tests.utils.utils import resolve_path_env_var


@pytest.fixture(scope="session")
def integration_test_path_config() -> IntegrationTestPathConfig:
    """Provides paths relevant to all integration tests."""
    return IntegrationTestPathConfig(
        clp_build_dir=resolve_path_env_var("CLP_BUILD_DIR"),
        integration_tests_project_root=resolve_path_env_var("INTEGRATION_TESTS_PROJECT_ROOT"),
    )
