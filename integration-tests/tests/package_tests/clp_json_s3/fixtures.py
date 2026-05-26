"""Session-scoped fixtures used in clp-json-s3 integration tests."""

import pytest

from tests.package_tests.clp_json_s3.classes import S3Dataset
from tests.utils.classes import IntegrationTestPathConfig


@pytest.fixture(scope="session")
def json_s3_multifile(
    integration_test_path_config: IntegrationTestPathConfig,
) -> S3Dataset:
    """Returns an object corresponding to the `json_s3_multifile` test dataset."""
    return S3Dataset(
        dataset_root_dir=integration_test_path_config.test_data_dir / "json_s3_multifile",
    )
