"""Tests for the clp-text package."""

import pytest
from clp_py_utils.clp_config import (
    ClpConfig,
    Package,
    QueryEngine,
    StorageEngine,
)

from tests.utils.asserting_utils import (
    validate_package_instance,
    verify_package_compression,
)
from tests.utils.clp_mode_utils import CLP_BASE_COMPONENTS
from tests.utils.config import PackageCompressionJob, PackageInstance, PackageModeConfig
from tests.utils.package_utils import run_package_compression_script

# Mode description for this module.
CLP_TEXT_MODE = PackageModeConfig(
    mode_name="clp-text",
    clp_config=ClpConfig(
        package=Package(
            storage_engine=StorageEngine.CLP,
            query_engine=QueryEngine.CLP,
        ),
        api_server=None,
        log_ingestor=None,
    ),
    component_list=(*CLP_BASE_COMPONENTS,),
)


# Pytest markers for this module.
pytestmark = [
    pytest.mark.package,
    pytest.mark.clp_text,
    pytest.mark.parametrize(
        "fixt_package_test_config", [CLP_TEXT_MODE], indirect=True, ids=[CLP_TEXT_MODE.mode_name]
    ),
]


@pytest.mark.startup
def test_clp_text_startup(fixt_package_instance: PackageInstance) -> None:
    """
    Validates that the `clp-text` package starts up successfully.

    :param fixt_package_instance:
    """
    validate_package_instance(fixt_package_instance)


@pytest.mark.compression
def test_clp_text_compression_text_multifile(
    request: pytest.FixtureRequest, fixt_package_instance: PackageInstance
) -> None:
    """
    Validate that the `clp-text` package successfully compresses the `text_multifile` dataset.

    :param fixt_package_instance:
    """
    validate_package_instance(fixt_package_instance)

    # Clear archives before compressing.
    package_test_config = fixt_package_instance.package_test_config
    package_path_config = package_test_config.path_config
    package_path_config.clear_package_archives()

    # Compress a dataset.
    compression_job = PackageCompressionJob(
        sample_dataset_name="text_multifile",
        options=None,
        path_to_original_dataset=(
            package_path_config.clp_text_test_data_path / "text_multifile" / "logs"
        ),
        begin_ts_ms=0,
        end_ts_ms=0,
    )
    run_package_compression_script(request, compression_job, package_test_config)

    # Check the correctness of compression.
    verify_package_compression(request, compression_job, package_test_config)

    # Clear archives.
    package_path_config.clear_package_archives()
