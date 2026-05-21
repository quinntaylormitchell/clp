"""Session-scoped path configuration fixtures shared across package integration tests."""

import logging
from collections.abc import Iterator

import pytest

from tests.package_tests.classes import (
    ClpPackage,
    ClpPackageModeConfig,
    ClpPackageTestPathConfig,
)
from tests.package_tests.clp_json.utils.clear_archives import clear_package_archives_clp_json
from tests.package_tests.clp_text.utils.clear_archives import clear_package_archives_clp_text
from tests.package_tests.utils.modes import ClpMode
from tests.package_tests.utils.port_assignment import assign_ports_from_base
from tests.package_tests.utils.start_stop import (
    start_clp_package,
    stop_clp_package,
    verify_start_clp_action,
    verify_stop_clp_action,
)
from tests.utils.classes import ClpAction  # noqa: TC001
from tests.utils.utils import resolve_path_env_var, write_dict_to_yaml

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def clp_package_test_path_config() -> ClpPackageTestPathConfig:
    """Provides paths relevant to all package integration tests."""
    return ClpPackageTestPathConfig(
        clp_build_dir=resolve_path_env_var("CLP_BUILD_DIR"),
        integration_tests_project_root=resolve_path_env_var("INTEGRATION_TESTS_PROJECT_ROOT"),
        clp_package_dir=resolve_path_env_var("CLP_PACKAGE_DIR"),
    )


@pytest.fixture(scope="module")
def clp_package(
    request: pytest.FixtureRequest,
    clp_package_test_path_config: ClpPackageTestPathConfig,
) -> Iterator[ClpPackage]:
    """
    Creates and maintains a module-level `ClpPackage` for a specific CLP mode, starts the package
    instance, and stops it during teardown. For efficiency, group all tests for a given mode in the
    same module.

    :param request: Provides `ClpPackageModeConfig` via `request.param`.
    :param clp_package_test_path_config:
    :return: An iterator that yields the running `ClpPackage` for the specified mode.
    :raise ValueError: if the CLP base port's value is invalid.
    """
    mode_config: ClpPackageModeConfig = request.param
    mode_name = mode_config.mode_name
    clp_config = mode_config.clp_config
    component_list = mode_config.component_list

    # Assign ports to the `ClpConfig` pydantic object.
    logger.info("Assigning ports to the '%s' package.", mode_name)
    base_port_string = request.config.getoption("--base-port")
    try:
        base_port = int(base_port_string)
    except ValueError as err:
        err_msg = f"Invalid value '{base_port_string}' for '--base-port'; expected an integer."
        raise ValueError(err_msg) from err
    assign_ports_from_base(base_port, clp_config)

    # Construct `ClpPackage` object.
    clp_package = ClpPackage(
        path_config=clp_package_test_path_config,
        mode_name=mode_name,
        clp_config=clp_config,
        component_list=component_list,
    )

    # Write the temporary config file.
    logger.info("Writing the temporary config file for the '%s' package.", mode_name)
    write_dict_to_yaml(
        clp_config.dump_to_primitive_dict(),  # type: ignore[no-untyped-call]
        clp_package.temp_config_file_path,
    )

    try:
        start_clp_action: ClpAction = start_clp_package(clp_package)
        start_result = verify_start_clp_action(start_clp_action, clp_package)
        assert start_result, start_result.failure_message
        yield clp_package
    finally:
        stop_clp_action: ClpAction = stop_clp_package(clp_package)
        stop_result = verify_stop_clp_action(stop_clp_action, clp_package)
        assert stop_result, stop_result.failure_message

        clp_package.temp_config_file_path.unlink(missing_ok=True)


@pytest.fixture
def clear_package_archives(clp_package: ClpPackage) -> Iterator[None]:
    """
    Clears all archives from the `clp_package` at the beginning and end of each test that uses the
    fixture. Archives are cleared using a method appropriate to the type of CLP package being used.

    :param clp_package:
    """
    match clp_package.mode_name:
        case ClpMode.JSON | ClpMode.JSON_S3 | ClpMode.PRESTO:
            clear = clear_package_archives_clp_json
        case ClpMode.TEXT:
            clear = clear_package_archives_clp_text
        case _:
            pytest.fail(f"Unrecognized CLP package mode name: '{clp_package.mode_name}'")

    clear(clp_package)
    yield
    clear(clp_package)
