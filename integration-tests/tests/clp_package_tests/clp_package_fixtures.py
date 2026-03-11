"""Session-scoped path configuration fixtures shared across package integration tests."""

import logging
from collections.abc import Iterator

import pytest

from tests.clp_package_tests.clp_package_utils.actions import (
    run_start_clp_cmd,
    run_stop_clp_cmd,
)
from tests.clp_package_tests.clp_package_utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
    ClpPackageTestPathConfig,
)
from tests.clp_package_tests.clp_package_utils.verification import (
    verify_start_clp_action,
    verify_stop_clp_action,
)
from tests.utils.port_utils import assign_ports_from_base
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


@pytest.fixture(scope="package")
def clp_package(
    request: pytest.FixtureRequest,
    clp_package_test_path_config: ClpPackageTestPathConfig,
) -> Iterator[ClpPackage]:
    """Docstring for clp_package fixture."""
    mode_config = request.param
    path_config = clp_package_test_path_config
    mode_name = mode_config.mode_name
    clp_config = mode_config.clp_config
    component_list = mode_config.component_list

    # Assign ports to the `ClpConfig` pydantic object.
    logger.info(f"Assigning ports to the '{mode_name}' package.")
    base_port_string = request.config.getoption("--base-port")
    try:
        base_port = int(base_port_string)
    except ValueError as err:
        err_msg = f"Invalid value '{base_port_string}' for '--base-port'; expected an integer."
        raise ValueError(err_msg) from err
    assign_ports_from_base(base_port, clp_config)

    # Write the temporary config file.
    logger.info(f"Writing the temporary config file for the '{mode_name}' package.")
    temp_config_file_path = path_config.temp_config_dir / f"clp-config-{mode_name}.yaml"
    write_dict_to_yaml(clp_config.dump_to_primitive_dict(), temp_config_file_path)

    # Construct `ClpPackage` object.
    clp_package = ClpPackage(
        path_config=path_config,
        mode_name=mode_name,
        clp_config=clp_config,
        component_list=component_list,
    )

    try:
        # Start the CLP package with start-clp.sh.
        start_clp_cmd: list[str] = [
            str(path_config.start_clp_path),
            "--config",
            str(temp_config_file_path),
        ]
        start_clp_action: ClpPackageExternalAction = run_start_clp_cmd(start_clp_cmd)

        # Verify start.
        start_clp_action_verified, failure_message = verify_start_clp_action(
            start_clp_action, clp_package
        )
        assert start_clp_action_verified, failure_message

        yield clp_package
    finally:
        # Stop the CLP package with stop-clp.sh.
        stop_clp_cmd: list[str] = [
            str(path_config.stop_clp_path),
            "--config",
            str(temp_config_file_path),
        ]
        stop_clp_action: ClpPackageExternalAction = run_stop_clp_cmd(stop_clp_cmd)

        # Verify stop.s
        stop_clp_action_verified, failure_message = verify_stop_clp_action(
            stop_clp_action, clp_package
        )
        assert stop_clp_action_verified, failure_message

        # Clean up anything in the int-tests folder that was created in this fixture.
        clp_package.temp_config_file_path.unlink(missing_ok=True)
