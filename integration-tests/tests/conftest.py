"""Global pytest setup."""

import datetime
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.utils.utils import resolve_path_env_var

# Make the fixtures defined in `tests/fixtures/` globally available without imports.
pytest_plugins = [
    "tests.fixtures.datasets",
    "tests.fixtures.path_configs",
    "tests.clp_binary_tests.integration_test_logs",
    "tests.clp_binary_tests.clp_binary_path_configs",
    "tests.clp_package_tests.clp_package_fixtures",
]


BLUE = "\033[34m"
BOLD = "\033[1m"
RESET = "\033[0m"


unique_testlog_dir: Path | None = None


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    """Docstring."""
    global unique_testlog_dir  # noqa: PLW0603

    now = datetime.datetime.now()  # noqa: DTZ005
    test_run_id = now.strftime("%Y-%m-%d-%H-%M-%S")

    unique_testlog_dir = (
        resolve_path_env_var("CLP_BUILD_DIR")
        / "integration_tests"
        / "test_logs"
        / f"testrun_{test_run_id}"
    )

    unique_testlog_dir.mkdir(parents=True, exist_ok=True)


def get_test_log_dir() -> Path:
    """Docstring."""
    if unique_testlog_dir is None:
        err_msg = "test log directory has not been initialized"
        raise RuntimeError(err_msg)

    return unique_testlog_dir


@pytest.hookimpl(tryfirst=True)
def pytest_report_header(config: pytest.Config) -> str:  # noqa: ARG001
    """Docstring."""
    log_dir = get_test_log_dir()
    return f"Log directory for this test run: '{log_dir}'"


@pytest.hookimpl()
def pytest_addoption(parser: pytest.Parser) -> None:
    """
    Adds options for `pytest`.

    :param parser:
    """
    parser.addoption(
        "--base-port",
        action="store",
        default="56000",
        help="Base port for CLP package integration tests.",
    )


def pytest_itemcollected(item: pytest.Item) -> None:
    """
    Prettifies the name of the test for output purposes.

    :param item:
    """
    item._nodeid = f"{BOLD}{BLUE}{item.nodeid}{RESET}"  # noqa: SLF001


@pytest.hookimpl(wrapper=True)
def pytest_runtest_setup(item: pytest.Item) -> Iterator[None]:
    """
    Sets and stores the test_output log file.

    :param item:
    """
    config = item.config
    logging_plugin = config.pluginmanager.get_plugin("logging-plugin")
    if logging_plugin is None:
        err_msg = "Expected pytest plugin 'logging-plugin' to be registered."
        raise RuntimeError(err_msg)

    test_output_log_file = get_test_log_dir() / "test_output.log"
    logging_plugin.set_log_path(str(test_output_log_file))
    yield
