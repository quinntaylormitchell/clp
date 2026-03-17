"""Global pytest setup."""

import datetime
from collections.abc import Iterator, Sequence
from pathlib import Path

import pytest

from tests.utils.utils import resolve_path_env_var

# Define all fixtures globally so they are available without imports.
pytest_plugins = [
    "tests.fixtures.datasets",
    "tests.fixtures.path_configs",
    "tests.clp_binary_tests.integration_test_logs",
    "tests.clp_binary_tests.clp_binary_path_configs",
    "tests.clp_package_tests.fixtures",
]


BLUE = "\033[34m"
BOLD = "\033[1m"
RESET = "\033[0m"


class _UniqueTestLogDir:
    _path: Path | None = None

    @classmethod
    def set(cls, path: Path) -> None:
        cls._path = path

    @classmethod
    def get(cls) -> Path:
        if cls._path is None:
            err_msg = "test log directory has not been initialized"
            raise RuntimeError(err_msg)
        return cls._path


def get_test_log_dir() -> Path:
    """Returns the unique test log directory for this test run."""
    return _UniqueTestLogDir.get()


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    """Docstring."""
    now = datetime.datetime.now()  # noqa: DTZ005
    test_run_id = now.strftime("%Y-%m-%d-%H-%M-%S")

    path = (
        resolve_path_env_var("CLP_BUILD_DIR")
        / "integration_tests"
        / "test_logs"
        / f"testrun_{test_run_id}"
    )
    path.mkdir(parents=True, exist_ok=True)
    _UniqueTestLogDir.set(path)


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


@pytest.hookimpl(tryfirst=True)
def pytest_report_header(config: pytest.Config) -> str:  # noqa: ARG001
    """Docstring."""
    log_dir = _UniqueTestLogDir.get()
    return f"Log directory for this test run: {log_dir}"


@pytest.hookimpl()
def pytest_report_collectionfinish(
    config: pytest.Config,  # noqa: ARG001
    start_path: Path,  # noqa: ARG001
    items: Sequence[pytest.Item],
) -> str | list[str]:
    """Docstring."""
    report: str = ""
    if len(items) == 0:
        report = f"{BOLD}No tests match the specified parameters.{RESET}\n"
    else:
        report = f"{BOLD}The following tests will run:{RESET}\n"
        for item in items:
            report += f"\t{BOLD}{BLUE}{item.name}{RESET}\n"
        report += f"\n{BOLD}Running tests now...{RESET}"
    return report


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

    test_output_log_file = _UniqueTestLogDir.get() / "test_output.log"
    logging_plugin.set_log_path(str(test_output_log_file))
    yield


@pytest.hookimpl()
def pytest_itemcollected(item: pytest.Item) -> None:
    """
    Prettifies the name of the test for output purposes.

    :param item:
    """
    item._nodeid = f"{BOLD}{BLUE}{item.name}{RESET}"  # noqa: SLF001
