"""Utilities for logging during the test run."""

import datetime
import logging
from pathlib import Path

from tests.conftest import get_test_log_dir
from tests.utils.classes import IntegrationTestExternalAction

logger = logging.getLogger(__name__)


def construct_log_err_msg(err_msg: str) -> str:
    """
    Append a signal that directs readers to the test output log file.

    :param err_msg: The base error message onto which the signal will be appended.
    :return: An error message that directs readers to look in the test output log file.

    """
    return (
        err_msg
        + " Check the full test output log for more information (see test header for file path)."
    )


def log_subprocess_output_to_file(subprocess: IntegrationTestExternalAction) -> None:
    """Docstring."""
    now = datetime.datetime.now()  # noqa: DTZ005
    test_run_id = now.strftime("%Y-%m-%d-%H-%M-%S")
    subprocess_output_file_path = (
        get_test_log_dir()
        / "subprocess_output"
        / f"{Path(subprocess.cmd[0]).name}_{test_run_id}.log"
    )
    subprocess_output_file_path.parent.mkdir(parents=True, exist_ok=True)

    completed_proc = subprocess.completed_proc

    stdout_content = completed_proc.stdout or "(empty)"
    stderr_content = completed_proc.stderr or "(empty)"

    # Ensure both end with a newline
    if not stdout_content.endswith("\n"):
        stdout_content += "\n"
    if not stderr_content.endswith("\n"):
        stderr_content += "\n"

    sep = "-" * 32
    lines = [
        "SUBPROCESS RUN SUMMARY\n",
        f"{sep}\n",
        f"Timestamp at completion : {now.strftime('%Y-%m-%d %H:%M:%S')}\n",
        f"Command                 : {completed_proc.args}\n",
        f"Return Code             : {completed_proc.returncode}\n",
        "\n\n",
        "captured stdout\n",
        f"{sep}\n",
        stdout_content,
        "\n",
        "\n\n",
        "captured stderr\n",
        f"{sep}\n",
        stderr_content,
        "\n",
    ]

    with subprocess_output_file_path.open("w", encoding="utf-8") as log_file:
        log_file.writelines(lines)

    logger.info(
        f"Subprocess completed. stdout and stderr written to '{subprocess_output_file_path}'"
    )
