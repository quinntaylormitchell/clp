"""Functions to facilitate CLP package decompression testing."""

import logging
from typing import Any

from clp_package_utils.general import EXTRACT_FILE_CMD

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.utils.subprocess_utils import execute_external_action

logger = logging.getLogger(__name__)


def decompress_clp_package(
    clp_package: ClpPackage,
    extraction_dir: Any,
    paths: list[str] | None = None,
) -> ClpPackageExternalAction:
    """Docstring."""
    logger.info(f"Decompressing '{clp_package.mode_name}' package.")

    arg_dict: dict[str, Any] = construct_decompress_arg_dict(clp_package, extraction_dir, paths)
    decompress_action = ClpPackageExternalAction(
        cmd=construct_decompress_cmd(arg_dict), arg_dict=arg_dict
    )
    execute_external_action(decompress_action)

    return decompress_action


def construct_decompress_arg_dict(
    clp_package: ClpPackage,
    extraction_dir: Any,
    paths: list[str] | None = None,
) -> dict[str, Any]:
    """Docstring."""
    path_config = clp_package.path_config

    arg_dict: dict[str, Any] = {
        "script_path": path_config.decompress_path,
        "config": clp_package.temp_config_file_path,
        "extraction_dir": extraction_dir,
    }

    if paths:
        arg_dict["paths"] = paths

    return arg_dict


def construct_decompress_cmd(arg_dict: dict[str, Any]) -> list[str]:
    """Docstring."""
    decompress_cmd: list[str] = [
        str(arg_dict["script_path"]),
        "--config",
        str(arg_dict["config"]),
        EXTRACT_FILE_CMD,
        "--extraction-dir",
        str(arg_dict["extraction_dir"]),
    ]

    if "paths" in arg_dict:
        decompress_cmd.extend(arg_dict["paths"])

    return decompress_cmd
