"""Functions to facilitate CLP package compression testing."""

import logging
from pathlib import Path

import pytest
from clp_py_utils.clp_config import StorageEngine
from pydantic import BaseModel

from tests.clp_package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.clp_package_tests.utils.decompress import decompress_clp_package
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.logging_utils import format_action_failure_msg
from tests.utils.subprocess_utils import execute_external_action
from tests.utils.utils import (
    clear_directory,
    is_dir_tree_content_equal,
)

logger = logging.getLogger(__name__)


class CompressArgs(BaseModel):
    """Docstring."""

    script_path: Path
    config: Path
    dataset: str | None = None
    timestamp_key: str | None = None
    unstructured: bool = False
    paths: list[Path]

    def to_cmd(self) -> list[str]:
        """Docstring."""
        cmd: list[str] = [
            str(self.script_path),
            "--config",
            str(self.config),
        ]

        if self.dataset:
            cmd.append("--dataset")
            cmd.append(self.dataset)
        if self.timestamp_key:
            cmd.append("--timestamp-key")
            cmd.append(self.timestamp_key)
        if self.unstructured:
            cmd.append("--unstructured")

        cmd.extend([str(path) for path in self.paths])

        return cmd


def compress_clp_package(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
) -> ClpPackageExternalAction[CompressArgs]:
    """Docstring."""
    log_msg = f"Compressing the '{dataset.dataset_name}' dataset."
    logger.info(log_msg)

    args: CompressArgs = _construct_compress_args(clp_package, dataset)
    action: ClpPackageExternalAction[CompressArgs] = ClpPackageExternalAction(
        cmd=args.to_cmd(), args=args
    )
    execute_external_action(action)

    return action


def _construct_compress_args(
    clp_package: ClpPackage, dataset: IntegrationTestDataset
) -> CompressArgs:
    """Docstring."""
    path_config = clp_package.path_config
    args = CompressArgs(
        script_path=path_config.compress_path,
        config=clp_package.temp_config_file_path,
        paths=[dataset.path_to_dataset_logs],
    )

    if clp_package.clp_config.package.storage_engine == StorageEngine.CLP_S:
        args.dataset = dataset.metadata_dict["dataset"]
        args.timestamp_key = dataset.metadata_dict["timestamp_key"]
        args.unstructured = dataset.metadata_dict["data"]["unstructured"]

    return args


def verify_compress_action(
    compress_action: ClpPackageExternalAction[CompressArgs],
    clp_package: ClpPackage,
    original_dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    if compress_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The compress.sh subprocess returned a non-zero exit code.",
            compress_action,
        )

    mode = clp_package.mode_name
    match mode:
        case "clp-json" | "clp-presto":
            result = _verify_compress_action_clp_json(compress_action, clp_package)
        case "clp-text":
            result = _verify_compress_action_clp_text(
                compress_action, clp_package, original_dataset
            )
        case _:
            pytest.fail(f"Compression verification for the '{mode}' mode is not supported.")

    return result


def _verify_compress_action_clp_json(
    compress_action: ClpPackageExternalAction[CompressArgs], clp_package: ClpPackage
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    if compress_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The compress.sh subprocess returned a non-zero exit code.",
            compress_action,
        )

    # TODO: Waiting for PR 1299 (clp-json decompression) to be merged.
    return True, ""


def _verify_compress_action_clp_text(
    compress_action: ClpPackageExternalAction[CompressArgs],
    clp_package: ClpPackage,
    original_dataset: IntegrationTestDataset,
) -> tuple[bool, str]:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    if compress_action.completed_proc.returncode != 0:
        return format_action_failure_msg(
            "The 'compress.sh' subprocess returned a non-zero exit code.",
            compress_action,
        )

    # Decompress the contents of `clp-package/var/data/archives`.
    path_config = clp_package.path_config
    clear_directory(path_config.package_decompression_dir)

    decompress_action = decompress_clp_package(clp_package, path_config.package_decompression_dir)
    if decompress_action.completed_proc.returncode != 0:
        pytest.fail(
            "During compress action verification, internal decompress.sh command returned a"
            f"non-zero exit code. Subprocess log: {decompress_action.log_file_path}"
        )

    # Verify equality between original logs and decompressed logs.
    original_logs_path = original_dataset.path_to_dataset_logs
    decompressed_logs_path = path_config.package_decompression_dir / original_logs_path.relative_to(
        original_logs_path.anchor
    )

    equal = is_dir_tree_content_equal(original_logs_path, decompressed_logs_path)
    clear_directory(path_config.package_decompression_dir)
    if equal:
        return True, ""
    return format_action_failure_msg(
        f"Compress verification failure: mismatch between original logs at '{original_logs_path}'"
        f" and decompressed logs at '{decompressed_logs_path}'.",
        compress_action,
    )
