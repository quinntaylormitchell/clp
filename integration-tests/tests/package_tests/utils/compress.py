"""Functions to facilitate CLP package compression testing."""

import logging
from pathlib import Path

import pytest
from clp_py_utils.clp_config import StorageEngine

from tests.package_tests.classes import ClpPackage
from tests.package_tests.utils.decompress import decompress_clp_package
from tests.utils.classes import ClpAction, ClpVerificationResult, CmdArgs, SampleDataset
from tests.utils.fs_validation import is_dir_tree_content_equal
from tests.utils.utils import clear_directory

logger = logging.getLogger(__name__)


class CompressArgs(CmdArgs):
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
    dataset: SampleDataset,
) -> ClpAction:
    """Docstring."""
    log_msg = f"Compressing the '{dataset.dataset_name}' dataset."
    logger.info(log_msg)

    args: CompressArgs = _construct_compress_args(clp_package, dataset)
    return ClpAction.from_args(args)


def _construct_compress_args(clp_package: ClpPackage, dataset: SampleDataset) -> CompressArgs:
    """Docstring."""
    path_config = clp_package.path_config
    args = CompressArgs(
        script_path=path_config.compress_path,
        config=clp_package.temp_config_file_path,
        paths=[dataset.logs_path],
    )

    if clp_package.clp_config.package.storage_engine == StorageEngine.CLP_S:
        args.dataset = dataset.metadata.dataset_name
        args.timestamp_key = dataset.metadata.timestamp_key
        args.unstructured = dataset.metadata.unstructured

    return args


def verify_compress_action(
    compress_action: ClpAction,
    clp_package: ClpPackage,
    original_dataset: SampleDataset,
) -> ClpVerificationResult:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    returncode_result = compress_action.verify_returncode()
    if not returncode_result:
        return returncode_result

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
    compress_action: ClpAction, clp_package: ClpPackage
) -> ClpVerificationResult:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    returncode_result = compress_action.verify_returncode()
    if not returncode_result:
        return returncode_result

    # TODO: Waiting for PR 1299 (clp-json decompression) to be merged.
    return compress_action.pass_verification()


def _verify_compress_action_clp_text(
    compress_action: ClpAction,
    clp_package: ClpPackage,
    original_dataset: SampleDataset,
) -> ClpVerificationResult:
    """Docstring."""
    logger.info(f"Verifying {clp_package.mode_name} package compression.")
    returncode_result = compress_action.verify_returncode()
    if not returncode_result:
        return returncode_result

    # Decompress the contents of `clp-package/var/data/archives`.
    path_config = clp_package.path_config
    clear_directory(path_config.package_decompression_dir)

    decompress_action = decompress_clp_package(clp_package, path_config.package_decompression_dir)
    decompress_result = decompress_action.verify_returncode()
    if not decompress_result:
        return compress_action.fail_verification(
            "Supporting decompress action failed during compress verification.",
            supporting_action=decompress_action,
        )

    # Verify equality between original logs and decompressed logs.
    original_logs_path = original_dataset.logs_path
    decompressed_logs_path = path_config.package_decompression_dir / original_logs_path.relative_to(
        original_logs_path.anchor
    )

    equal = is_dir_tree_content_equal(original_logs_path, decompressed_logs_path)
    clear_directory(path_config.package_decompression_dir)
    if equal:
        return compress_action.pass_verification()

    return compress_action.fail_verification(
        f"Compress verification failure: mismatch between original logs at"
        f" '{original_logs_path}' and decompressed logs at '{decompressed_logs_path}'.",
    )
