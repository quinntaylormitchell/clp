"""Functions to facilitate CLP package S3 compression testing."""

import logging
from pathlib import Path

from clp_py_utils.clp_config import (
    ArchiveS3Storage,
)

from tests.package_tests.classes import ClpPackage
from tests.package_tests.clp_json_s3.classes import S3Dataset
from tests.utils.classes import ClpAction, ClpVerificationResult, CmdArgs, SampleDataset

logger = logging.getLogger(__name__)


class CompressS3Args(CmdArgs):
    """Structured arguments for invoking the clp-json-s3 compression script."""

    script_path: Path
    config: Path
    dataset: str | None = None
    timestamp_key: str | None = None
    unstructured: bool = False
    subcommand: str
    inputs: list[str]

    def to_cmd(self) -> list[str]:
        """:return: The command-line invocation built from this argument set."""
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

        cmd.append(self.subcommand)
        cmd.extend(self.inputs)

        return cmd


def compress_s3_clp_package(
    clp_package: ClpPackage,
    dataset: S3Dataset,
) -> ClpAction:
    """
    Compresses `dataset` into `clp_package` by invoking the clp-json-s3 compression script with the
    arguments required to ingest the dataset from S3.

    :param clp_package:
    :param dataset:
    :return: The launched `ClpAction` representing the compression subprocess.
    """
    log_msg = f"Compressing the '{dataset.dataset_name}' dataset with S3."
    logger.info(log_msg)

    args: CompressS3Args = _construct_compress_s3_args(clp_package, dataset)
    return ClpAction.from_args(args)


def _construct_compress_s3_args(clp_package: ClpPackage, dataset: S3Dataset) -> CompressS3Args:
    """
    :param clp_package:
    :param dataset:
    :return: `CompressS3Args` populated from `clp_package` and `dataset` to compress the dataset
        from its S3 key prefix.
    """
    path_config = clp_package.path_config
    metadata = dataset.metadata
    return CompressS3Args(
        script_path=path_config.compress_from_s3_path,
        config=clp_package.temp_config_file_path,
        dataset=metadata.dataset_name,
        timestamp_key=metadata.timestamp_key,
        unstructured=metadata.unstructured,
        subcommand="s3-key-prefix",
        inputs=[
            "https://private-clp-test-bucket.s3.us-west-1.amazonaws.com/integration_tests/s3_datasets/json_s3_multifile/"
        ],
    )


def verify_compress_s3_action(
    action: ClpAction,
    clp_package: ClpPackage,
    original_dataset: SampleDataset | S3Dataset,
) -> ClpVerificationResult:
    """
    Verifies the S3 compression action.

    :param action:
    :param clp_package:
    :param original_dataset:
    :return: A `ClpVerificationResult` indicating the success or failure of the verification.
    """
    # TODO: log the specific subconfiguration for compression
    logger.info("Verifying '%s' package S3 compression.", clp_package.mode_name)

    if isinstance(clp_package.clp_config.archive_output.storage, ArchiveS3Storage):
        pass

    returncode_result = action.verify_returncode()
    if not returncode_result:
        return returncode_result

    # TODO: remove
    assert original_dataset

    return action.pass_verification()
