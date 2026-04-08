"""Functions to facilitate CLP package S3 compression testing."""

import logging
from pathlib import Path

from pydantic import BaseModel

from tests.package_tests.utils.classes import (
    ClpPackage,
    ClpPackageExternalAction,
)
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.subprocess_utils import execute_external_action

logger = logging.getLogger(__name__)


class CompressS3Args(BaseModel):
    """Docstring."""

    script_path: Path
    config: Path
    dataset: str | None = None
    timestamp_key: str | None = None
    unstructured: bool = False
    subcommand: str
    inputs: list[str]

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

        cmd.append(self.subcommand)
        cmd.extend(self.inputs)

        return cmd


def compress_s3_clp_package(
    clp_package: ClpPackage,
    dataset: IntegrationTestDataset,
) -> ClpPackageExternalAction[CompressS3Args]:
    """Docstring."""
    log_msg = f"Compressing the '{dataset.dataset_name}' dataset with S3."
    logger.info(log_msg)

    args: CompressS3Args = _construct_compress_s3_args(clp_package, dataset)
    action: ClpPackageExternalAction[CompressS3Args] = ClpPackageExternalAction(
        cmd=args.to_cmd(), args=args
    )
    execute_external_action(action)

    return action


def _construct_compress_s3_args(
    clp_package: ClpPackage, dataset: IntegrationTestDataset
) -> CompressS3Args:
    """Docstring."""
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
            "https://private-clp-test-bucket.s3.us-west-1.amazonaws.com/integration_tests/permanent/json_s3_multifile/"
        ],
    )
