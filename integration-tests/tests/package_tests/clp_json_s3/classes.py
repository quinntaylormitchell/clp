"""Classes for clp-json-s3 testing."""

import logging
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import BaseModel

from tests.utils.utils import validate_dir_exists, validate_file_exists

DEFAULT_CMD_TIMEOUT_SECONDS = 120.0

logger = logging.getLogger(__name__)


class S3DatasetMetadata(BaseModel):
    """
    Metadata for an S3 dataset. All `<dataset_name>/metadata.json` files must conform to this
    schema.
    """

    dataset_name: str
    unstructured: bool
    timestamp_key: str | None
    begin_ts: int
    end_ts: int
    logs_subdir: str
    file_names: list[str]
    single_match_wildcard_query: str
    single_match_file: str
    columns_file_name: str | None = None


@dataclass
class S3Dataset:
    """Path layout and metadata storage for an S3 dataset."""

    #: Absolute path to the dataset root directory.
    dataset_root_dir: Path

    #: Pydantic model of metadata describing the dataset.
    metadata: S3DatasetMetadata = field(init=False)

    #: The name of the dataset (for logging purposes).
    dataset_name: str = field(init=False)

    def __post_init__(self) -> None:
        """Validates data members and loads metadata."""
        validate_dir_exists(self.dataset_root_dir)

        # Load metadata.
        validate_file_exists(self.metadata_file_path)
        raw_metadata = self.metadata_file_path.read_text()
        self.metadata = S3DatasetMetadata.model_validate_json(raw_metadata)

        # Set dataset name from metadata.
        self.dataset_name = self.metadata.dataset_name

        # Validate metadata properties.
        validate_dir_exists(self.logs_path)

        if self.metadata.begin_ts > self.metadata.end_ts:
            err_msg = (
                f"Dataset metadata failure: `begin_ts` '{self.metadata.begin_ts}' is larger than"
                f" `end_ts` '{self.metadata.end_ts}'"
            )
            raise ValueError(err_msg)

        for file_path in self.metadata.file_names:
            file_path_abs = self.logs_path / file_path
            validate_file_exists(file_path_abs)

        if self.metadata.single_match_file not in self.metadata.file_names:
            err_msg = (
                f"`single_match_file` '{self.metadata.single_match_file}' is not listed in"
                " `file_names`."
            )
            raise ValueError(err_msg)

    @property
    def metadata_file_path(self) -> Path:
        """:return: The absolute path to the file containing metadata for the dataset."""
        return self.dataset_root_dir / "metadata.json"

    @property
    def logs_path(self) -> Path:
        """:return: The absolute path to the logs directory."""
        return self.dataset_root_dir / self.metadata.logs_subdir
