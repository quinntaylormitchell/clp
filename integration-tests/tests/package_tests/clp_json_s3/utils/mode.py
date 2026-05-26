"""`ClpPackageModeConfig` for the clp-json-s3 package mode."""

import logging
import uuid
from pathlib import Path

import pytest
from _pytest.mark.structures import ParameterSet
from clp_py_utils.clp_config import (
    ArchiveOutput,
    ArchiveS3Storage,
    AwsAuthentication,
    AwsAuthType,
    ClpConfig,
    FsIngestionConfig,
    Package,
    QueryEngine,
    S3Config,
    S3Credentials,
    S3IngestionConfig,
    StorageEngine,
    StreamOutput,
    StreamS3Storage,
)

from tests.package_tests.classes import ClpPackageModeConfig
from tests.package_tests.utils.modes import (
    CLP_API_SERVER_COMPONENT,
    CLP_BASE_COMPONENTS,
    CLP_GARBAGE_COLLECTOR_COMPONENT,
    CLP_LOG_INGESTOR_COMPONENT,
    CLP_QUERY_COMPONENTS,
    CLP_REDUCER_COMPONENT,
    ClpMode,
)
from tests.utils.utils import load_yaml_to_dict

logger = logging.getLogger(__name__)


def _get_aws_authentication_obj() -> AwsAuthentication:
    return AwsAuthentication(
        type=AwsAuthType.credentials,
        profile=None,
        credentials=_get_s3_credentials_from_quinn_file(
            Path("/home/quinnmitchell/clp/quinn_s3_credentials.yaml")
        ),
    )


def _get_s3_credentials_from_quinn_file(cred_file_path: Path) -> S3Credentials:
    credentials_dict = load_yaml_to_dict(cred_file_path)
    return S3Credentials(
        access_key_id=credentials_dict["credentials"]["access_key_id"],
        secret_access_key=credentials_dict["credentials"]["secret_access_key"],
    )


SHARED_S3_CONFIG = S3Config(
    region_code="us-west-1",
    bucket="private-clp-test-bucket",
    key_prefix=f"integration_tests/{uuid.uuid4()}/",
    aws_authentication=_get_aws_authentication_obj(),
)

LOGS_INPUT_FS = FsIngestionConfig()
LOGS_INPUT_S3 = S3IngestionConfig(aws_authentication=_get_aws_authentication_obj())
ARCHIVE_OUTPUT_FS = ArchiveOutput()
ARCHIVE_OUTPUT_S3 = ArchiveOutput(storage=ArchiveS3Storage(s3_config=SHARED_S3_CONFIG))
STREAM_OUTPUT_FS = StreamOutput()
STREAM_OUTPUT_S3 = StreamOutput(storage=StreamS3Storage(s3_config=SHARED_S3_CONFIG))


def _logs_label(li: S3IngestionConfig | FsIngestionConfig) -> str:
    return "s3" if isinstance(li, S3IngestionConfig) else "fs"


def _output_label(o: ArchiveOutput | StreamOutput) -> str:
    return "s3" if isinstance(o.storage, (ArchiveS3Storage, StreamS3Storage)) else "fs"


def make_s3_mode_param(
    logs_input: S3IngestionConfig | FsIngestionConfig,
    archive_output: ArchiveOutput,
    stream_output: StreamOutput,
) -> ParameterSet:
    """
    Returns a `ParameterSet` carrying a ClpPackageModeConfig object set up with some unique
    configuration of `log_input`, `archive_output`, and `stream_output`. ParameterSet.id is set to
    a string that describes the configuration.
    """
    components: tuple[str, ...] = (
        *CLP_BASE_COMPONENTS,
        CLP_REDUCER_COMPONENT,
        *CLP_QUERY_COMPONENTS,
        CLP_GARBAGE_COLLECTOR_COMPONENT,
        CLP_API_SERVER_COMPONENT,
    )
    if isinstance(logs_input, S3IngestionConfig):
        components = (*components, CLP_LOG_INGESTOR_COMPONENT)

    return pytest.param(
        ClpPackageModeConfig(
            mode_name=ClpMode.JSON_S3,
            clp_config=ClpConfig(
                package=Package(
                    storage_engine=StorageEngine.CLP_S,
                    query_engine=QueryEngine.CLP_S,
                ),
                logs_input=logs_input,
                archive_output=archive_output,
                stream_output=stream_output,
            ),
            component_list=components,
        ),
        id=(
            f"logs-{_logs_label(logs_input)}_"
            f"archive-{_output_label(archive_output)}_"
            f"stream-{_output_label(stream_output)}"
        ),
    )


ALL_S3_MODE_PARAMS = [
    make_s3_mode_param(LOGS_INPUT_FS, ARCHIVE_OUTPUT_FS, STREAM_OUTPUT_S3),
    make_s3_mode_param(LOGS_INPUT_FS, ARCHIVE_OUTPUT_S3, STREAM_OUTPUT_FS),
    make_s3_mode_param(LOGS_INPUT_FS, ARCHIVE_OUTPUT_S3, STREAM_OUTPUT_S3),
    make_s3_mode_param(LOGS_INPUT_S3, ARCHIVE_OUTPUT_FS, STREAM_OUTPUT_FS),
    make_s3_mode_param(LOGS_INPUT_S3, ARCHIVE_OUTPUT_FS, STREAM_OUTPUT_S3),
    make_s3_mode_param(LOGS_INPUT_S3, ARCHIVE_OUTPUT_S3, STREAM_OUTPUT_FS),
    make_s3_mode_param(LOGS_INPUT_S3, ARCHIVE_OUTPUT_S3, STREAM_OUTPUT_S3),
]
