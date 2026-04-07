"""Docstring."""

import logging
import uuid
from pathlib import Path

from clp_py_utils.clp_config import (
    ArchiveOutput,
    ArchiveS3Storage,
    AwsAuthentication,
    AwsAuthType,
    ClpConfig,
    Package,
    QueryEngine,
    S3Config,
    S3Credentials,
    S3IngestionConfig,
    StorageEngine,
)

from tests.package_tests.utils.classes import (
    ClpPackageModeConfig,
)
from tests.package_tests.utils.modes import (
    CLP_API_SERVER_COMPONENT,
    CLP_BASE_COMPONENTS,
    CLP_GARBAGE_COLLECTOR_COMPONENT,
    CLP_LOG_INGESTOR_COMPONENT,
    CLP_QUERY_COMPONENTS,
    CLP_REDUCER_COMPONENT,
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


CLP_JSON_S3_MODE = ClpPackageModeConfig(
    mode_name="clp-json-s3",
    clp_config=ClpConfig(
        package=Package(
            storage_engine=StorageEngine.CLP_S,
            query_engine=QueryEngine.CLP_S,
        ),
        logs_input=S3IngestionConfig(
            aws_authentication=_get_aws_authentication_obj(),
        ),
        archive_output=ArchiveOutput(
            storage=ArchiveS3Storage(
                s3_config=S3Config(
                    region_code="us-west-1",
                    bucket="private-clp-test-bucket",
                    key_prefix=f"integration_tests/testrun_{uuid.uuid4()}/",
                    aws_authentication=_get_aws_authentication_obj(),
                ),
            ),
        ),
    ),
    component_list=(
        *CLP_BASE_COMPONENTS,
        CLP_REDUCER_COMPONENT,
        *CLP_QUERY_COMPONENTS,
        CLP_GARBAGE_COLLECTOR_COMPONENT,
        CLP_API_SERVER_COMPONENT,
        CLP_LOG_INGESTOR_COMPONENT,
    ),
)
