"""Docstring."""

import logging

from clp_py_utils.clp_config import (
    ClpConfig,
    Package,
    QueryEngine,
    StorageEngine,
)

from tests.clp_package_tests.utils.classes import (
    ClpPackageModeConfig,
)
from tests.clp_package_tests.utils.modes import (
    CLP_BASE_COMPONENTS,
)

logger = logging.getLogger(__name__)

CLP_TEXT_MODE = ClpPackageModeConfig(
    mode_name="clp-text",
    clp_config=ClpConfig(
        package=Package(
            storage_engine=StorageEngine.CLP,
            query_engine=QueryEngine.CLP,
        ),
        api_server=None,
        log_ingestor=None,
    ),
    component_list=(*CLP_BASE_COMPONENTS,),
)
