"""Docstring."""

import logging

from clp_py_utils.clp_config import (
    ClpConfig,
    Package,
    Presto,
    QueryEngine,
    ResultsCache,
    StorageEngine,
)

from tests.package_tests.classes import (
    ClpPackageModeConfig,
)
from tests.package_tests.utils.modes import (
    CLP_API_SERVER_COMPONENT,
    CLP_BASE_COMPONENTS,
)

logger = logging.getLogger(__name__)

CLP_PRESTO_MODE = ClpPackageModeConfig(
    mode_name="clp-presto",
    clp_config=ClpConfig(
        package=Package(
            storage_engine=StorageEngine.CLP_S,
            query_engine=QueryEngine.PRESTO,
        ),
        results_cache=ResultsCache(
            retention_period=None,
        ),
        presto=Presto(
            host="localhost",
            port=8889,
        ),
    ),
    component_list=(
        *CLP_BASE_COMPONENTS,
        CLP_API_SERVER_COMPONENT,
    ),
)
