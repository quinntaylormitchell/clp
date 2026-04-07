"""Utilities that facilitate split filters for a Presto cluster."""

import json
import logging
from pathlib import Path
from typing import Any

from tests.package_tests.clp_presto.utils.classes import (
    PrestoCluster,
)
from tests.utils.classes import (
    IntegrationTestDataset,
)
from tests.utils.utils import load_json_to_dict

logger = logging.getLogger(__name__)


def add_split_filter_for_dataset(
    dataset: IntegrationTestDataset, presto_cluster: PrestoCluster
) -> None:
    """Add a split filter to the split filter file for the given dataset."""
    log_msg = f"Adding a split filter for the '{dataset.dataset_name}' dataset."
    logger.info(log_msg)

    path_config = presto_cluster.path_config
    split_filter_file_path = path_config.split_filter_file_path
    existing_filters = load_json_to_dict(split_filter_file_path)
    new_filter = _construct_split_filter_for_dataset(dataset)
    existing_filters.update(new_filter)
    _write_split_filters_json(existing_filters, split_filter_file_path)


def clear_split_filter_file(presto_cluster: PrestoCluster) -> None:
    """Docstring."""
    log_msg = "Clearing split-filter.json of all filters."
    logger.info(log_msg)

    split_filter_file_path = presto_cluster.path_config.split_filter_file_path
    empty_json: dict[str, Any] = {}
    _write_split_filters_json(empty_json, split_filter_file_path)


def _construct_split_filter_for_dataset(
    dataset: IntegrationTestDataset,
) -> dict[str, Any]:
    """Docstring."""
    timestamp_key = dataset.metadata_dict["timestamp_key"]
    return {
        f"clp.default.{dataset.dataset_name}": [
            {
                "columnName": timestamp_key,
                "customOptions": {
                    "rangeMapping": {
                        "lowerBound": "begin_timestamp",
                        "upperBound": "end_timestamp",
                    }
                },
                "required": False,
            }
        ]
    }


def _write_split_filters_json(split_filters: dict[str, Any], split_filter_file_path: Path) -> None:
    """
    Write JSON split filters with pretty formatting and a trailing newline.

    Empty dictionaries are written with opening and closing braces on separate lines.
    """
    with split_filter_file_path.open("w", encoding="utf-8") as split_filter_file:
        if not split_filters:
            split_filter_file.write("{\n}\n")
        else:
            json.dump(split_filters, split_filter_file, indent=2)
            split_filter_file.write("\n")
