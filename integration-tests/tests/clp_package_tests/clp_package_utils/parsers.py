"""Module docstring."""

import argparse
from pathlib import Path

from clp_package_utils.cli_utils import RESTART_POLICY
from clp_package_utils.general import (
    EXTRACT_FILE_CMD,
    EXTRACT_IR_CMD,
    EXTRACT_JSON_CMD,
    S3_KEY_PREFIX_COMPRESSION,
    S3_OBJECT_COMPRESSION,
)
from clp_package_utils.scripts.archive_manager import (
    BEGIN_TS_ARG,
    DEL_BY_FILTER_SUBCOMMAND,
    DEL_BY_IDS_SUBCOMMAND,
    DEL_COMMAND,
    DRY_RUN_ARG,
    END_TS_ARG,
    FIND_COMMAND,
)
from clp_package_utils.scripts.dataset_manager import (
    LIST_COMMAND,
)

# TODO: find out how you're going to determine this dynamically per test run.
DEFAULT_CONFIG_FILE_PATH = Path("/to/be/determined")


def get_archive_manager_parser() -> argparse.ArgumentParser:
    """Docstring."""
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "--config",
        "-c",
        default=str(DEFAULT_CONFIG_FILE_PATH),
        help="CLP package configuration file.",
    )
    args_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args_parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="The dataset that the archives belong to.",
    )

    # Top-level commands
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser] = args_parser.add_subparsers(
        dest="subcommand",
        required=True,
    )
    find_parser: argparse.ArgumentParser = subparsers.add_parser(
        FIND_COMMAND,
        help="Lists IDs of compressed archives.",
    )
    del_parser: argparse.ArgumentParser = subparsers.add_parser(
        DEL_COMMAND,
        help="Deletes compressed archives from the database and file system.",
    )

    # Options for find subcommand
    find_parser.add_argument(
        BEGIN_TS_ARG,
        dest="begin_ts",
        type=int,
        default=0,
        help="Time-range lower-bound (inclusive) as milliseconds from the UNIX epoch.",
    )
    find_parser.add_argument(
        END_TS_ARG,
        dest="end_ts",
        type=int,
        help="Time-range upper-bound (inclusive) as milliseconds from the UNIX epoch.",
    )

    # Options for delete subcommand
    del_parser.add_argument(
        DRY_RUN_ARG,
        dest="dry_run",
        action="store_true",
        help="Only prints the archives to be deleted, without actually deleting them.",
    )

    # Subcommands for delete subcommand
    del_subparsers: argparse._SubParsersAction[argparse.ArgumentParser] = del_parser.add_subparsers(
        dest="del_subcommand",
        required=True,
    )

    # Delete by ID subcommand
    del_id_parser: argparse.ArgumentParser = del_subparsers.add_parser(
        DEL_BY_IDS_SUBCOMMAND,
        help="Deletes archives by ID.",
    )

    # Delete by ID arguments
    del_id_parser.add_argument(
        "ids",
        nargs="+",
        help="List of archive IDs to delete",
    )

    # Delete by filter subcommand
    del_filter_parser: argparse.ArgumentParser = del_subparsers.add_parser(
        DEL_BY_FILTER_SUBCOMMAND,
        help="Deletes compressed archives that fall within the specified time range.",
    )

    # Delete by filter arguments
    del_filter_parser.add_argument(
        BEGIN_TS_ARG,
        type=int,
        default=0,
        help="Time-range lower-bound (inclusive) as milliseconds from the UNIX epoch.",
    )
    del_filter_parser.add_argument(
        END_TS_ARG,
        type=int,
        required=True,
        help="Time-range upper-bound (inclusive) as milliseconds from the UNIX epoch.",
    )

    return args_parser


def get_compress_from_s3_parser() -> argparse.ArgumentParser:
    """Docstring."""
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "--config",
        "-c",
        default=str(DEFAULT_CONFIG_FILE_PATH),
        help="CLP package configuration file.",
    )
    args_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args_parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="The dataset that the archives belong to.",
    )
    args_parser.add_argument(
        "--timestamp-key",
        help="The path (e.g. x.y) for the field containing the log event's timestamp.",
    )
    args_parser.add_argument(
        "--unstructured",
        action="store_true",
        help="Treat all inputs as unstructured text logs.",
    )
    args_parser.add_argument(
        "--no-progress-reporting", action="store_true", help="Disables progress reporting."
    )

    subparsers = args_parser.add_subparsers(dest="subcommand", required=True)

    object_compression_option_parser = subparsers.add_parser(
        S3_OBJECT_COMPRESSION, help="Compress specific S3 objects identified by their full URLs."
    )
    object_compression_option_parser.add_argument(
        "inputs", metavar="URL", nargs="*", help="S3 object URLs."
    )
    object_compression_option_parser.add_argument(
        "--inputs-from", type=str, help="A file containing all S3 object URLs to compress."
    )

    prefix_compression_option_parser = subparsers.add_parser(
        S3_KEY_PREFIX_COMPRESSION, help="Compress all S3 objects under the key prefix."
    )
    prefix_compression_option_parser.add_argument(
        "inputs", metavar="URL", nargs="*", help="S3 prefix URL."
    )
    prefix_compression_option_parser.add_argument(
        "--inputs-from", type=str, help="A file containing S3 key prefix to compress."
    )

    return args_parser


def get_compress_parser() -> argparse.ArgumentParser:
    """Docstring."""
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "--config",
        "-c",
        default=str(DEFAULT_CONFIG_FILE_PATH),
        help="CLP package configuration file.",
    )
    args_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args_parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="The dataset that the archives belong to.",
    )
    args_parser.add_argument(
        "--timestamp-key",
        help="The path (e.g. x.y) for the field containing the log event's timestamp.",
    )
    args_parser.add_argument(
        "--unstructured",
        action="store_true",
        help="Treat all inputs as unstructured text logs.",
    )
    args_parser.add_argument(
        "--no-progress-reporting", action="store_true", help="Disables progress reporting."
    )
    args_parser.add_argument("paths", metavar="PATH", nargs="*", help="Paths to compress.")
    args_parser.add_argument(
        "-f", "--path-list", dest="path_list", help="A file listing all paths to compress."
    )

    return args_parser


def get_dataset_manager_parser() -> argparse.ArgumentParser:
    """Docstring."""
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "--config",
        "-c",
        default=str(DEFAULT_CONFIG_FILE_PATH),
        help="CLP package configuration file.",
    )
    args_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )

    # Top-level commands
    subparsers = args_parser.add_subparsers(
        dest="subcommand",
        required=True,
    )
    subparsers.add_parser(
        LIST_COMMAND,
        help="List existing datasets.",
    )

    # Options for delete subcommand
    del_parser = subparsers.add_parser(
        DEL_COMMAND,
        help="Delete datasets from the database and file storage.",
    )
    del_parser.add_argument(
        "datasets",
        nargs="*",
        help="Datasets to delete.",
    )
    del_parser.add_argument(
        "-a",
        "--all",
        dest="del_all",
        action="store_true",
        help="Delete all existing datasets.",
    )

    return args_parser


def get_decompress_parser() -> argparse.ArgumentParser:
    """Docstring."""
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "--config",
        "-c",
        default=str(DEFAULT_CONFIG_FILE_PATH),
        help="CLP configuration file.",
    )
    args_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    command_args_parser = args_parser.add_subparsers(dest="command", required=True)

    # File extraction command parser
    file_extraction_parser = command_args_parser.add_parser(EXTRACT_FILE_CMD)
    file_extraction_parser.add_argument(
        "paths", metavar="PATH", nargs="*", help="Files to extract."
    )
    file_extraction_parser.add_argument(
        "-f", "--files-from", help="A file listing all files to extract."
    )
    file_extraction_parser.add_argument(
        "-d",
        "--extraction-dir",
        metavar="DIR",
        default=".",
        help="Extract files into DIR.",
    )

    # IR extraction command parser
    ir_extraction_parser = command_args_parser.add_parser(EXTRACT_IR_CMD)
    ir_extraction_parser.add_argument("msg_ix", type=int, help="Message index.")
    ir_extraction_parser.add_argument(
        "--target-uncompressed-size", type=int, help="Target uncompressed IR size."
    )

    group = ir_extraction_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--orig-file-id", type=str, help="Original file's ID.")
    group.add_argument("--orig-file-path", type=str, help="Original file's path.")

    # JSON extraction command parser
    json_extraction_parser = command_args_parser.add_parser(EXTRACT_JSON_CMD)
    json_extraction_parser.add_argument("archive_id", type=str, help="Archive ID")
    json_extraction_parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="The dataset that the archives belong to.",
    )
    json_extraction_parser.add_argument(
        "--target-chunk-size",
        type=int,
        help="Target chunk size (B).",
    )

    return args_parser


def get_search_parser() -> argparse.ArgumentParser:
    """Docstring."""
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "--config",
        "-c",
        default=str(DEFAULT_CONFIG_FILE_PATH),
        help="CLP package configuration file.",
    )
    args_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args_parser.add_argument("wildcard_query", help="Wildcard query.")
    args_parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="The dataset that the archives belong to.",
    )
    args_parser.add_argument(
        "--begin-time",
        type=int,
        help="Time range filter lower-bound (inclusive) as milliseconds from the UNIX epoch.",
    )
    args_parser.add_argument(
        "--end-time",
        type=int,
        help="Time range filter upper-bound (inclusive) as milliseconds from the UNIX epoch.",
    )
    args_parser.add_argument(
        "--ignore-case",
        action="store_true",
        help="Ignore case distinctions between values in the query and the compressed data.",
    )
    args_parser.add_argument("--file-path", help="File to search.")
    args_parser.add_argument("--count", action="store_true", help="Count the number of results.")
    args_parser.add_argument(
        "--count-by-time",
        type=int,
        help="Count the number of results in each time span of the given size (ms).",
    )
    args_parser.add_argument(
        "--raw", action="store_true", help="Output the search results as raw logs."
    )

    return args_parser


def get_start_clp_parser() -> argparse.ArgumentParser:
    """Docstring."""
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "--config",
        "-c",
        default=str(DEFAULT_CONFIG_FILE_PATH),
        help="CLP package configuration file.",
    )
    args_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args_parser.add_argument(
        "--restart-policy",
        default="on-failure:3",
        type=RESTART_POLICY,
        help=f"Docker restart policy ({RESTART_POLICY.VALID_POLICIES_STR}).",
    )
    args_parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Validate configuration and prepare directories without starting services.",
    )

    return args_parser


def get_stop_clp_parser() -> argparse.ArgumentParser:
    """Docstring."""
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "--config",
        "-c",
        default=str(DEFAULT_CONFIG_FILE_PATH),
        help="CLP package configuration file.",
    )

    return args_parser
