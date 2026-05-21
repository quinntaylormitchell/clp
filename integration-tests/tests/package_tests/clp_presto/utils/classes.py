"""Classes used in clp-presto integration tests."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pytest
from typing_extensions import Self

from tests.package_tests.classes import ClpPackageTestPathConfig
from tests.utils.classes import CmdArgs, ExternalAction
from tests.utils.utils import (
    validate_dir_exists,
    validate_file_exists,
)

logger = logging.getLogger(__name__)


@dataclass
class PrestoClusterTestPathConfig(ClpPackageTestPathConfig):
    """Path configuration for clp-presto integration tests."""

    #: Default Presto deployment directory.
    presto_deployment_dir: Path

    def __post_init__(self) -> None:
        """Validates directories."""
        super().__post_init__()

        # Ensure that the Presto deployment directory exists and that it is structured correctly.
        presto_deployment_dir = self.presto_deployment_dir
        validate_dir_exists(presto_deployment_dir)
        validate_file_exists(self.set_up_config_path)
        validate_file_exists(self.split_filter_file_path)
        validate_file_exists(self.docker_compose_file_path)

    @property
    def set_up_config_path(self) -> Path:
        """:return: The absolute path to the Presto set-up-config script."""
        return self.presto_deployment_dir / "scripts" / "set-up-config.sh"

    @property
    def split_filter_file_path(self) -> Path:
        """:return: The absolute path to the Presto split-filter file."""
        return self.presto_deployment_dir / "coordinator" / "config-template" / "split-filter.json"

    @property
    def docker_compose_file_path(self) -> Path:
        """:return: The absolute path to the docker compose file for Presto deployment."""
        return self.presto_deployment_dir / "docker-compose.yaml"


@dataclass
class PrestoCluster:
    """Represents a running Presto cluster used by clp-presto integration tests."""

    # The `PrestoClusterTestPathConfig` object for this Presto cluster.
    path_config: PrestoClusterTestPathConfig


@dataclass
class PrestoAction(ExternalAction):
    """
    External action for a Presto-related subprocess (e.g., `docker compose` against the Presto
    deployment, or `presto-cli` queries). The testing system does not assume that Presto behaves
    correctly, and so `verify_returncode()` returns a `PrestoVerificationResult` object which should
    be processed at the callsite. Instances should be constructed via `from_args` or `from_cmd`.
    """

    #: Optional structured arguments. Not used by `PrestoAction` itself; available for verification.
    args: CmdArgs | None = None

    @classmethod
    def from_cmd(cls, cmd: list[str]) -> Self:
        """:return: A `PrestoAction` for the given raw `cmd`, with no associated `args`."""
        return cls(cmd=cmd)

    @classmethod
    def from_args(cls, args: CmdArgs) -> Self:
        """:return: A `PrestoAction` whose `cmd` is derived from `args.to_cmd()`."""
        return cls(cmd=args.to_cmd(), args=args)

    def __post_init__(self) -> None:
        """
        Validates `args`/`cmd` agreement when both are provided during construction. Then executes
        the action.
        """
        if self.args is not None and self.cmd != self.args.to_cmd():
            pytest.fail(
                "Cannot create `PrestoAction` object: `cmd` does not match `args.to_cmd()`."
            )
        super().__post_init__()

    def verify_returncode(
        self,
        success_returncodes: tuple[int, ...] = (0,),
    ) -> PrestoVerificationResult:
        """
        :param success_returncodes:
        :return: A successful `PrestoVerificationResult` if `completed_proc.returncode` is in
            `success_returncodes`; otherwise a failed `PrestoVerificationResult` with a message
            describing the bad return code.
        """
        if self.completed_proc.returncode in success_returncodes:
            return self.pass_verification()

        reason = (
            f"The '{Path(self.cmd[0]).name}' subprocess returned a bad return code"
            f" ({self.completed_proc.returncode})."
        )
        return self.fail_verification(reason)

    def pass_verification(self) -> PrestoVerificationResult:
        """:return: A successful `PrestoVerificationResult`."""
        return PrestoVerificationResult(success=True)

    def fail_verification(
        self,
        reason: str,
        supporting_action: PrestoAction | None = None,
    ) -> PrestoVerificationResult:
        """
        Builds a failed `PrestoVerificationResult` for this action and logs a warning. When this
        action's verification has failed as a direct result of some other `PrestoAction`, that
        action should be passed into `supporting_action` so that the path to its log file is
        included in the failure message.

        :param reason: A description of the failure.
        :param supporting_action: A previous action that caused this failure.
        :return: A failed `PrestoVerificationResult` carrying the formatted `failure_message`.
        """
        failure_message = self._format_failure_msg(reason, supporting_action=supporting_action)
        logger.warning(failure_message)
        return PrestoVerificationResult(success=False, failure_message=failure_message)

    def _format_failure_msg(
        self,
        reason: str,
        supporting_action: PrestoAction | None = None,
    ) -> str:
        msg = f"{reason} See subprocess log at: '{self.log_file_path}'."
        if supporting_action is not None:
            supporting_exe = Path(supporting_action.cmd[0]).name
            msg += (
                f" See supporting subprocess ({supporting_exe}) log at:"
                f" '{supporting_action.log_file_path}'."
            )
        return msg


@dataclass(frozen=True)
class PrestoVerificationResult:
    """Outcome returned from functions that verify Presto functionality."""

    #: Whether or not the verification was successful.
    success: bool

    #: Message describing the failure, if the verification failed.
    failure_message: str = ""

    def __bool__(self) -> bool:
        """Makes the class truthy."""
        return self.success
