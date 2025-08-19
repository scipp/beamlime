# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""
Models for data reduction workflow widget creation and configuration.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeVar

from pydantic import BaseModel, Field

T = TypeVar('T')

WorkflowId = str


class WorkflowSpec(BaseModel):
    """
    Model for workflow specification.

    This model is used to define a workflow and its parameters. Beamlime publishes
    workflow specifications to Kafka, which can be used to create user interfaces for
    configuring workflows.
    """

    instrument: str = Field(
        description="Name of the instrument this workflow is associated with."
    )
    name: str = Field(description="Name of the workflow. Used internally.")
    version: int = Field(description="Version of the workflow.")
    title: str = Field(description="Title of the workflow. For display in the UI.")
    description: str = Field(description="Description of the workflow.")
    source_names: list[str] = Field(
        default_factory=list,
        description="List of detectors the workflow can be applied to.",
    )
    aux_source_names: list[str] = Field(
        default_factory=list,
        description="List of auxiliary data streams the workflow needs.",
    )
    params: type[BaseModel] | None = Field(description="Model for workflow param.")

    def get_id(self) -> WorkflowId:
        """
        Get a unique identifier for the workflow.

        The identifier is a combination of instrument, name, and version.
        """
        return f"{self.instrument}/{self.name}/{self.version}"


@dataclass
class JobSchedule:
    """
    Defines when a job should start and optionally when it should end.

    All timestamps are in nanoseconds since the epoch (UTC) and reference the timestamps
    of the raw data being processed (as opposed to when it should be processed).
    """

    start_time: int | None = None  # When job should start processing
    end_time: int | None = None  # When job should stop (None = no limit)

    def __post_init__(self) -> None:
        """Validate the schedule configuration."""
        if (
            self.end_time is not None
            and self.start_time is not None
            and self.end_time <= self.start_time
        ):
            raise ValueError(
                f"Job end_time={self.end_time} must be greater than start_time="
                f"{self.start_time}, or start_time must be None (immediate start)"
            )

    def should_start(self, current_time: int) -> bool:
        """
        Check if the job should start based on the current time.

        Returns True if the job should start, False otherwise.
        """
        return self.start_time is None or current_time >= self.start_time


class WorkflowConfig(BaseModel):
    """
    Model for workflow configuration.

    This model is used to set the parameter values for a specific workflow. The values
    correspond to the parameters defined in the workflow specification
    :py:class:`WorkflowSpec`.
    """

    identifier: WorkflowId | None = Field(
        description="Hash of the workflow, used to identify the workflow."
    )
    schedule: JobSchedule = Field(
        default_factory=JobSchedule, description="Schedule for the workflow."
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters for the workflow, as JSON-serialized Pydantic model.",
    )

    def get_instrument(self) -> str | None:
        """
        Get the instrument name from the workflow identifier.

        The identifier is expected to be in the format 'instrument/name/version'.
        """
        if self.identifier is None or '/' not in self.identifier:
            return None
        return self.identifier.split('/')[0]


class PersistentWorkflowConfig(BaseModel):
    """
    Persistent storage for workflow configuration including source selection.

    This model stores both the source names selection and the parameter values
    for a workflow, allowing the UI to restore the last-used configuration.
    """

    source_names: list[str] = Field(
        default_factory=list,
        description="Selected source names for this workflow",
    )
    config: WorkflowConfig = Field(
        description="Configuration for the workflow, including parameter values",
    )


class PersistentWorkflowConfigs(BaseModel):
    """
    Collection of all persistent workflow configurations.

    This model stores persistent configurations for multiple workflows in a single
    config item, making it easy to manage and clean up old configurations.
    """

    configs: dict[WorkflowId, PersistentWorkflowConfig] = Field(
        default_factory=dict,
        description="Persistent configurations indexed by workflow ID",
    )

    def cleanup_missing_workflows(self, current_workflow_ids: set[WorkflowId]) -> None:
        """Remove configurations for workflows that no longer exist."""
        missing_ids = set(self.configs.keys()) - current_workflow_ids
        for workflow_id in missing_ids:
            del self.configs[workflow_id]


class WorkflowStatusType(str, Enum):
    """
    Status of a workflow execution.

    The idea of the "stopped" status is to have the option of still displaying the data
    in the UI. The UI may then remove the workflow entirely in a separate step. This is
    not implemented yet.
    """

    STARTING = "starting"
    STOPPING = "stopping"
    RUNNING = "running"
    STARTUP_ERROR = "startup_error"
    STOPPED = "stopped"
    UNKNOWN = "unknown"


class WorkflowStatus(BaseModel):
    """
    Model for workflow status.

    This model is used to define the status of a workflow, including its ID and status.
    """

    source_name: str = Field(description="Source name the workflow is associated with.")
    workflow_id: WorkflowId | None = Field(
        default=None, description="ID of the workflow."
    )
    status: WorkflowStatusType = Field(
        default=WorkflowStatusType.UNKNOWN, description="Status of the workflow."
    )
    message: str = Field(
        default='', description="Optional message providing additional information."
    )
    timestamp: int = Field(
        default_factory=lambda: int(time.time()),
        description="Unix timestamp when the status was created or updated.",
    )
