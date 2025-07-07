# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""
Models for data reduction workflow widget creation and configuration.
"""

from __future__ import annotations

import time
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
    params: type[BaseModel] | None = Field(description="Model for workflow param.")

    def get_id(self) -> WorkflowId:
        """
        Get a unique identifier for the workflow.

        The identifier is a combination of instrument, name, and version.
        """
        return f"{self.instrument}/{self.name}/{self.version}"


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
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters for the workflow, as JSON-serialized Pydantic model.",
    )


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
