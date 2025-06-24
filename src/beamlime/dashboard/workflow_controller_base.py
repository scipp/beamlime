# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import Any, Protocol

from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    WorkflowId,
    WorkflowSpec,
    WorkflowStatus,
)


class WorkflowControllerBase(Protocol):
    """Core workflow control operations."""

    def start_workflow(
        self, workflow_id: WorkflowId, source_names: list[str], config: dict[str, Any]
    ) -> bool:
        """Start a workflow with given configuration.

        Returns True if the workflow was started successfully, False otherwise.
        """
        ...

    def stop_workflow_for_source(self, source_name: str) -> None:
        """Stop a running workflow for a specific source."""
        ...

    def remove_workflow_for_source(self, source_name: str) -> None:
        """Remove a stopped workflow from tracking."""
        ...

    def get_all_workflow_status(self) -> dict[str, WorkflowStatus]:
        """Get workflow status for all tracked sources."""
        ...

    def get_workflow_spec(self, workflow_id: WorkflowId) -> WorkflowSpec | None:
        """Get the current workflow specification for the given Id."""
        ...

    def get_workflow_specs(self) -> dict[WorkflowId, WorkflowSpec]:
        """Get all available workflow specifications."""
        ...

    def get_workflow_config(
        self, workflow_id: WorkflowId
    ) -> PersistentWorkflowConfig | None:
        """Get saved workflow configuration."""
        ...

    def subscribe_to_workflow_updates(self, callback: callable) -> None:
        """Subscribe to workflow updates."""
        ...

    def subscribe_to_workflow_status_updates(self, callback: callable) -> None:
        """Subscribe to workflow status updates."""
        ...
