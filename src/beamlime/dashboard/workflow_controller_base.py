from typing import Any, Protocol

from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    WorkflowId,
    WorkflowSpecs,
    WorkflowStatus,
)


class WorkflowController(Protocol):
    """Protocol for workflow control operations."""

    def start_workflow(
        self, workflow_id: WorkflowId, source_names: list[str], config: dict[str, Any]
    ) -> None:
        """Start a workflow with given configuration."""
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

    def get_workflow_specs(self) -> WorkflowSpecs:
        """Get the current workflow specifications."""
        ...

    def subscribe_to_workflow_specs_updates(self, callback: callable) -> None:
        """Subscribe to workflow specs updates."""
        ...

    def subscribe_to_workflow_status_updates(self, callback: callable) -> None:
        """Subscribe to workflow status updates."""
        ...

    def load_workflow_config(
        self, workflow_id: WorkflowId
    ) -> PersistentWorkflowConfig | None:
        """Load saved workflow configuration."""
        ...
