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


class WorkflowSpecsManager:
    """Centralized manager for workflow specifications."""

    def __init__(self, workflow_specs: WorkflowSpecs) -> None:
        """
        Initialize workflow specs manager.

        Parameters
        ----------
        workflow_specs
            Initial workflow specifications
        """
        self._workflow_specs = workflow_specs
        self._subscribers: list[callable] = []

    def update_workflow_specs(self, workflow_specs: WorkflowSpecs) -> None:
        """Update workflow specs and notify subscribers."""
        self._workflow_specs = workflow_specs
        for callback in self._subscribers:
            callback(workflow_specs)

    def subscribe_to_updates(self, callback: callable) -> None:
        """Subscribe to workflow specs updates."""
        self._subscribers.append(callback)

    @property
    def workflow_specs(self) -> WorkflowSpecs:
        """Get current workflow specifications."""
        return self._workflow_specs
