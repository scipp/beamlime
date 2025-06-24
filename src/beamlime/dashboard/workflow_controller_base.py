from typing import Any, Protocol

from beamlime.config.workflow_spec import WorkflowId, WorkflowSpec, WorkflowStatus


class WorkflowControllerBase(Protocol):
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

    def get_workflow_spec(self, workflow_id: WorkflowId) -> WorkflowSpec | None:
        """Get the current workflow specification for the given Id."""
        ...

    def subscribe_to_workflow_updates(self, callback: callable) -> None:
        """Subscribe to workflow updates."""
        ...

    def subscribe_to_workflow_status_updates(self, callback: callable) -> None:
        """Subscribe to workflow status updates."""
        ...

    def get_workflow_name(self, workflow_id: WorkflowId | None) -> str:
        """Get workflow name from ID, fallback to ID if not found."""
        ...

    def get_status_display_info(self, status: WorkflowStatus) -> dict[str, str]:
        """Get display information for a workflow status."""
        ...

    def get_workflow_options(self) -> dict[str, WorkflowId | object]:
        """Get workflow options for selector widget."""
        ...

    def get_initial_parameter_values(self, workflow_id: WorkflowId) -> dict[str, Any]:
        """Get initial parameter values for a workflow."""
        ...

    def get_initial_source_names(self, workflow_id: WorkflowId) -> list[str]:
        """Get initial source names for a workflow."""
        ...

    def is_no_selection(self, value: WorkflowId | object) -> bool:
        """Check if the given value represents no workflow selection."""
        ...

    def get_default_workflow_selection(self) -> object:
        """Get the default value for no workflow selection."""
        ...

    def get_workflow_description(self, workflow_id: WorkflowId | object) -> str | None:
        """Get the description for a workflow ID or selection value."""
        ...

    def workflow_exists(self, workflow_id: WorkflowId) -> bool:
        """Check if a workflow ID exists in current specs."""
        ...
