from typing import Any, Protocol

from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    WorkflowId,
    WorkflowSpec,
    WorkflowStatus,
    WorkflowStatusType,
)


class WorkflowControllerBase(Protocol):
    """Core workflow control operations."""

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

    def get_workflow_specs(self) -> dict[WorkflowId, WorkflowSpec]:
        """Get all available workflow specifications."""
        ...

    def load_workflow_config(
        self, workflow_id: WorkflowId
    ) -> PersistentWorkflowConfig | None:
        """Load saved workflow configuration."""
        ...

    def workflow_exists(self, workflow_id: WorkflowId) -> bool:
        """Check if a workflow ID exists in current specs."""
        ...

    def subscribe_to_workflow_updates(self, callback: callable) -> None:
        """Subscribe to workflow updates."""
        ...

    def subscribe_to_workflow_status_updates(self, callback: callable) -> None:
        """Subscribe to workflow status updates."""
        ...


class WorkflowUIHelper:
    """Helper class for workflow UI operations."""

    def __init__(self, controller: WorkflowControllerBase):
        """Initialize UI helper with controller reference."""
        self._controller = controller
        self.no_selection = object()

    def get_workflow_options(self) -> dict[str, WorkflowId | object]:
        """Get workflow options for selector widget."""
        select_text = "--- Click to select a workflow ---"
        options = {select_text: self.no_selection}

        specs = self._controller.get_workflow_specs()
        options.update({spec.name: workflow_id for workflow_id, spec in specs.items()})
        return options

    def is_no_selection(self, value: WorkflowId | object) -> bool:
        """Check if the given value represents no workflow selection."""
        return value is self.no_selection

    def get_default_workflow_selection(self) -> object:
        """Get the default value for no workflow selection."""
        return self.no_selection

    def get_workflow_name(self, workflow_id: WorkflowId | None) -> str:
        """Get workflow name from ID, fallback to ID if not found."""
        if workflow_id is None:
            return "None"
        if (spec := self._controller.get_workflow_spec(workflow_id)) is not None:
            return spec.name
        return str(workflow_id)

    def get_workflow_description(self, workflow_id: WorkflowId | object) -> str | None:
        """
        Get the description for a workflow ID or selection value.

        Returns `None` if no workflow is selected.
        """
        if self.is_no_selection(workflow_id):
            return None
        if (spec := self._controller.get_workflow_spec(workflow_id)) is None:
            return None
        return spec.description

    def get_initial_parameter_values(self, workflow_id: WorkflowId) -> dict[str, Any]:
        """Get initial parameter values for a workflow."""
        spec = self._controller.get_workflow_spec(workflow_id)
        if not spec:
            return {}

        values = {param.name: param.default for param in spec.parameters}

        # Override with persistent config if available
        persistent_config = self._controller.load_workflow_config(workflow_id)
        if persistent_config:
            values.update(persistent_config.config.values)

        return values

    def get_initial_source_names(self, workflow_id: WorkflowId) -> list[str]:
        """Get initial source names for a workflow."""
        persistent_config = self._controller.load_workflow_config(workflow_id)
        return persistent_config.source_names if persistent_config else []


class WorkflowStatusUIHelper:
    """Helper class for workflow status display."""

    @staticmethod
    def get_status_display_info(status: WorkflowStatus) -> dict[str, str]:
        """Get display information for a workflow status."""
        if status.status == WorkflowStatusType.STARTING:
            return {
                'color': '#ffc107',  # Yellow
                'text': 'Starting...',
                'button_name': 'Stop',
                'button_type': 'primary',
                'opacity_style': '',
            }
        elif status.status == WorkflowStatusType.RUNNING:
            return {
                'color': '#28a745',  # Green
                'text': 'Running',
                'button_name': 'Stop',
                'button_type': 'primary',
                'opacity_style': '',
            }
        elif status.status == WorkflowStatusType.STOPPING:
            return {
                'color': '#b87817',  # Orange
                'text': 'Stopping...',
                'button_name': 'Stop',
                'button_type': 'primary',
                'opacity_style': '',
            }
        elif status.status == WorkflowStatusType.STARTUP_ERROR:
            return {
                'color': '#dc3545',  # Red
                'text': 'Error',
                'button_name': 'Remove',
                'button_type': 'light',
                'opacity_style': 'opacity: 0.7;',
            }
        elif status.status == WorkflowStatusType.STOPPED:
            return {
                'color': '#6c757d',  # Gray
                'text': 'Stopped',
                'button_name': 'Remove',
                'button_type': 'light',
                'opacity_style': 'opacity: 0.7;',
            }
        else:  # UNKNOWN
            return {
                'color': '#6c757d',  # Gray
                'text': 'Unknown',
                'button_name': 'Remove',
                'button_type': 'light',
                'opacity_style': 'opacity: 0.7;',
            }
