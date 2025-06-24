# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import Any

from beamlime.config.workflow_spec import WorkflowId, WorkflowSpec
from beamlime.dashboard.workflow_controller_base import WorkflowControllerBase


class WorkflowUIHelper:
    """Helper class for workflow UI operations."""

    no_selection = object()

    def __init__(self, controller: WorkflowControllerBase):
        """Initialize UI helper with controller reference."""
        self._controller = controller

    @staticmethod
    def make_workflow_options(
        specs: dict[WorkflowId, WorkflowSpec] | None = None,
    ) -> dict[str, WorkflowId | object]:
        """
        Get workflow options for selector widget.

        Note that is uses the specs passed as an argument and not the ones from the
        controller.
        """
        specs = specs or {}
        select_text = "--- Click to select a workflow ---"
        options = {select_text: WorkflowUIHelper.no_selection}
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
        if not isinstance(workflow_id, WorkflowId) or self.is_no_selection(workflow_id):
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
        persistent_config = self._controller.get_workflow_config(workflow_id)
        if persistent_config:
            values.update(persistent_config.config.values)

        return values

    def get_initial_source_names(self, workflow_id: WorkflowId) -> list[str]:
        """Get initial source names for a workflow."""
        persistent_config = self._controller.get_workflow_config(workflow_id)
        return persistent_config.source_names if persistent_config else []
