# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import Any

from ess.livedata.config.workflow_spec import WorkflowId, WorkflowSpec
from ess.livedata.dashboard.workflow_controller import BoundWorkflowController


class WorkflowUIHelper:
    """Helper class for workflow UI operations."""

    no_selection = object()

    def __init__(self, controller: BoundWorkflowController):
        """Initialize UI helper with bound controller reference."""
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
        options.update({spec.title: workflow_id for workflow_id, spec in specs.items()})
        return options

    @staticmethod
    def is_no_selection(value: WorkflowId | object) -> bool:
        """Check if the given value represents no workflow selection."""
        return value is WorkflowUIHelper.no_selection

    @staticmethod
    def get_default_workflow_selection() -> object:
        """Get the default value for no workflow selection."""
        return WorkflowUIHelper.no_selection

    def get_workflow_title(self) -> str:
        """Get workflow title from bound controller."""
        return self._controller.spec.title

    def get_workflow_description(self) -> str:
        """Get the description for the bound workflow."""
        return self._controller.spec.description

    def get_source_names(self) -> list[str]:
        """Get all source names the workflow supports."""
        return self._controller.spec.source_names

    def get_initial_source_names(self) -> list[str]:
        """Get initial source names for the bound workflow controller."""
        persistent_config = self._controller.get_persistent_config()
        return persistent_config.source_names if persistent_config else []

    def get_initial_parameter_values(self) -> dict[str, Any]:
        """Get initial parameter values for the bound workflow controller."""
        persistent_config = self._controller.get_persistent_config()
        if not persistent_config:
            return {}
        return persistent_config.config.params
