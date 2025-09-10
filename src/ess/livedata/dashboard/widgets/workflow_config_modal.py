# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import panel as pn

from ess.livedata.dashboard.workflow_controller import BoundWorkflowController

from .configuration_widget import (
    ConfigurationAdapter,
    ConfigurationModal,
    ConfigurationWidget,
)
from .workflow_ui_helper import WorkflowUIHelper


class WorkflowConfigurationAdapter(ConfigurationAdapter):
    """Adapter for workflow configuration using BoundWorkflowController."""

    def __init__(self, controller: BoundWorkflowController) -> None:
        """Initialize adapter with workflow controller."""
        self._controller = controller
        self._ui_helper = WorkflowUIHelper(controller)

    @property
    def title(self) -> str:
        """Get workflow title."""
        return self._ui_helper.get_workflow_title()

    @property
    def description(self) -> str:
        """Get workflow description."""
        return self._ui_helper.get_workflow_description()

    @property
    def model_class(self) -> type:
        """Get workflow parameters model class."""
        return self._controller.params_model_class

    @property
    def source_names(self) -> list[str]:
        """Get available source names."""
        return self._ui_helper.get_source_names()

    @property
    def initial_source_names(self) -> list[str]:
        """Get initial source names."""
        return self._ui_helper.get_initial_source_names()

    @property
    def initial_parameter_values(self) -> dict[str, Any]:
        """Get initial parameter values."""
        return self._ui_helper.get_initial_parameter_values()

    def start_action(self, selected_sources: list[str], parameter_values: Any) -> bool:
        """Start the workflow with given sources and parameters."""
        return self._controller.start_workflow(selected_sources, parameter_values)


class WorkflowConfigWidget:
    """Widget for configuring workflow parameters and source selection."""

    def __init__(self, controller: BoundWorkflowController) -> None:
        """
        Initialize workflow configuration widget.

        Parameters
        ----------
        controller
            Controller bound to a specific workflow
        """
        self._adapter = WorkflowConfigurationAdapter(controller)
        self._generic_widget = ConfigurationWidget(self._adapter)

    @property
    def widget(self):
        """Get the Panel widget."""
        return self._generic_widget.widget

    @property
    def selected_sources(self) -> list[str]:
        """Get the selected source names."""
        return self._generic_widget.selected_sources

    @property
    def parameter_values(self):
        """Get current parameter values as a model instance."""
        return self._generic_widget.parameter_values

    def validate_configuration(self) -> tuple[bool, list[str]]:
        """Validate that required fields are configured."""
        return self._generic_widget.validate_configuration()

    def clear_validation_errors(self) -> None:
        """Clear all validation error states."""
        self._generic_widget.clear_validation_errors()


class WorkflowConfigModal:
    """Modal dialog for workflow configuration."""

    def __init__(self, controller: BoundWorkflowController) -> None:
        """
        Initialize workflow configuration modal.

        Parameters
        ----------
        controller
            Controller bound to a specific workflow
        """
        self._controller = controller
        self._adapter = WorkflowConfigurationAdapter(controller)
        self._generic_modal = ConfigurationModal(
            config=self._adapter,
            start_button_text="Start Workflow",
        )

    def show(self) -> None:
        """Show the modal dialog."""
        self._generic_modal.show()

    @property
    def modal(self) -> pn.Modal:
        """Get the modal widget."""
        return self._generic_modal.modal
