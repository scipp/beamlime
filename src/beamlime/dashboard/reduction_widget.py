# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Widget and subwidgets for configuring, running, and stopping data reduction workflows.

Given a :py:class:`~beamlime.config.workflow_spec.WorkflowSpec`, this module provides a
Panel widget that allows users to configure, run, and stop data reduction workflows.
Concretely we have:

- A list-like selection widget of a workflow. Also displays a description of each
  workflow.
- A subwidget that is dynamically generated based on the selected workflow, containing
  1. A selection widget that allows for selecting multiple source names to apply the
     workflow to.
  2. A subwidget for each parameter of the workflow, allowing users to configure
     parameters of the workflow. Based on
     :py:class:`~beamlime.config.workflow_spec.Parameter`. If available,
     the initial value of the parameters is configured from a
     :py:class:`~beamlime.config.workflow_spec.WorkflowConfig`, otherwise the
     default value from the parameter is used.
- A list widget displaying running workflows, allowing users to stop them.
"""

from __future__ import annotations

from typing import Any

import panel as pn

from beamlime.config.workflow_spec import (
    Parameter,
    ParameterType,
    PersistentWorkflowConfig,
    WorkflowId,
    WorkflowSpec,
    WorkflowSpecs,
)
from beamlime.dashboard.workflow_status_list_widget import WorkflowStatusListWidget

from .workflow_controller_base import WorkflowController, WorkflowSpecsManager


class ParameterWidget:
    """Widget for configuring a single workflow parameter."""

    def __init__(self, parameter: Parameter, initial_value: Any | None = None) -> None:
        """
        Initialize parameter widget.

        Parameters
        ----------
        parameter
            Parameter specification
        initial_value
            Initial value for the parameter, if None uses parameter default
        """
        self._parameter = parameter
        self._value = initial_value if initial_value is not None else parameter.default
        self._widget = self._create_widget()

    def _create_widget(self) -> pn.Param:
        """Create the appropriate Panel widget based on parameter type."""
        widget_config = {
            "name": self._parameter.name,
            "value": self._value,
        }

        if self._parameter.unit:
            widget_config["name"] = f"{self._parameter.name} ({self._parameter.unit})"

        if self._parameter.param_type == ParameterType.INT:
            return pn.widgets.IntInput(**widget_config)
        elif self._parameter.param_type == ParameterType.FLOAT:
            return pn.widgets.FloatInput(**widget_config)
        elif self._parameter.param_type == ParameterType.STRING:
            return pn.widgets.TextInput(**widget_config)
        elif self._parameter.param_type == ParameterType.BOOL:
            return pn.widgets.Checkbox(**widget_config)
        elif self._parameter.param_type == ParameterType.OPTIONS:
            widget_config["options"] = self._parameter.options
            return pn.widgets.Select(**widget_config)
        else:
            raise ValueError(
                f"Unsupported parameter type: {self._parameter.param_type}"
            )

    @property
    def widget(self) -> pn.Param:
        """Get the Panel widget."""
        return self._widget

    @property
    def value(self) -> Any:
        """Get the current value of the parameter."""
        return self._widget.value

    @property
    def name(self) -> str:
        """Get the parameter name."""
        return self._parameter.name

    @property
    def description(self) -> str:
        """Get the parameter description."""
        return self._parameter.description


class WorkflowConfigWidget:
    """Widget for configuring workflow parameters and source selection."""

    def __init__(
        self,
        workflow_spec: WorkflowSpec,
        persistent_config: PersistentWorkflowConfig | None = None,
    ) -> None:
        """
        Initialize workflow configuration widget.

        Parameters
        ----------
        workflow_spec
            Specification of the workflow
        persistent_config
            Previously saved configuration including source names and parameters
        """
        self._workflow_spec = workflow_spec
        self._persistent_config = persistent_config
        self._parameter_widgets: dict[str, ParameterWidget] = {}
        self._source_selector = self._create_source_selector()
        self._parameter_panel = self._create_parameter_panel()
        self._widget = self._create_widget()

    def _create_source_selector(self) -> pn.widgets.MultiChoice:
        """Create source selection widget."""
        # Use persistent config source names if available
        initial_sources = []
        if self._persistent_config:
            initial_sources = self._persistent_config.source_names

        return pn.widgets.MultiChoice(
            name="Source Names",
            options=self._workflow_spec.source_names,
            value=initial_sources,
            placeholder="Select source names to apply workflow to",
        )

    def _create_parameter_panel(self) -> pn.Column:
        """Create panel containing all parameter widgets."""
        parameter_widgets = []

        for param in self._workflow_spec.parameters:
            initial_value = None

            # Use persistent config if available, otherwise parameter default
            if self._persistent_config:
                initial_value = self._persistent_config.config.values.get(param.name)

            param_widget = ParameterWidget(param, initial_value)
            self._parameter_widgets[param.name] = param_widget

            # Create a row with widget and description
            widget_row = pn.Row(
                param_widget.widget,
                pn.pane.HTML(f"<small>{param_widget.description}</small>"),
            )
            parameter_widgets.append(widget_row)

        return pn.Column(*parameter_widgets)

    def _create_widget(self) -> pn.Column:
        """Create the main configuration widget."""
        return pn.Column(
            pn.pane.HTML(f"<h4>{self._workflow_spec.name}</h4>"),
            pn.pane.HTML(f"<p>{self._workflow_spec.description}</p>"),
            self._source_selector,
            self._parameter_panel,
        )

    @property
    def widget(self) -> pn.Column:
        """Get the Panel widget."""
        return self._widget

    @property
    def selected_sources(self) -> list[str]:
        """Get the selected source names."""
        return self._source_selector.value

    @property
    def parameter_values(self) -> dict[str, Any]:
        """Get current parameter values as a dictionary."""
        return {name: widget.value for name, widget in self._parameter_widgets.items()}

    def validate_configuration(self) -> bool:
        """Validate that required fields are configured."""
        return len(self.selected_sources) > 0


class WorkflowSelectorWidget:
    """Widget for selecting workflows from available specifications."""

    def __init__(self, specs_manager: WorkflowSpecsManager) -> None:
        """
        Initialize workflow selector.

        Parameters
        ----------
        specs_manager
            Manager for workflow specifications
        """
        self._specs_manager = specs_manager
        self.no_selection = object()
        self._selector = self._create_selector()
        self._description_pane = pn.pane.HTML(
            "Select a workflow to see its description"
        )
        self._widget = self._create_widget()
        self._setup_callbacks()

        # Subscribe to specs updates
        self._specs_manager.subscribe_to_updates(self._on_workflow_specs_updated)

    def _create_options(self, specs: WorkflowSpecs) -> dict[str, WorkflowId | object]:
        """Create options dictionary for the selector."""
        select = "--- Click to select a workflow ---"
        options = {select: self.no_selection}
        options.update(
            {spec.name: workflow_id for workflow_id, spec in specs.workflows.items()}
        )
        return options

    def _create_selector(self) -> pn.widgets.Select:
        """Create workflow selection widget."""
        return pn.widgets.Select(
            name="Workflow",
            options=self._create_options(self._specs_manager.workflow_specs),
            value=self.no_selection,
        )

    def _create_widget(self) -> pn.Column:
        """Create the main selector widget."""
        return pn.Column(
            self._selector,
            self._description_pane,
        )

    def _setup_callbacks(self) -> None:
        """Setup callbacks for widget interactions."""
        self._selector.param.watch(self._on_workflow_selected, "value")

    def _on_workflow_selected(self, event) -> None:
        """Handle workflow selection change."""
        if event.new is self.no_selection:
            self._description_pane.object = "Select a workflow to see its description"
        else:
            workflow_spec = self._specs_manager.workflow_specs.workflows[event.new]
            self._description_pane.object = (
                f"<p><strong>Description:</strong> {workflow_spec.description}</p>"
            )

    def _on_workflow_specs_updated(self, workflow_specs: WorkflowSpecs) -> None:
        """Handle workflow specs updates."""
        self._selector.options = self._create_options(workflow_specs)
        self._selector.value = self.no_selection

    @property
    def widget(self) -> pn.Column:
        """Get the Panel widget."""
        return self._widget

    @property
    def selected_workflow_id(self) -> WorkflowId | None:
        """Get the currently selected workflow ID."""
        return (
            self._selector.value
            if self._selector.value is not self.no_selection
            else None
        )

    @property
    def selected_workflow_spec(self) -> WorkflowSpec | None:
        """Get the currently selected workflow specification."""
        if self.selected_workflow_id is None:
            return None
        return self._specs_manager.workflow_specs.workflows[self.selected_workflow_id]


class WorkflowConfigModal:
    """Modal dialog for workflow configuration."""

    def __init__(
        self,
        workflow_spec: WorkflowSpec,
        workflow_specs: WorkflowSpecs,
        controller: WorkflowController,
    ) -> None:
        """
        Initialize workflow configuration modal.

        Parameters
        ----------
        workflow_spec
            Specification of the workflow to configure
        workflow_specs
            All available workflow specifications (for validation)
        controller
            Controller for workflow operations
        """
        self._workflow_spec = workflow_spec
        self._workflow_specs = workflow_specs
        self._controller = controller

        # Load persistent config from controller
        workflow_id = self._find_workflow_id()
        persistent_config = None
        if workflow_id:
            persistent_config = self._controller.load_workflow_config(workflow_id)

        self._config_widget = WorkflowConfigWidget(workflow_spec, persistent_config)
        self._modal = self._create_modal()

    def _find_workflow_id(self) -> WorkflowId | None:
        """Find the workflow ID for the current spec."""
        for workflow_id, spec in self._workflow_specs.workflows.items():
            if spec.name == self._workflow_spec.name:
                return workflow_id
        return None

    def _create_modal(self) -> pn.Modal:
        """Create the modal dialog."""
        start_button = pn.widgets.Button(
            name="Start Workflow",
            button_type="primary",
        )
        start_button.on_click(self._on_start_workflow)

        cancel_button = pn.widgets.Button(
            name="Cancel",
            button_type="light",
        )
        cancel_button.on_click(self._on_cancel)

        content = pn.Column(
            self._config_widget.widget,
            pn.Row(
                pn.Spacer(),
                cancel_button,
                start_button,
                margin=(10, 0),
            ),
        )

        modal = pn.Modal(
            content,
            name=f"Configure {self._workflow_spec.name}",
            margin=20,
            width=800,
            height=600,
        )

        # Watch for modal close events to clean up
        modal.param.watch(self._on_modal_closed, 'open')

        return modal

    def _on_cancel(self, event) -> None:
        """Handle cancel button click."""
        self._modal.open = False

    def _on_modal_closed(self, event) -> None:
        """Handle modal being closed (cleanup)."""
        if not event.new:  # Modal was closed
            # Remove modal from its parent container after a short delay
            # to allow the close animation to complete
            def cleanup():
                try:
                    if hasattr(self._modal, '_parent') and self._modal._parent:
                        self._modal._parent.remove(self._modal)
                except Exception:  # noqa: S110
                    pass  # Ignore cleanup errors

            pn.state.add_periodic_callback(cleanup, period=100, count=1)

    def _on_start_workflow(self, event) -> None:
        """Handle start workflow button click."""
        # Validate that workflow still exists
        workflow_id = self._find_workflow_id()

        if workflow_id is None:
            self._show_error_modal(
                f"Error: Workflow '{self._workflow_spec.name}' "
                "is no longer available. Please select a different workflow."
            )
            return

        if not self._config_widget.validate_configuration():
            self._show_error_modal("Please select at least one source name.")
            return

        self._controller.start_workflow(
            workflow_id,
            self._config_widget.selected_sources,
            self._config_widget.parameter_values,
        )

        self._modal.open = False

    def _show_error_modal(self, message: str) -> None:
        """Show an error message in a modal."""
        error_modal = pn.Modal(
            pn.pane.HTML(f"<p style='color: red;'>{message}</p>"),
            name="Error",
            margin=20,
            width=400,
        )

        # Add to the same parent as this modal if possible
        if hasattr(self._modal, '_parent') and self._modal._parent:
            self._modal._parent.append(error_modal)

        error_modal.open = True

        # Auto-close error modal after 3 seconds
        def close_error():
            error_modal.open = False

        pn.state.add_periodic_callback(close_error, period=3000, count=1)

    def show(self) -> None:
        """Show the modal dialog."""
        self._modal.open = True

    @property
    def modal(self) -> pn.Modal:
        """Get the modal widget."""
        return self._modal


class ReductionWidget:
    """Main widget for data reduction workflow configuration and control."""

    def __init__(
        self,
        workflow_specs: WorkflowSpecs,
        controller: WorkflowController,
    ) -> None:
        """
        Initialize reduction widget.

        Parameters
        ----------
        workflow_specs
            Available workflow specifications
        controller
            Controller for workflow operations
        """
        self._controller = controller

        self._specs_manager = WorkflowSpecsManager(workflow_specs)
        self._workflow_selector = WorkflowSelectorWidget(self._specs_manager)
        self._running_workflows_widget = WorkflowStatusListWidget(
            controller, self._specs_manager
        )

        self._configure_button = pn.widgets.Button(
            name="Configure & Start",
            button_type="primary",
            disabled=True,
        )

        # Container for modals - they need to be part of the served structure
        self._modal_container = pn.Column()

        self._widget = self._create_widget()
        self._setup_callbacks()

    def _create_widget(self) -> pn.Column:
        """Create the main widget layout."""
        return pn.Column(
            pn.pane.HTML("<h3>Data Reduction Workflows</h3>"),
            pn.Column(
                self._workflow_selector.widget,
                self._configure_button,
                width=500,
            ),
            pn.Column(
                self._running_workflows_widget.widget,
                width=400,
            ),
            self._modal_container,  # Add modal container to main structure
        )

    def _setup_callbacks(self) -> None:
        """Setup callbacks for widget interactions."""
        self._workflow_selector._selector.param.watch(
            self._on_workflow_selected, "value"
        )
        self._configure_button.on_click(self._on_configure_workflow)

    def _on_workflow_selected(self, event) -> None:
        """Handle workflow selection change."""
        workflow_id = event.new
        self._configure_button.disabled = (
            workflow_id is self._workflow_selector.no_selection
        )

    def _on_configure_workflow(self, event) -> None:
        """Handle configure workflow button click."""
        workflow_id = self._workflow_selector.selected_workflow_id
        if workflow_id is None:
            return

        workflow_spec = self._specs_manager.workflow_specs.workflows[workflow_id]

        modal = WorkflowConfigModal(
            workflow_spec=workflow_spec,
            workflow_specs=self._specs_manager.workflow_specs,
            controller=self._controller,
        )

        # Add modal to container and show it
        self._modal_container.append(modal.modal)
        modal.show()

    def update_workflow_specs(self, workflow_specs: WorkflowSpecs) -> None:
        """Update the available workflow specifications."""
        self._specs_manager.update_workflow_specs(workflow_specs)

    @property
    def widget(self) -> pn.Column:
        """Get the Panel widget."""
        return self._widget
