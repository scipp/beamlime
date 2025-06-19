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

from collections.abc import Callable
from typing import Any, Protocol

import panel as pn

from beamlime.config.workflow_spec import (
    Parameter,
    ParameterType,
    WorkflowConfig,
    WorkflowId,
    WorkflowSpec,
    WorkflowSpecs,
)


class WorkflowController(Protocol):
    """Protocol for workflow control operations."""

    def start_workflow(
        self, workflow_id: WorkflowId, source_names: list[str], config: dict[str, Any]
    ) -> None:
        """Start a workflow with given configuration."""
        ...

    def stop_workflow(self, workflow_id: WorkflowId) -> None:
        """Stop a running workflow."""
        ...

    def get_running_workflows(self) -> dict[WorkflowId, list[str]]:
        """Get currently running workflows and their source names."""
        ...


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
        workflow_config: WorkflowConfig | None = None,
    ) -> None:
        """
        Initialize workflow configuration widget.

        Parameters
        ----------
        workflow_spec
            Specification of the workflow
        workflow_config
            Optional initial configuration values
        """
        self._workflow_spec = workflow_spec
        self._workflow_config = workflow_config
        self._parameter_widgets: dict[str, ParameterWidget] = {}
        self._source_selector = self._create_source_selector()
        self._parameter_panel = self._create_parameter_panel()
        self._widget = self._create_widget()

    def _create_source_selector(self) -> pn.widgets.MultiChoice:
        """Create source selection widget."""
        return pn.widgets.MultiChoice(
            name="Source Names",
            options=self._workflow_spec.source_names,
            value=[],
            placeholder="Select source names to apply workflow to",
        )

    def _create_parameter_panel(self) -> pn.Column:
        """Create panel containing all parameter widgets."""
        parameter_widgets = []

        for param in self._workflow_spec.parameters:
            initial_value = None
            if self._workflow_config:
                initial_value = self._workflow_config.values.get(param.name)

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

    def __init__(self, workflow_specs: WorkflowSpecs) -> None:
        """
        Initialize workflow selector.

        Parameters
        ----------
        workflow_specs
            Available workflow specifications
        """
        self._workflow_specs = workflow_specs
        self._selector = self._create_selector()
        self._description_pane = pn.pane.HTML(
            "Select a workflow to see its description"
        )
        self._widget = self._create_widget()
        self._setup_callbacks()

    def _create_selector(self) -> pn.widgets.Select:
        """Create workflow selection widget."""
        options = {"(Select a workflow)": None}
        options.update(
            {
                spec.name: workflow_id
                for workflow_id, spec in self._workflow_specs.workflows.items()
            }
        )
        return pn.widgets.Select(
            name="Workflow",
            options=options,
            value=None,
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
        if event.new is None:
            self._description_pane.object = "Select a workflow to see its description"
        else:
            workflow_spec = self._workflow_specs.workflows[event.new]
            self._description_pane.object = (
                f"<p><strong>Description:</strong> {workflow_spec.description}</p>"
            )

    @property
    def widget(self) -> pn.Column:
        """Get the Panel widget."""
        return self._widget

    @property
    def selected_workflow_id(self) -> WorkflowId | None:
        """Get the currently selected workflow ID."""
        return self._selector.value

    @property
    def selected_workflow_spec(self) -> WorkflowSpec | None:
        """Get the currently selected workflow specification."""
        if self.selected_workflow_id is None:
            return None
        return self._workflow_specs.workflows[self.selected_workflow_id]


class RunningWorkflowsWidget:
    """Widget for displaying and controlling running workflows."""

    def __init__(self, controller: WorkflowController) -> None:
        """
        Initialize running workflows widget.

        Parameters
        ----------
        controller
            Controller for workflow operations
        """
        self._controller = controller
        self._workflow_list = pn.Column()
        self._widget = self._create_widget()

    def _create_widget(self) -> pn.Column:
        """Create the main widget."""
        return pn.Column(
            pn.pane.HTML("<h4>Running Workflows</h4>"),
            self._workflow_list,
        )

    def refresh(self) -> None:
        """Refresh the list of running workflows."""
        running_workflows = self._controller.get_running_workflows()

        if not running_workflows:
            self._workflow_list.objects = [pn.pane.HTML("<p>No running workflows</p>")]
            return

        workflow_items = []
        for workflow_id, source_names in running_workflows.items():
            sources_text = ", ".join(source_names)
            stop_button = pn.widgets.Button(
                name="Stop",
                button_type="primary",
                width=80,
            )

            # Create closure to capture workflow_id
            def create_stop_callback(wf_id: WorkflowId) -> Callable:
                return lambda event: self._controller.stop_workflow(wf_id)

            stop_button.on_click(create_stop_callback(workflow_id))

            workflow_row = pn.Row(
                pn.pane.HTML(
                    f"<strong>{workflow_id}</strong><br>Sources: {sources_text}"
                ),
                stop_button,
            )
            workflow_items.append(workflow_row)

        self._workflow_list.objects = workflow_items

    @property
    def widget(self) -> pn.Column:
        """Get the Panel widget."""
        return self._widget


class ReductionWidget:
    """Main widget for data reduction workflow configuration and control."""

    def __init__(
        self,
        workflow_specs: WorkflowSpecs,
        controller: WorkflowController,
        initial_config: WorkflowConfig | None = None,
    ) -> None:
        """
        Initialize reduction widget.

        Parameters
        ----------
        workflow_specs
            Available workflow specifications
        controller
            Controller for workflow operations
        initial_config
            Optional initial workflow configuration
        """
        self._workflow_specs = workflow_specs
        self._controller = controller
        self._initial_config = initial_config

        self._workflow_selector = WorkflowSelectorWidget(workflow_specs)
        self._config_widget: WorkflowConfigWidget | None = None
        self._running_workflows_widget = RunningWorkflowsWidget(controller)

        self._config_panel = pn.Column()
        self._start_button = pn.widgets.Button(
            name="Start Workflow",
            button_type="primary",
            disabled=True,
        )

        self._widget = self._create_widget()
        self._setup_callbacks()

    def _create_widget(self) -> pn.Column:
        """Create the main widget layout."""
        return pn.Column(
            pn.pane.HTML("<h3>Data Reduction Workflows</h3>"),
            pn.Row(
                pn.Column(
                    self._workflow_selector.widget,
                    self._config_panel,
                    self._start_button,
                    width=500,
                ),
                pn.Column(
                    self._running_workflows_widget.widget,
                    width=400,
                ),
            ),
        )

    def _setup_callbacks(self) -> None:
        """Setup callbacks for widget interactions."""
        self._workflow_selector._selector.param.watch(
            self._on_workflow_selected, "value"
        )
        self._start_button.on_click(self._on_start_workflow)

    def _on_workflow_selected(self, event) -> None:
        """Handle workflow selection change."""
        workflow_id = event.new
        if workflow_id is None:
            self._config_panel.objects = []
            self._start_button.disabled = True
            self._config_widget = None
            return

        workflow_spec = self._workflow_specs.workflows[workflow_id]

        # Use initial config if it matches the selected workflow
        config = None
        if self._initial_config and self._initial_config.identifier == workflow_id:
            config = self._initial_config

        self._config_widget = WorkflowConfigWidget(workflow_spec, config)
        self._config_panel.objects = [self._config_widget.widget]
        self._start_button.disabled = False

    def _on_start_workflow(self, event) -> None:
        """Handle start workflow button click."""
        if self._config_widget is None:
            return

        if not self._config_widget.validate_configuration():
            # In a real implementation, you might want to show an error message
            return

        workflow_id = self._workflow_selector.selected_workflow_id
        if workflow_id is None:
            return

        self._controller.start_workflow(
            workflow_id,
            self._config_widget.selected_sources,
            self._config_widget.parameter_values,
        )

        # Refresh running workflows display
        self._running_workflows_widget.refresh()

    def refresh_running_workflows(self) -> None:
        """Refresh the display of running workflows."""
        self._running_workflows_widget.refresh()

    @property
    def widget(self) -> pn.Column:
        """Get the Panel widget."""
        return self._widget
