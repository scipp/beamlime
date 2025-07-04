# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import panel as pn
import pydantic

from beamlime.config.workflow_spec import WorkflowId
from beamlime.dashboard.workflow_controller import WorkflowController

from .param_widget import ParamWidget
from .workflow_ui_helper import WorkflowUIHelper


class WorkflowConfigWidget:
    """Widget for configuring workflow parameters and source selection."""

    def __init__(
        self,
        workflow_id: WorkflowId,
        controller: WorkflowController,
    ) -> None:
        """
        Initialize workflow configuration widget.

        Parameters
        ----------
        workflow_id
            ID of the workflow
        controller
            Controller for workflow operations
        """
        self._workflow_id = workflow_id
        self._controller = controller
        self._ui_helper = WorkflowUIHelper(controller)
        if (spec := controller.get_workflow_spec(workflow_id)) is None:
            raise ValueError(f"Workflow with ID '{workflow_id}' does not exist.")
        self._workflow_spec = spec
        self._parameter_widgets: dict[str, ParamWidget] = {}
        self._source_selector = self._create_source_selector()
        self._parameter_panel = self._create_parameter_panel()
        self._widget = self._create_widget()

    def _create_source_selector(self) -> pn.widgets.MultiChoice:
        """Create source selection widget."""
        initial_sources = self._ui_helper.get_initial_source_names(self._workflow_id)
        return pn.widgets.MultiChoice(
            name="Source Names",
            options=self._workflow_spec.source_names,
            value=initial_sources,
            placeholder="Select source names to apply workflow to",
            sizing_mode='stretch_width',
        )

    def _create_parameter_panel(self) -> pn.Column:
        """Create panel containing all parameter widgets."""
        widget_data = self._ui_helper.get_parameter_widget_data(self._workflow_id)

        parameter_cards = []
        for field_name, data in widget_data.items():
            param_widget = ParamWidget(data['field_type'])
            param_widget.set_values(data['values'])
            self._parameter_widgets[field_name] = param_widget

            # Create card content
            card_content = [param_widget.panel()]

            # Add description if available
            if data['description']:
                description_pane = pn.pane.HTML(
                    "<p style='margin: 0 0 10px 0; color: #666; font-size: 0.9em;'>"
                    f"{data['description']}</p>"
                )
                card_content.insert(0, description_pane)

            card = pn.Card(
                *card_content,
                title=data['title'],
                margin=(5, 0),
                collapsed=False,
                width_policy='max',
                sizing_mode='stretch_width',
            )
            parameter_cards.append(card)

        return pn.Column(*parameter_cards, sizing_mode='stretch_width')

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
    def parameter_values(self) -> pydantic.BaseModel:
        """Get current parameter values as a dictionary."""
        widget_values = {
            name: widget.create_model()
            for name, widget in self._parameter_widgets.items()
        }
        return self._ui_helper.assemble_parameter_values(
            self._workflow_id, widget_values
        )

    def validate_configuration(self) -> bool:
        """Validate that required fields are configured."""
        return len(self.selected_sources) > 0


class WorkflowConfigModal:
    """Modal dialog for workflow configuration."""

    def __init__(
        self,
        workflow_id: WorkflowId,
        controller: WorkflowController,
    ) -> None:
        """
        Initialize workflow configuration modal.

        Parameters
        ----------
        workflow_id
            ID of the workflow
        controller
            Controller for workflow operations
        """
        self._workflow_id = workflow_id
        self._controller = controller
        if (spec := controller.get_workflow_spec(workflow_id)) is None:
            raise ValueError(f"Workflow with ID '{workflow_id}' does not exist.")
        self._workflow_spec = spec
        self._config_widget = WorkflowConfigWidget(workflow_id, controller)
        self._modal = self._create_modal()

    def _create_modal(self) -> pn.Modal:
        """Create the modal dialog."""
        start_button = pn.widgets.Button(name="Start Workflow", button_type="primary")
        start_button.on_click(self._on_start_workflow)

        cancel_button = pn.widgets.Button(name="Cancel", button_type="light")
        cancel_button.on_click(self._on_cancel)

        content = pn.Column(
            self._config_widget.widget,
            pn.Row(pn.Spacer(), cancel_button, start_button, margin=(10, 0)),
        )

        modal = pn.Modal(
            content,
            name=f"Configure {self._workflow_spec.name}",
            margin=20,
            width=800,
            height=800,
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
        if not self._config_widget.validate_configuration():
            self._show_error_modal("Please select at least one source name.")
            return

        success = self._controller.start_workflow(
            self._workflow_id,
            self._config_widget.selected_sources,
            self._config_widget.parameter_values,
        )

        if not success:
            self._show_error_modal(
                f"Error: Workflow '{self._workflow_spec.name}' "
                "is no longer available. Please select a different workflow."
            )
            return

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
