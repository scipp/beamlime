# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Generic, TypeVar

import panel as pn
import pydantic

from .model_widget import ModelWidget

Model = TypeVar('Model')


class ConfigurationAdapter(ABC, Generic[Model]):
    """Abstract adapter for providing configuration data to generic widgets."""

    @property
    @abstractmethod
    def title(self) -> str:
        """Configuration title."""

    @property
    @abstractmethod
    def description(self) -> str:
        """Configuration description."""

    @property
    @abstractmethod
    def aux_source_names(self) -> dict[str, list[str]]:
        """Available auxiliary source names grouped by category."""

    @abstractmethod
    def model_class(self, aux_source_names: dict[str, str]) -> type[Model] | None:
        """
        Pydantic model class for parameters.

        Parameters
        ----------
        aux_source_names
            Selected auxiliary source names by category
        """

    @property
    @abstractmethod
    def source_names(self) -> list[str]:
        """Available source names."""

    @property
    @abstractmethod
    def initial_source_names(self) -> list[str]:
        """Initially selected source names."""

    @property
    @abstractmethod
    def initial_parameter_values(self) -> dict[str, Any]:
        """Initial parameter values."""

    @abstractmethod
    def start_action(
        self, selected_sources: list[str], parameter_values: Model
    ) -> bool:
        """
        Execute the start action with selected sources and parameters.

        Returns
        -------
        bool
            True if successful, False otherwise
        """


class ConfigurationWidget:
    """Generic widget for configuring parameters and source selection."""

    def __init__(self, config: ConfigurationAdapter) -> None:
        """
        Initialize generic configuration widget.

        Parameters
        ----------
        config
            Configuration adapter providing data and callbacks
        """
        self._config = config
        self._source_selector = self._create_source_selector()
        self._aux_source_selectors = self._create_aux_source_selectors()
        self._model_widget = self._create_model_widget()
        self._source_error_pane = pn.pane.HTML("", sizing_mode='stretch_width')
        self._widget = self._create_widget()

    def _create_source_selector(self) -> pn.widgets.MultiChoice:
        """Create source selection widget."""
        if (
            not self._config.initial_source_names
            and len(self._config.source_names) == 1
        ):
            initial_source_names = self._config.source_names
        else:
            initial_source_names = self._config.initial_source_names
        return pn.widgets.MultiChoice(
            name="Source Names",
            options=sorted(self._config.source_names),
            value=sorted(initial_source_names),
            placeholder="Select source names to apply workflow to",
            sizing_mode='stretch_width',
            margin=(0, 0, 0, 0),
        )

    def _create_aux_source_selectors(self) -> dict[str, pn.widgets.Select]:
        """Create auxiliary source selection widgets."""
        selectors = {}
        for category, options in self._config.aux_source_names.items():
            if not options:
                raise ValueError(
                    f"Empty options list for aux source category '{category}'"
                )

            selector = pn.widgets.Select(
                name=category,
                options=options,
                value=options[0],  # Default to first entry
                sizing_mode='stretch_width',
                margin=(0, 0, 0, 0),
            )
            selector.param.watch(self._on_aux_source_changed, 'value')
            selectors[category] = selector
        return selectors

    def _create_model_widget(self) -> ModelWidget | NoParamsWidget:
        """Create model widget based on current aux source selections."""
        aux_selections = {
            category: selector.value
            for category, selector in self._aux_source_selectors.items()
        }
        model_class = self._config.model_class(aux_selections)

        if model_class is None:
            return NoParamsWidget()
        else:
            return ModelWidget(
                model_class=model_class,
                initial_values=self._config.initial_parameter_values,
                show_descriptions=True,
                cards_collapsed=False,
            )

    def _on_aux_source_changed(self, event) -> None:
        """Handle auxiliary source selection change."""
        # Recreate model widget with new model class
        old_widget = self._model_widget
        self._model_widget = self._create_model_widget()

        # Replace widget in the column
        widget_index = None
        for i, item in enumerate(self._widget.objects):
            if item is old_widget.widget:
                widget_index = i
                break

        if widget_index is not None:
            self._widget.objects = (
                self._widget.objects[:widget_index]
                + [self._model_widget.widget]
                + self._widget.objects[widget_index + 1 :]
            )

    def _create_widget(self) -> pn.Column:
        """Create the main configuration widget."""
        components = [
            pn.pane.HTML(
                f"<h1>{self._config.title}</h1><p>{self._config.description}</p>"
            ),
            self._source_selector,
            self._source_error_pane,
        ]

        # Add auxiliary source selectors if any exist
        if self._aux_source_selectors:
            aux_row = pn.Row(
                *self._aux_source_selectors.values(), sizing_mode='stretch_width'
            )
            components.append(aux_row)

        components.append(self._model_widget.widget)

        return pn.Column(*components)

    @property
    def widget(self) -> pn.Column:
        """Get the Panel widget."""
        return self._widget

    @property
    def selected_sources(self) -> list[str]:
        """Get the selected source names."""
        return self._source_selector.value

    @property
    def selected_aux_sources(self) -> dict[str, str]:
        """Get the selected auxiliary source names."""
        return {
            category: selector.value
            for category, selector in self._aux_source_selectors.items()
        }

    @property
    def parameter_values(self):
        """Get current parameter values as a model instance."""
        return self._model_widget.parameter_values

    def validate_configuration(self) -> tuple[bool, list[str]]:
        """
        Validate that required fields are configured.

        Returns
        -------
        tuple[bool, list[str]]
            (is_valid, list_of_error_messages)
        """
        errors = []

        # Validate source selection
        if len(self.selected_sources) == 0:
            errors.append("Please select at least one source name.")
            self._highlight_source_error(True)
        else:
            self._highlight_source_error(False)

        # Validate parameter widgets
        param_valid, param_errors = self._model_widget.validate_parameters()
        if not param_valid:
            errors.extend(param_errors)

        return len(errors) == 0, errors

    def _highlight_source_error(self, has_error: bool) -> None:
        """Highlight source selector with error state."""
        if has_error:
            self._source_selector.styles = {
                'border': '2px solid #dc3545',
                'border-radius': '4px',
            }
            self._source_error_pane.object = (
                "<p style='color: #dc3545; margin: 5px 0; font-size: 0.9em;'>"
                "Please select at least one source name.</p>"
            )
        else:
            self._source_selector.styles = {'border': 'none'}
            self._source_error_pane.object = ""

    def clear_validation_errors(self) -> None:
        """Clear all validation error states."""
        self._highlight_source_error(False)
        self._model_widget.clear_validation_errors()


class ConfigurationModal:
    """Generic modal dialog for configuration."""

    def __init__(
        self,
        config: ConfigurationAdapter,
        start_button_text: str = "Start",
        success_callback: Callable[[], None] | None = None,
        error_callback: Callable[[str], None] | None = None,
    ) -> None:
        """
        Initialize generic configuration modal.

        Parameters
        ----------
        config
            Configuration adapter providing data and callbacks
        start_button_text
            Text for the start button
        success_callback
            Called when action completes successfully
        error_callback
            Called when an error occurs
        """
        self._config = config
        self._config_widget = ConfigurationWidget(config)
        self._success_callback = success_callback
        self._error_callback = error_callback
        self._error_pane = pn.pane.HTML("", sizing_mode='stretch_width')
        self._modal = self._create_modal(start_button_text)

    def _create_modal(self, start_button_text: str) -> pn.Modal:
        """Create the modal dialog."""
        start_button = pn.widgets.Button(name=start_button_text, button_type="primary")
        start_button.on_click(self._on_start_action)

        cancel_button = pn.widgets.Button(name="Cancel", button_type="light")
        cancel_button.on_click(self._on_cancel)

        content = pn.Column(
            self._config_widget.widget,
            self._error_pane,
            pn.Row(pn.Spacer(), cancel_button, start_button, margin=(10, 0)),
        )

        modal = pn.Modal(
            content,
            name=f"Configure {self._config.title}",
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

    def _on_start_action(self, event) -> None:
        """Handle start action button click."""
        # Clear previous errors
        self._config_widget.clear_validation_errors()
        self._error_pane.object = ""

        # Validate configuration
        is_valid, errors = self._config_widget.validate_configuration()

        if not is_valid:
            self._show_validation_errors(errors)
            return

        success = self._config.start_action(
            self._config_widget.selected_sources,
            self._config_widget.parameter_values,
        )

        if not success:
            error_message = f"Error: '{self._config.title}' is no longer available."
            self._show_action_error(error_message)
            if self._error_callback:
                self._error_callback(error_message)
            return

        self._modal.open = False
        if self._success_callback:
            self._success_callback()

    def _show_validation_errors(self, errors: list[str]) -> None:
        """Show validation errors inline."""
        error_html = (
            "<div style='background-color: #f8d7da; border: 1px solid #f5c6cb; "
            "border-radius: 4px; padding: 10px; margin: 10px 0;'>"
            "<h6 style='color: #721c24; margin: 0 0 10px 0;'>"
            "Please fix the following errors:</h6>"
            "<ul style='color: #721c24; margin: 0; padding-left: 20px;'>"
        )
        for error in errors:
            error_html += f"<li>{error}</li>"
        error_html += "</ul></div>"

        self._error_pane.object = error_html

    def _show_action_error(self, message: str) -> None:
        """Show action error inline."""
        error_html = (
            "<div style='background-color: #f8d7da; border: 1px solid #f5c6cb; "
            "border-radius: 4px; padding: 10px; margin: 10px 0;'>"
            f"<p style='color: #721c24; margin: 0;'>{message}</p>"
            "</div>"
        )
        self._error_pane.object = error_html

    def show(self) -> None:
        """Show the modal dialog."""
        self._modal.open = True

    @property
    def modal(self) -> pn.Modal:
        """Get the modal widget."""
        return self._modal


class NoParamsWidget:
    class EmptyModel(pydantic.BaseModel): ...

    def __init__(self):
        self.widget = pn.pane.HTML(
            "<div style='padding: 20px; text-align: center; color: #666; "
            "font-style: italic; border: 1px solid #ddd; border-radius: 4px; "
            "background-color: #f9f9f9;'>"
            "There are no parameters to configure."
            "</div>",
            sizing_mode='stretch_width',
        )

    @property
    def parameter_values(self) -> pydantic.BaseModel:
        """Return empty model serializing to empty dict."""
        return self.EmptyModel()

    def validate_parameters(self) -> tuple[bool, list[str]]:
        """Always valid when no parameters."""
        return True, []

    def clear_validation_errors(self) -> None:
        """No-op for no parameters."""
