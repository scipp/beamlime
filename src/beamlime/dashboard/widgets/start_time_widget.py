# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import time
from typing import Any

import panel as pn

from beamlime.dashboard.controller_factory import Controller
from beamlime.dashboard.widgets.config_widget import ConfigWidget


class StartTimeWidget(ConfigWidget):
    """Widget for configuring StartTime with a button to set current time."""

    def _format_time(self, time_ns: int) -> str:
        """Convert nanoseconds since epoch to human-readable time format."""
        time_seconds = time_ns / 1_000_000_000
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_seconds))

    def _create_display_text(self, time_ns: int) -> str:
        """Create formatted display text for the time value."""
        formatted_time = self._format_time(time_ns)
        return f"**Last reset:**<br>{formatted_time}"

    def __init__(self, controller: Controller) -> None:
        """Initialize the start time widget."""
        defaults = controller.get_defaults()
        self._set_now_button = pn.widgets.Button(
            name="Reset counts", button_type="primary", width=150
        )
        self._value_display = pn.pane.Markdown(
            # Hack to fix vertical alignment of the text
            self._create_display_text(int(defaults['value'])),
            margin=(-7, 10),
        )

        # Hidden widgets to store the actual values for the controller. This should
        # probably be an IntInput instead, but the underlying Pydantic model currently
        # uses float.
        self._value_input = pn.widgets.FloatInput(
            value=defaults['value'], visible=False
        )

        self._unit_input = pn.widgets.TextInput(value="ns", visible=False)
        self._widgets = {'value': self._value_input, 'unit': self._unit_input}
        self._set_now_button.on_click(self._on_set_now)

        self._panel = pn.Column(
            pn.pane.Markdown("### Accumulation"),
            pn.Row(self._set_now_button, self._value_display),
        )

        super().__init__(controller)

    def _on_set_now(self, event) -> None:
        """Handle the 'Set to Current Time' button click."""
        current_time_ns = time.time_ns()
        self._value_input.value = current_time_ns
        self._update_display()

    def _update_display(self) -> None:
        """Update the display with the current value."""
        value = self._value_input.value
        self._value_display.object = self._create_display_text(value)

    def _on_config_change(self, value: dict[str, Any]) -> None:
        """Handle configuration value changes from the service."""
        super()._on_config_change(value)
        self._update_display()

    def _get_widgets(self) -> dict[str, pn.widgets.Widget]:
        """Return a dictionary of widgets for the configuration fields."""
        return self._widgets

    @property
    def panel(self) -> pn.viewable.Viewable:
        """Get the Panel viewable object for this widget."""
        return self._panel
