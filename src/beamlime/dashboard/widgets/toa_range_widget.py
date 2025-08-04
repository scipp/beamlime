# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import panel as pn

from beamlime.dashboard.controller_factory import Controller
from beamlime.dashboard.widgets.config_widget import ConfigWidget


class TOARangeWidget(ConfigWidget):
    """Widget for configuring TOA range with center/width layout."""

    def __init__(self, controller: Controller) -> None:
        """Initialize the TOA range widget."""
        defaults = controller.get_defaults()
        descriptions = controller.get_descriptions()

        # Calculate center and width from low/high defaults
        center_default = (defaults['low'] + defaults['high']) / 2
        width_default = defaults['high'] - defaults['low']

        self._enabled_input = pn.widgets.Checkbox(
            name="Enabled", value=defaults['enabled']
        )
        self._center_input = pn.widgets.FloatInput(
            name="Center",
            value=center_default,
            description="Center of the time range",
            width=100,
        )
        self._width_input = pn.widgets.FloatInput(
            name="Width",
            value=width_default,
            description="Width of the time range",
            width=100,
        )
        self._unit_select = pn.widgets.Select(
            name="Unit",
            options=["ns", "μs", "ms", "s"],
            value=defaults['unit'],
            description=descriptions['unit'],
            width=60,
        )

        self._widgets = {
            'enabled': self._enabled_input,
            'center': self._center_input,
            'width': self._width_input,
            'unit': self._unit_select,
        }

        self._panel = pn.Column(
            pn.pane.Markdown("### Time-of-arrival range"),
            self._enabled_input,
            pn.Row(
                self._center_input,
                self._width_input,
                self._unit_select,
                sizing_mode="stretch_width",
            ),
        )

        super().__init__(controller)

    def _get_widgets(self) -> dict[str, pn.widgets.Widget]:
        """Set up handlers for widget value changes."""
        return self._widgets

    @property
    def panel(self) -> pn.viewable.Viewable:
        """Get the Panel viewable object for this widget."""
        return self._panel
