# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import panel as pn

from beamlime.dashboard.controller_factory import Controller
from beamlime.dashboard.widgets.config_widget import ConfigWidget


class TOAEdgesWidget(ConfigWidget):
    """Widget for configuring TOAEdges with compact horizontal layout."""

    def __init__(self, controller: Controller) -> None:
        """Initialize the TOA edges widget."""
        defaults = controller.get_defaults()
        descriptions = controller.get_descriptions()
        self._low_input = pn.widgets.FloatInput(
            name="Low",
            value=defaults['low'],
            description=descriptions['low'],
            width=100,
        )
        self._high_input = pn.widgets.FloatInput(
            name="High",
            value=defaults['high'],
            description=descriptions['high'],
            width=100,
        )
        # Panel sliders do not support descriptions, see
        # https://github.com/holoviz/panel/issues/7499.
        self._num_bins_input = pn.widgets.EditableIntSlider(
            name="Number of histogram bins",
            value=defaults['num_bins'],
            start=1,
            end=2000,
        )
        self._unit_select = pn.widgets.Select(
            name="Unit",
            options=["ns", "μs", "ms", "s"],
            value=defaults['unit'],
            description=descriptions['unit'],
            width=60,
        )

        self._panel = pn.Column(
            pn.pane.Markdown("### Time-of-arrival bin edges"),
            pn.Row(
                self._low_input,
                self._high_input,
                self._unit_select,
                sizing_mode="stretch_width",
            ),
            self._num_bins_input,
        )

        super().__init__(controller)

    def _on_config_change(self, value: dict[str, Any]) -> None:
        """Handle configuration value changes from the service."""
        self._low_input.value = value['low']
        self._high_input.value = value['high']
        self._num_bins_input.value = value['num_bins']
        self._unit_select.value = value['unit']

    def _get_widgets(self) -> dict[str, pn.viewable.Viewable]:
        """Set up handlers for widget value changes."""
        return {
            'low': self._low_input,
            'high': self._high_input,
            'num_bins': self._num_bins_input,
            'unit': self._unit_select,
        }

    @property
    def panel(self) -> pn.viewable.Viewable:
        """Get the Panel viewable object for this widget."""
        return self._panel
