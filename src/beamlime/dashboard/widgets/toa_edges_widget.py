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
        # Create input widgets with compact sizing
        self._low_input = pn.widgets.FloatInput(name="Low", value=0.0, width=80)
        self._high_input = pn.widgets.FloatInput(
            name="High", value=1000.0 / 14, width=80
        )
        self._num_bins_input = pn.widgets.IntInput(
            name="Bins", value=100, start=1, width=60
        )
        self._unit_select = pn.widgets.Select(
            name="Unit", options=["ns", "us", "ms", "s"], value="ms", width=60
        )

        # Create compact row layout
        self._panel = pn.Row(
            self._low_input,
            self._high_input,
            self._num_bins_input,
            self._unit_select,
            margin=(5, 5),
            sizing_mode="stretch_width",
        )

        super().__init__(controller)

    def _on_config_change(self, value: dict[str, Any]) -> None:
        """Handle configuration value changes from the service."""
        self._low_input.value = value.get("low", 0.0)
        self._high_input.value = value.get("high", 1000.0 / 14)
        self._num_bins_input.value = value.get("num_bins", 100)
        self._unit_select.value = value.get("unit", "ms")

    def _setup_widget_handlers(self) -> None:
        pn.bind(
            self._on_widget_change,
            low=self._low_input,
            high=self._high_input,
            num_bins=self._num_bins_input,
            unit=self._unit_select,
            watch=True,
        )

    @property
    def panel(self) -> pn.viewable.Viewable:
        """Get the Panel viewable object for this widget."""
        return self._panel
