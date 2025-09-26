# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import panel as pn

from ..correlation_histogram import CorrelationHistogramController
from .configuration_widget import ConfigurationModal


class CorrelationHistogramWidget:
    """Widget for configuring a correlatio histogram."""

    def __init__(
        self, *, correlation_histogram_controller: CorrelationHistogramController
    ) -> None:
        self._correlation_histogram_controller = correlation_histogram_controller

        # Create the new 1D and 2D configuration buttons
        self._config_1d_button = pn.widgets.Button(
            name='1D Correlation Histogram', disabled=True, button_type='primary'
        )
        self._config_1d_button.on_click(self._on_config_1d_button_click)

        self._config_2d_button = pn.widgets.Button(
            name='2D Correlation Histogram', disabled=True, button_type='primary'
        )
        self._config_2d_button.on_click(self._on_config_2d_button_click)

        # Register for updates
        self._correlation_histogram_controller.register_update_subscriber(
            self._update_button_states
        )

        # Initial update
        self._update_button_states()

        # Container for modals - they need to be part of the served structure
        self._modal_container = pn.Column()

    def _update_button_states(self) -> None:
        """Update button states based on available timeseries."""
        timeseries_keys = self._correlation_histogram_controller.get_timeseries()
        num_timeseries = len(timeseries_keys)
        self._config_1d_button.disabled = num_timeseries < 1
        self._config_2d_button.disabled = num_timeseries < 2

    def _on_config_1d_button_click(self, event: Any) -> None:
        """Handle 1D configuration button clicks."""
        config = self._correlation_histogram_controller.create_1d_config()
        self._create_and_show_modal(config, ndim=1)

    def _on_config_2d_button_click(self, event: Any) -> None:
        """Handle 2D configuration button clicks."""
        config = self._correlation_histogram_controller.create_2d_config()
        self._create_and_show_modal(config, ndim=2)

    def _create_and_show_modal(self, config: Any, ndim: int) -> None:
        # Create and show configuration modal
        modal = ConfigurationModal(
            config=config, start_button_text=f"Create {ndim}D Correlation Histogram"
        )

        # Clear previous modals and add the new one
        self._modal_container.clear()
        self._modal_container.append(modal.modal)
        modal.show()

    @property
    def panel(self) -> pn.Column:
        """Get the panel widget for display."""
        return pn.Column(
            pn.Column(self._config_1d_button, self._config_2d_button),
            self._modal_container,
            sizing_mode='stretch_width',
        )
