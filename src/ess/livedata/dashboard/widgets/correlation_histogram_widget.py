# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import pandas as pd
import panel as pn

from ..correlation_histogram import CorrelationHistogramController
from .configuration_widget import ConfigurationModal


class CorrelationHistogramWidget:
    """Widget for configuring a correlatio histogram."""

    def __init__(
        self, *, correlation_histogram_controller: CorrelationHistogramController
    ) -> None:
        self._correlation_histogram_controller = correlation_histogram_controller

        # Create the tabulator widget
        self._tabulator = pn.widgets.Tabulator(
            value=pd.DataFrame(columns=['source_name', 'job_id', 'output_name']),
            selectable='checkbox-single',
            sortable=True,
            disabled=True,
            pagination='remote',
            page_size=10,
            sizing_mode='stretch_width',
        )

        # Create the configuration button
        self._config_button = pn.widgets.Button(
            name='Create Configuration',
            disabled=True,
            button_type='primary',
        )
        self._config_button.on_click(self._on_config_button_click)

        # Register for updates and set up selection watching
        self._correlation_histogram_controller.register_update_subscriber(
            self._update_timeseries_list
        )
        self._tabulator.param.watch(self._on_selection_change, 'selection')

        # Initial update
        self._update_timeseries_list()

        # Container for modals - they need to be part of the served structure
        self._modal_container = pn.Column()

    def _update_timeseries_list(self) -> None:
        """Update the tabulator with current timeseries data."""
        timeseries_keys = self._correlation_histogram_controller.get_timeseries()
        data = pd.DataFrame(
            [
                {
                    'source_name': key.job_id.source_name,
                    'job_id': str(key.job_id.job_number)[:8],
                    'output_name': key.output_name or '',
                    'result_key': key,
                }
                for key in set(timeseries_keys) - {'result_key'}
            ]
        )
        self._tabulator.value = data
        self._on_selection_change()  # Update button state

    def _on_selection_change(self, *args: Any) -> None:
        """Handle selection changes in the tabulator."""
        selection = self._tabulator.selection
        num_selected = len(selection)

        # Enable button only for 1 or 2 selections
        self._config_button.disabled = num_selected not in (1, 2)

        # Update button text based on selection
        if num_selected == 1:
            self._config_button.name = 'Create 1D Configuration'
        elif num_selected == 2:
            self._config_button.name = 'Create 2D Configuration'
        else:
            self._config_button.name = 'Create Configuration'

    def _on_config_button_click(self, event: Any) -> None:
        """Handle configuration button clicks."""
        selection = self._tabulator.selection
        indices = self._tabulator.value.iloc[selection].index
        config = self._correlation_histogram_controller.create_config(
            [self._result_key.iloc[i] for i in indices]
        )

        # Create and show configuration modal
        modal = ConfigurationModal(
            config=config, start_button_text="Create Correlation Histogram"
        )

        # Clear previous modals and add the new one
        self._modal_container.clear()
        self._modal_container.append(modal.modal)
        modal.show()

    @property
    def panel(self) -> pn.Column:
        """Get the panel widget for display."""
        return pn.Column(
            pn.pane.Markdown("## Correlation Histogram Configuration"),
            pn.pane.Markdown(
                "Select one or two timeseries to create a correlation histogram:"
            ),
            self._tabulator,
            self._config_button,
            self._modal_container,
            sizing_mode='stretch_width',
        )
