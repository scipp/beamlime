# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import pandas as pd
import panel as pn

from beamlime.config.workflow_spec import JobNumber
from beamlime.dashboard.job_service import JobService
from beamlime.dashboard.plot_service import PlotService


class PlotCreationWidget:
    """Widget for creating plots from job data."""

    def __init__(self, job_service: JobService, plot_service: PlotService) -> None:
        """
        Initialize plot creation widget.

        Parameters
        ----------
        job_service
            Service for accessing job data
        """
        self._job_service = job_service
        self._plot_service = plot_service
        self._selected_job: JobNumber | None = None
        self._plot_counter = 0  # Counter for unique plot tab names

        # Create UI components
        self._job_table = self._create_job_table()
        self._source_selector = self._create_source_selector()
        self._output_selector = self._create_output_selector()
        self._plot_options = self._create_plot_options()
        self._error_pane = pn.pane.HTML("", sizing_mode='stretch_width')
        self._create_button = self._create_plot_button()
        self._refresh_button = self._create_refresh_button()

        # Set up watchers
        self._job_table.param.watch(self._on_job_selection_change, 'selection')

        # Create main widget with tabs
        self._creation_tab = self._create_creation_tab()
        self._tabs = pn.Tabs(
            ("Create Plot", self._creation_tab), sizing_mode='stretch_width'
        )
        self._widget = self._tabs

        # Initial update
        self._update_job_table()

    def _create_job_table(self) -> pn.widgets.Tabulator:
        """Create job selection table."""
        return pn.widgets.Tabulator(
            name="Available Jobs",
            pagination='remote',
            page_size=10,
            sizing_mode='stretch_width',
            selectable=1,  # Single selection
            disabled=True,
            height=300,
            configuration={
                'columns': [
                    {'title': 'Job Number', 'field': 'job_number', 'width': 300},
                    {'title': 'Workflow', 'field': 'workflow_name', 'width': 300},
                    {'title': 'Source Names', 'field': 'source_names', 'width': 900},
                    # Future columns can be added here:
                    # {'title': 'Status', 'field': 'status', 'width': 100},
                    # {'title': 'Created', 'field': 'created_time', 'width': 150},
                ],
            },
        )

    def _create_source_selector(self) -> pn.widgets.MultiChoice:
        """Create source name selection widget."""
        return pn.widgets.MultiChoice(
            name="Source Names",
            options=[],
            value=[],
            placeholder="Select source names for plotting",
            sizing_mode='stretch_width',
            disabled=True,
        )

    def _create_output_selector(self) -> pn.widgets.Select:
        """Create output name selection widget."""
        return pn.widgets.Select(
            name="Output Name",
            options=[],
            value=None,
            sizing_mode='stretch_width',
            disabled=True,
            visible=False,
        )

    def _create_plot_options(self) -> pn.Card:
        """Create plot configuration options section."""
        # Placeholder widgets for plot configuration
        plot_type_selector = pn.widgets.Select(
            name="Plot Type",
            options=["Line Plot", "Scatter Plot", "Heatmap", "Histogram"],
            value="Line Plot",
            sizing_mode='stretch_width',
        )

        axis_config = pn.widgets.TextInput(
            name="Axis Configuration",
            placeholder="e.g., x='time', y='counts'",
            sizing_mode='stretch_width',
        )

        color_scheme = pn.widgets.Select(
            name="Color Scheme",
            options=["viridis", "plasma", "coolwarm", "tab10"],
            value="viridis",
            sizing_mode='stretch_width',
        )

        return pn.Card(
            pn.Column(
                plot_type_selector,
                axis_config,
                color_scheme,
                # More options can be added here in the future
                pn.pane.HTML(
                    "<p style='color: #666; font-style: italic;'>"
                    "Additional plot configuration options will be added here.</p>"
                ),
                sizing_mode='stretch_width',
            ),
            title="Plot Configuration",
            collapsed=False,
            sizing_mode='stretch_width',
        )

    def _create_plot_button(self) -> pn.widgets.Button:
        """Create the plot creation button."""
        button = pn.widgets.Button(
            name="Create Plot",
            button_type="primary",
            sizing_mode='stretch_width',
            disabled=True,
        )
        button.on_click(self._on_create_plot)
        return button

    def _create_refresh_button(self) -> pn.widgets.Button:
        """Create the refresh button."""
        button = pn.widgets.Button(
            name="Refresh Jobs",
            button_type="default",
            sizing_mode='stretch_width',
        )
        button.on_click(lambda event: self.refresh())
        return button

    def _create_creation_tab(self) -> pn.Column:
        """Create the plot creation tab content."""
        return pn.Column(
            pn.pane.HTML("<h2>Create Plot from Job Data</h2>"),
            pn.Row(self._refresh_button, sizing_mode='stretch_width'),
            self._job_table,
            self._source_selector,
            self._output_selector,
            self._plot_options,
            self._error_pane,
            self._create_button,
            sizing_mode='stretch_width',
        )

    def _update_job_table(self) -> None:
        """Update the job table with current job data."""
        job_data = []
        for job_number, workflow_id in self._job_service.job_info.items():
            sources = list(self._job_service.job_data.get(job_number, {}).keys())
            job_data.append(
                {
                    'job_number': job_number.hex,
                    'workflow_name': workflow_id.name,
                    'source_names': ', '.join(sources),
                }
            )

        if job_data:
            # Convert to DataFrame for Tabulator widget
            df = pd.DataFrame(job_data)
            self._job_table.value = df
        else:
            # Empty DataFrame with correct columns
            df = pd.DataFrame(columns=['job_number', 'workflow_name', 'source_names'])
            self._job_table.value = df

    def _on_job_selection_change(self, event) -> None:
        """Handle job selection change."""
        selection = event.new
        if not selection:
            self._selected_job = None
            self._update_dependent_widgets()
            return

        # Get selected job number from index
        selected_row = selection[0]
        job_number_str = self._job_table.value['job_number'].iloc[selected_row]
        self._selected_job = JobNumber(job_number_str)

        self._update_dependent_widgets()

    def _update_dependent_widgets(self) -> None:
        """Update source selector and output selector based on job selection."""
        if self._selected_job is None:
            # No job selected - disable everything
            self._source_selector.options = []
            self._source_selector.value = []
            self._source_selector.disabled = True
            self._output_selector.visible = False
            self._create_button.disabled = True
            return

        # Get available sources for selected job
        job_data = self._job_service.job_data.get(self._selected_job, {})
        available_sources = list(job_data.keys())

        # Update source selector
        self._source_selector.options = available_sources
        self._source_selector.value = []
        self._source_selector.disabled = len(available_sources) == 0

        # Check if we need output selector
        self._update_output_selector()

        # Enable create button if we have sources
        self._create_button.disabled = len(available_sources) == 0

    def _update_output_selector(self) -> None:
        """Update output selector based on current job and source selection."""
        if self._selected_job is None:
            self._output_selector.visible = False
            return

        # Check if any source has outputs (they all have the same outputs per backend guarantee)
        job_data = self._job_service.job_data.get(self._selected_job, {})
        output_names = set()

        for source_data in job_data.values():
            if isinstance(source_data, dict):
                output_names.update(source_data.keys())
                break  # Since all sources have same outputs, we only need to check one

        if output_names:
            self._output_selector.options = list(output_names)
            self._output_selector.value = (
                list(output_names)[0] if output_names else None
            )
            self._output_selector.visible = True
            self._output_selector.disabled = False
        else:
            self._output_selector.visible = False

    def _on_create_plot(self, event) -> None:
        """Handle create plot button click."""
        # Clear previous errors
        self._error_pane.object = ""

        # Validate selection
        is_valid, errors = self._validate_selection()
        if not is_valid:
            self._show_errors(errors)
            return

        # Placeholder for plot creation controller call
        try:
            self._create_plot_via_controller()
            # Show success message
            self._show_success("Plot created successfully!")
        except Exception as e:
            self._show_errors([f"Failed to create plot: {str(e)}"])

    def _validate_selection(self) -> tuple[bool, list[str]]:
        """Validate current selection."""
        errors = []

        if self._selected_job is None:
            errors.append("Please select a job.")

        if not self._source_selector.value:
            errors.append("Please select at least one source name.")

        if self._output_selector.visible and self._output_selector.value is None:
            errors.append("Please select an output name.")

        return len(errors) == 0, errors

    def _create_plot_via_controller(self) -> None:
        """Create plot via controller and add it as a new tab."""
        dmap = self._plot_service.create_plot(
            job_number=self._selected_job,
            source_names=self._source_selector.value,
            output_name=self._output_selector.value
            if self._output_selector.visible
            else None,
        )

        # Create HoloViews pane
        plot_pane = pn.pane.HoloViews(dmap, sizing_mode='stretch_width')

        # Generate tab name
        self._plot_counter += 1
        sources_str = "_".join(self._source_selector.value[:2])  # Limit length
        if len(self._source_selector.value) > 2:
            sources_str += f"_+{len(self._source_selector.value) - 2}"
        tab_name = f"Plot {self._plot_counter}: {sources_str}"

        # Add as new tab
        self._tabs.append((tab_name, plot_pane))

        # Switch to the new plot tab
        self._tabs.active = len(self._tabs) - 1

    def _get_plot_options(self) -> dict[str, Any]:
        """Get current plot configuration options."""
        # TODO: Extract values from plot options widgets
        return {
            'plot_type': 'line',  # placeholder
            'color_scheme': 'viridis',  # placeholder
        }

    def _show_errors(self, errors: list[str]) -> None:
        """Show validation errors."""
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

    def _show_success(self, message: str) -> None:
        """Show success message."""
        success_html = (
            "<div style='background-color: #d4edda; border: 1px solid #c3e6cb; "
            "border-radius: 4px; padding: 10px; margin: 10px 0;'>"
            f"<p style='color: #155724; margin: 0;'>{message}</p>"
            "</div>"
        )
        self._error_pane.object = success_html

    def refresh(self) -> None:
        """Refresh the widget with current job data."""
        self._update_job_table()
        if self._selected_job is not None:
            self._update_dependent_widgets()

    @property
    def widget(self) -> pn.Tabs:
        """Get the Panel widget."""
        return self._widget
