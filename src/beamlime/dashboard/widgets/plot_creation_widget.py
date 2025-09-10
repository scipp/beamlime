# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import pandas as pd
import panel as pn

from beamlime.config.workflow_spec import JobNumber
from beamlime.dashboard.job_service import JobService
from beamlime.dashboard.plotting import PlotterSpec
from beamlime.dashboard.plotting_controller import PlottingController

from .configuration_widget import ConfigurationAdapter, ConfigurationModal


class PlotConfigurationAdapter(ConfigurationAdapter):
    """Adapter for plot configuration modal."""

    def __init__(
        self,
        job_number: JobNumber,
        output_name: str | None,
        plot_spec: PlotterSpec,
        available_sources: list[str],
        plotting_controller: PlottingController,
        success_callback,
    ):
        self._job_number = job_number
        self._output_name = output_name
        self._plot_spec = plot_spec
        self._available_sources = available_sources
        self._plotting_controller = plotting_controller
        self._success_callback = success_callback

    @property
    def title(self) -> str:
        return f"Configure {self._plot_spec.title}"

    @property
    def description(self) -> str:
        return self._plot_spec.description

    @property
    def model_class(self) -> type:
        return self._plot_spec.params

    @property
    def source_names(self) -> list[str]:
        return self._available_sources

    @property
    def initial_source_names(self) -> list[str]:
        return self._available_sources

    @property
    def initial_parameter_values(self) -> dict[str, Any]:
        # We rely on defaults in the Pydantic model
        return {}

    def start_action(self, selected_sources: list[str], parameter_values: Any) -> bool:
        try:
            dmap = self._plotting_controller.create_plot(
                job_number=self._job_number,
                source_names=selected_sources,
                output_name=self._output_name,
                plot_name=self._plot_spec.name,
                params=parameter_values,
            )
            self._success_callback(dmap, selected_sources)
            return True
        except Exception:
            raise
            return False


class PlotCreationWidget:
    """Widget for creating plots from job data."""

    def __init__(
        self, job_service: JobService, plotting_controller: PlottingController
    ) -> None:
        """
        Initialize plot creation widget.

        Parameters
        ----------
        job_service:
            Service for accessing job data
        plotting_controller:
            Controller for creating plotters
        """
        self._job_service = job_service
        self._plotting_controller = plotting_controller
        self._selected_job: JobNumber | None = None
        self._selected_output: str | None = None
        self._plot_counter = 0  # Counter for unique plot tab names

        # Create UI components
        self._job_output_table = self._create_job_output_table()
        self._plot_selector = self._create_plot_selector()
        self._create_button = self._create_plot_button()
        self._refresh_button = self._create_refresh_button()

        # Set up watchers
        self._job_output_table.param.watch(
            self._on_job_output_selection_change, 'selection'
        )
        # Container for modals - they need to be part of the served structure
        self._modal_container = pn.Column()

        # Create main widget with tabs
        self._creation_tab = pn.Column(
            self._create_creation_tab(), self._modal_container
        )
        self._tabs = pn.Tabs(
            ("Create Plot", self._creation_tab), sizing_mode='stretch_width'
        )
        self._widget = self._tabs

        # Initial update
        self._update_job_output_table()

    def _create_job_output_table(self) -> pn.widgets.Tabulator:
        """Create job and output selection table with grouping."""
        return pn.widgets.Tabulator(
            name="Available Jobs and Outputs",
            pagination='remote',
            page_size=20,
            sizing_mode='stretch_width',
            selectable=1,  # Single selection
            disabled=True,
            height=800,
            groupby=['workflow_name', 'job_number'],
            configuration={
                'columns': [
                    {'title': 'Job Number', 'field': 'job_number', 'width': 300},
                    {'title': 'Workflow', 'field': 'workflow_name', 'width': 200},
                    {'title': 'Output Name', 'field': 'output_name', 'width': 200},
                    {'title': 'Source Names', 'field': 'source_names', 'width': 600},
                ],
            },
        )

    def _create_plot_selector(self) -> pn.widgets.Select:
        """Create plot type selection widget."""
        return pn.widgets.Select(
            name="Plot Type",
            options=[],
            value=None,
            sizing_mode='stretch_width',
            disabled=True,
        )

    def _create_plot_button(self) -> pn.widgets.Button:
        """Create the plot creation button."""
        button = pn.widgets.Button(
            name="Configure & Create Plot",
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
            self._job_output_table,
            pn.Row(
                self._refresh_button,
                self._plot_selector,
                self._create_button,
                sizing_mode='stretch_width',
            ),
            sizing_mode='stretch_width',
        )

    def _update_job_output_table(self) -> None:
        """Update the job and output table with current job data."""
        job_output_data = []
        for job_number, workflow_id in self._job_service.job_info.items():
            job_data = self._job_service.job_data.get(job_number, {})
            sources = list(job_data.keys())

            # Get output names from any source (they all have the same outputs per
            # #backend guarantee)
            output_names = set()
            for source_data in job_data.values():
                if isinstance(source_data, dict):
                    output_names.update(source_data.keys())
                    break  # Since all sources have same outputs, we only check one

            # If no outputs found, create a row with empty output name
            if not output_names:
                job_output_data.append(
                    {
                        'output_name': '',
                        'workflow_name': workflow_id.name,
                        'source_names': ', '.join(sources),
                        'job_number': job_number.hex,
                    }
                )
            else:
                # Create one row per output name
                job_output_data.extend(
                    [
                        {
                            'output_name': output_name,
                            'workflow_name': workflow_id.name,
                            'source_names': ', '.join(sources),
                            'job_number': job_number.hex,
                        }
                        for output_name in sorted(output_names)
                    ]
                )

        if job_output_data:
            # Convert to DataFrame for Tabulator widget
            df = pd.DataFrame(job_output_data)
        else:
            # Empty DataFrame with correct columns
            df = pd.DataFrame(
                columns=['job_number', 'workflow_name', 'output_name', 'source_names']
            )
        self._job_output_table.value = df

    def _on_job_output_selection_change(self, event) -> None:
        """Handle job and output selection change."""
        selection = event.new
        if not selection:
            self._selected_job = None
            self._selected_output = None
            self._update_dependent_widgets()
            return

        # Get selected job number and output name from index
        selected_row = selection[0]
        job_number_str = self._job_output_table.value['job_number'].iloc[selected_row]
        output_name = self._job_output_table.value['output_name'].iloc[selected_row]

        self._selected_job = JobNumber(job_number_str)
        self._selected_output = output_name if output_name else None

        self._update_dependent_widgets()

    def _with_disabled_selector(self) -> None:
        """Reset plot selector to default state."""
        self._plot_selector.options = []
        self._plot_selector.value = None
        self._plot_selector.disabled = True
        self._create_button.disabled = True

    def _update_dependent_widgets(self) -> None:
        """Update plot selector based on job and output selection."""
        if self._selected_job is None:
            return self._with_disabled_selector()

        # Get available sources for selected job
        job_data = self._job_service.job_data.get(self._selected_job, {})
        available_sources = list(job_data.keys())

        if not available_sources:
            return self._with_disabled_selector()

        # Update plot selector
        try:
            available_plots = self._plotting_controller.get_available_plotters(
                self._selected_job, self._selected_output
            )
            if available_plots:
                # Create options with plot class names
                options = {spec.title: name for name, spec in available_plots.items()}
                self._plot_selector.options = options
                self._plot_selector.value = next(iter(options)) if options else None
                self._plot_selector.disabled = False
                self._create_button.disabled = False
            else:
                self._with_disabled_selector()
        except Exception:
            return self._with_disabled_selector()

    def _on_create_plot(self, event) -> None:
        """Handle create plot button click."""
        # Validate selection
        is_valid, errors = self._validate_selection()
        if not is_valid:
            return

        # Get available sources
        job_data = self._job_service.job_data.get(self._selected_job, {})
        available_sources = list(job_data.keys())

        # Get plot spec
        plot_name = self._plot_selector.value
        spec = self._plotting_controller.get_spec(plot_name)

        # Create configuration adapter
        config = PlotConfigurationAdapter(
            job_number=self._selected_job,
            output_name=self._selected_output,
            plot_spec=spec,
            available_sources=available_sources,
            plotting_controller=self._plotting_controller,
            success_callback=self._on_plot_created,
        )

        # Create and show configuration modal
        modal = ConfigurationModal(config=config, start_button_text="Create Plot")

        # Add modal to container and show it
        self._modal_container.append(modal.modal)
        modal.show()

    def _on_plot_created(self, dmap, selected_sources: list[str]) -> None:
        """Handle successful plot creation."""
        # Create HoloViews pane
        plot_pane = pn.pane.HoloViews(dmap, sizing_mode='stretch_width')

        # Generate tab name
        self._plot_counter += 1
        sources_str = "_".join(selected_sources[:2])  # Limit length
        if len(selected_sources) > 2:
            sources_str += f"_+{len(selected_sources) - 2}"
        tab_name = f"Plot {self._plot_counter}: {sources_str}"

        # Add as new tab
        self._tabs.append((tab_name, plot_pane))

        # Switch to the new plot tab
        self._tabs.active = len(self._tabs) - 1

    def _validate_selection(self) -> tuple[bool, list[str]]:
        """Validate current selection."""
        errors = []

        if self._selected_job is None:
            errors.append("Please select a job and output.")

        if self._plot_selector.value is None:
            errors.append("Please select a plot type.")

        return len(errors) == 0, errors

    def refresh(self) -> None:
        """Refresh the widget with current job data."""
        self._update_job_output_table()
        if self._selected_job is not None:
            self._update_dependent_widgets()

    @property
    def widget(self) -> pn.Tabs:
        """Get the Panel widget."""
        return self._widget
