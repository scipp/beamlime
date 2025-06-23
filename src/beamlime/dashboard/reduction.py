# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import holoviews as hv
import panel as pn

from beamlime import Service
from beamlime.config.workflow_spec import WorkflowSpecs

from . import plots
from .dashboard import DashboardBase
from .reduction_widget import ReductionWidget
from .workflow_controller import ConfigServiceWorkflowController

pn.extension('holoviews', 'modal', template='material')
hv.extension('bokeh')


class ReductionApp(DashboardBase):
    """Reduction dashboard application."""

    def __init__(self, *, instrument: str = 'dummy', dev: bool = False, log_level: int):
        super().__init__(
            instrument=instrument,
            dev=dev,
            log_level=log_level,
            dashboard_name='reduction_dashboard',
        )

        self._setup_workflow_management()
        self._setup_reduction_streams()
        self._logger.info("Reduction dashboard initialized")

    def _setup_workflow_management(self) -> None:
        """Initialize workflow controller and reduction widget."""
        # Define source names that we want to monitor
        source_names = [
            'mantle_detector',
            'endcap_forward_detector',
            'endcap_backward_detector',
            'high_resolution_detector',
        ]

        # Create workflow controller backed by config service
        self._workflow_controller = ConfigServiceWorkflowController(
            self._config_service, source_names=source_names
        )

        # Initialize with empty workflow specs, will be updated via config service
        initial_specs = WorkflowSpecs()

        # Create reduction widget
        self._reduction_widget = ReductionWidget(
            workflow_specs=initial_specs,
            controller=self._workflow_controller,
        )

        # Subscribe to workflow specs updates
        self._workflow_controller.subscribe_to_workflow_specs_updates(
            self._on_workflow_specs_updated
        )

    def _setup_reduction_streams(self) -> None:
        """Initialize streams for reduction data."""
        source_names = {
            'mantle_detector',
            'endcap_forward_detector',
            'endcap_backward_detector',
            'high_resolution_detector',
        }
        self._iofd_pipe = self._reduction_stream_manager.get_stream(
            source_names=source_names,
            view_name='ess.powder.types.FocussedDataDspacing[ess.reduce.nexus.types.SampleRun]',
        )
        self._iofd2theta_pipe = self._reduction_stream_manager.get_stream(
            source_names=source_names,
            view_name='ess.powder.types.FocussedDataDspacingTwoTheta[ess.reduce.nexus.types.SampleRun]',
        )

    def _on_workflow_specs_updated(self, workflow_specs: WorkflowSpecs) -> None:
        """Handle workflow specs updates from the controller."""
        self._logger.info(
            'Updating reduction widget with %d workflow specs',
            len(workflow_specs.workflows),
        )
        self._reduction_widget.update_workflow_specs(workflow_specs)
        # Also refresh running workflows to ensure they show updated names
        self._reduction_widget.refresh_running_workflows()

    def create_sidebar_content(self) -> pn.viewable.Viewable:
        """Create the sidebar content with workflow controls."""
        return pn.Column(
            pn.pane.Markdown("## Data Reduction"),
            self._reduction_widget.widget,
        )

    def create_main_content(self) -> pn.viewable.Viewable:
        """Create the main content area (empty for now)."""
        self._iofd_plot = plots.AutoscalingPlot()
        iofd = hv.DynamicMap(
            self._iofd_plot.plot_lines, streams=[self._iofd_pipe]
        ).opts(shared_axes=False)
        self._iofd2theta_plot = plots.AutoscalingPlot()
        iofd2theta = hv.DynamicMap(
            self._iofd2theta_plot.plot_sum_of_2d, streams=[self._iofd2theta_pipe]
        ).opts(shared_axes=False)
        return pn.Column(
            pn.pane.HoloViews(iofd),
            pn.pane.HoloViews(iofd2theta),
        )

    def _step(self):
        """Override step function to include workflow updates."""
        super()._step()
        # Process workflow configuration updates
        self._workflow_controller.process_config_updates()


def main() -> None:
    parser = Service.setup_arg_parser(description='Beamlime Reduction Dashboard')
    app = ReductionApp(**vars(parser.parse_args()))
    app.start(blocking=True)


if __name__ == "__main__":
    main()
