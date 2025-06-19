# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import holoviews as hv
import panel as pn

from beamlime import Service
from beamlime.config.workflow_spec import WorkflowSpecs

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
        self._logger.info("Reduction dashboard initialized")

    def _setup_workflow_management(self) -> None:
        """Initialize workflow controller and reduction widget."""
        # Create workflow controller backed by config service
        self._workflow_controller = ConfigServiceWorkflowController(
            self._config_service
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

    def _on_workflow_specs_updated(self, workflow_specs: WorkflowSpecs) -> None:
        """Handle workflow specs updates from the controller."""
        self._logger.info(
            'Updating reduction widget with %d workflow specs',
            len(workflow_specs.workflows),
        )
        self._reduction_widget.update_workflow_specs(workflow_specs)

    def create_sidebar_content(self) -> pn.viewable.Viewable:
        """Create the sidebar content with workflow controls."""
        return pn.Column(
            pn.pane.Markdown("## Data Reduction"),
            self._reduction_widget.widget,
        )

    def create_main_content(self) -> pn.viewable.Viewable:
        """Create the main content area (empty for now)."""
        return pn.Column(
            pn.pane.HTML(
                "<div style='text-align: center; padding: 50px;'>"
                "<h3>Reduction Results</h3>"
                "<p style='color: #6c757d;'>Reduction plots will appear here once "
                "workflows are configured and running.</p>"
                "</div>"
            )
        )

    def _step(self):
        """Override step function to include workflow updates."""
        super()._step()
        # Process workflow configuration updates
        self._workflow_controller.process_config_updates()
        # Refresh running workflows display
        # TODO Do we need this call?
        self._reduction_widget.refresh_running_workflows()


def main() -> None:
    parser = Service.setup_arg_parser(description='Beamlime Reduction Dashboard')
    app = ReductionApp(**vars(parser.parse_args()))
    app.start(blocking=True)


if __name__ == "__main__":
    main()
