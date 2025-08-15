# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import argparse

import holoviews as hv
import panel as pn

from beamlime import Service
from beamlime.config import instrument_registry, keys
from beamlime.config.instruments import get_config

from . import plots
from .dashboard import DashboardBase
from .widgets.reduction_widget import ReductionWidget
from .widgets.start_time_widget import StartTimeWidget
from .workflow_controller import WorkflowController

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
            port=5009,  # Default port for reduction dashboard
        )
        # Load the module to register the instrument's workflows.
        self._instrument_module = get_config(instrument)
        self._processor_factory = instrument_registry[instrument].processor_factory

        self._setup_workflow_management()
        self._setup_reduction_streams()
        self._reset_controller = self._controller_factory.create(
            config_key=keys.REDUCTION_START_TIME.create_key()
        )
        self._logger.info("Reduction dashboard initialized")

    def _setup_workflow_management(self) -> None:
        """Initialize workflow controller and reduction widget."""
        self._workflow_controller = WorkflowController.from_config_service(
            config_service=self._config_service,
            source_names=sorted(self._processor_factory.source_names),
            workflow_registry=self._processor_factory,
            data_service=self._data_services['data_reduction'],
        )
        self._reduction_widget = ReductionWidget(controller=self._workflow_controller)

    def _setup_reduction_streams(self) -> None:
        """Initialize streams for reduction data."""
        source_names = self._processor_factory.source_names
        self._focussed_d_pipe = self._reduction_stream_manager.get_stream(
            source_names=source_names,
            view_name='ess.powder.types.FocussedDataDspacing[ess.reduce.nexus.types.SampleRun]',
        )
        self._focussed_d2theta_pipe = self._reduction_stream_manager.get_stream(
            source_names=source_names,
            view_name='ess.powder.types.FocussedDataDspacingTwoTheta[ess.reduce.nexus.types.SampleRun]',
        )
        self._iofd_pipe = self._reduction_stream_manager.get_stream(
            source_names=source_names,
            view_name='ess.powder.types.IofDspacing[ess.reduce.nexus.types.SampleRun]',
        )
        self._iofd2theta_pipe = self._reduction_stream_manager.get_stream(
            source_names=source_names,
            view_name='ess.powder.types.IofDspacingTwoTheta[ess.reduce.nexus.types.SampleRun]',
        )

    def create_sidebar_content(self) -> pn.viewable.Viewable:
        """Create the sidebar content with workflow controls."""
        return pn.Column(
            pn.pane.Markdown("## Controls"),
            StartTimeWidget(self._reset_controller).panel,
            pn.pane.Markdown("## Data Reduction"),
            self._reduction_widget.widget,
        )

    def create_main_content(self) -> pn.viewable.Viewable:
        """Create the main content area (empty for now)."""
        self._foccussed_d_plot = plots.AutoscalingPlot(value_margin_factor=0.1)
        foc_d = hv.DynamicMap(
            self._foccussed_d_plot.plot_lines,
            streams=[self._focussed_d_pipe],
            cache_size=1,
        ).opts(shared_axes=False)
        self._foccussed_d2theta_plot = plots.AutoscalingPlot(value_margin_factor=0.1)
        foc_d2theta = hv.DynamicMap(
            self._foccussed_d2theta_plot.plot_sum_of_2d,
            streams=[self._focussed_d2theta_pipe],
            cache_size=1,
        ).opts(shared_axes=False)
        self._iofd_plot = plots.AutoscalingPlot(value_margin_factor=0.1)
        iofd = hv.DynamicMap(
            self._iofd_plot.plot_lines,
            streams=[self._iofd_pipe],
            cache_size=1,
        ).opts(shared_axes=False)
        self._iofd2theta_plot = plots.AutoscalingPlot(value_margin_factor=0.1)
        iofd2theta = hv.DynamicMap(
            self._iofd2theta_plot.plot_sum_of_2d,
            streams=[self._iofd2theta_pipe],
            cache_size=1,
        ).opts(shared_axes=False)
        return pn.Tabs(
            (
                "I(d) (vanadium normalized)",
                pn.Column(pn.pane.HoloViews(iofd), pn.pane.HoloViews(iofd2theta)),
            ),
            (
                "Focussed Data (before vanadium normalization)",
                pn.Column(pn.pane.HoloViews(foc_d), pn.pane.HoloViews(foc_d2theta)),
            ),
        )


def get_arg_parser() -> argparse.ArgumentParser:
    return Service.setup_arg_parser(description='Beamlime Reduction Dashboard')


def main() -> None:
    parser = get_arg_parser()
    app = ReductionApp(**vars(parser.parse_args()))
    app.start(blocking=True)


if __name__ == "__main__":
    main()
