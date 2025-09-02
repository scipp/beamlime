# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import argparse
import itertools

import holoviews as hv
import panel as pn

from beamlime import Service
from beamlime.config import keys
from beamlime.config.workflow_spec import WorkflowId

from . import plots
from .dashboard import DashboardBase
from .widgets.start_time_widget import StartTimeWidget

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
        self._setup_workflow_management('data_reduction')
        self._reset_controller = self._controller_factory.create(
            config_key=keys.REDUCTION_START_TIME.create_key()
        )
        self._logger.info("Reduction dashboard initialized")

    def create_sidebar_content(self) -> pn.viewable.Viewable:
        """Create the sidebar content with workflow controls."""
        return pn.Column(
            pn.pane.Markdown("## Controls"),
            StartTimeWidget(self._reset_controller).panel,
            pn.pane.Markdown("## Data Reduction"),
            self._reduction_widget.widget,
        )

    def create_main_content(self) -> pn.viewable.Viewable:
        """Create the main content area."""

        class PlotSumOf2d:
            def __init__(self) -> None:
                self._plot = plots.AutoscalingPlot(value_margin_factor=0.1)

            def __call__(self, data):
                return self._plot.plot_sum_of_2d(data)

        class PlotLines:
            def __init__(self) -> None:
                self._plot = plots.AutoscalingPlot(value_margin_factor=0.1)

            def __call__(self, data):
                return self._plot.plot_lines(data)

        print(list(self._workflow_registry))
        workflow_ids = [
            WorkflowId(
                instrument='dream', namespace='data_reduction', name=name, version=1
            )
            for name in ['powder_reduction', 'powder_reduction_with_vanadium']
        ]

        plot_clss = {
            'ess.powder.types.IofDspacing[ess.reduce.nexus.types.SampleRun]': PlotLines,
            'ess.powder.types.FocussedDataDspacing[ess.reduce.nexus.types.SampleRun]': PlotLines,
            'ess.powder.types.IofDspacingTwoTheta[ess.reduce.nexus.types.SampleRun]': PlotSumOf2d,
            'ess.powder.types.FocussedDataDspacingTwoTheta[ess.reduce.nexus.types.SampleRun]': PlotSumOf2d,
        }

        for workflow_id, (output_name, plot_cls) in itertools.product(
            workflow_ids, plot_clss.items()
        ):
            self._plot_service.register_plot(
                workflow_id=workflow_id, output_name=output_name, plot_cls=plot_cls
            )
        return pn.Row()


def get_arg_parser() -> argparse.ArgumentParser:
    return Service.setup_arg_parser(description='Beamlime Reduction Dashboard')


def main() -> None:
    parser = get_arg_parser()
    app = ReductionApp(**vars(parser.parse_args()))
    app.start(blocking=True)


if __name__ == "__main__":
    main()
