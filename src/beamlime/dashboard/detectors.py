# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Preliminary example of a monitor dashboard application using Beamlime.
"""

import argparse
from typing import Any

import holoviews as hv
import panel as pn
from holoviews import streams

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

# Rudimentary configuration for detector plots. There is some overlap with the
# instrument configs in beamlime.config.instruments, but currently the latter is for the
# backend, defining projections and more. I expect that the dashboard will require more
# advanced config in the future, which might either be done by subclassing, or a more
# complex config system. Until we have a better idea of what exactly we will need to
# configure, we keep it simple and postpone the decision.
_dream = {
    'Endcap Backward': ('endcap_backward_detector', 'detector_xy_projection'),
    'High Resolution': ('high_resolution_detector', 'detector_xy_projection'),
    'Mantle': ('mantle_detector', 'mantle_front_layer'),
    'Endcap Forward': ('endcap_forward_detector', 'detector_xy_projection'),
}
_config = {'dream': _dream}


class DashboardApp(DashboardBase):
    """Detector dashboard application."""

    def __init__(self, *, instrument: str = 'dummy', dev: bool = False, log_level: int):
        super().__init__(
            instrument=instrument,
            dev=dev,
            log_level=log_level,
            dashboard_name='detectors_dashboard',
            port=5008,  # Default port for detectors dashboard
        )
        self._config = _config[instrument]

        # Load the module to register the instrument's workflows.
        self._instrument_module = get_config(instrument)
        self._processor_factory = instrument_registry[
            f'{self._instrument}_detectors'
        ].processor_factory

        self._setup_workflow_management()
        self._setup_detector_streams()
        self._view_toggle = pn.widgets.RadioBoxGroup(
            value='Current', options=["Current", "Cumulative"], inline=True
        )
        self._view_toggle_group = pn.Column(
            pn.pane.Markdown("### View Mode"), self._view_toggle
        )

        self._logger.info("Detector dashboard initialized")
        self._reset_controller = self._controller_factory.create(
            config_key=keys.DETECTOR_START_TIME.create_key()
        )

    def _setup_workflow_management(self) -> None:
        """Initialize workflow controller and reduction widget."""
        self._workflow_controller = WorkflowController.from_config_service(
            config_service=self._config_service,
            source_names=sorted(self._processor_factory.source_names),
            workflow_registry=self._processor_factory,
            data_service=self._data_services['detector_data'],
        )
        self._reduction_widget = ReductionWidget(controller=self._workflow_controller)

    def _setup_detector_streams(self):
        """Initialize streams for detector data."""
        self._streams = {
            key: self._detector_stream_manager.get_stream(*value)
            for key, value in self._config.items()
        }

    def create_sidebar_content(self) -> pn.viewable.Viewable:
        """Create the sidebar content with status and controls."""

        return pn.Column(
            pn.pane.Markdown("## Status"),
            pn.pane.Markdown("## Controls"),
            self._view_toggle_group,
            StartTimeWidget(self._reset_controller).panel,
            pn.pane.Markdown("## Workflows"),
            self._reduction_widget.widget,
        )

    def create_main_content(self) -> pn.viewable.Viewable:
        """Create the main monitor plots content."""
        panes = [
            self._setup_holoviews(pipe, extra_image_opts={'title': title})
            for title, pipe in self._streams.items()
        ]
        return pn.Column(*[pn.Row(*panes[i : i + 2]) for i in range(0, len(panes), 2)])

    def _with_toggle(self, func):
        def toggled(data, view_mode: str = 'Current'):
            if data is None:
                return func(data=None)
            da = data.cumulative if view_mode == 'Cumulative' else data.current
            return func(data=da)

        return pn.bind(toggled, view_mode=self._view_toggle.param.value)

    def _setup_holoviews(
        self, pipe: streams.Pipe, extra_image_opts: dict[str, Any] | None = None
    ) -> pn.pane.HoloViews:
        # Equal aspect should work well for detector images, since they always define
        # some rectangular area in space, typically with equal units. Pure logic views
        # are exceptions, but we have not added those in the dashboard yet.
        image_opts = {'aspect': 'equal'}
        image_opts.update(extra_image_opts or {})
        plotter = plots.AutoscalingPlot(value_margin_factor=0.1, image_opts=image_opts)
        return pn.pane.HoloViews(
            hv.DynamicMap(
                self._with_toggle(plotter.plot_2d), streams=[pipe], cache_size=1
            ).opts(shared_axes=False)
        )


def get_arg_parser() -> argparse.ArgumentParser:
    return Service.setup_arg_parser(description='Beamlime Dashboard')


def main() -> None:
    parser = get_arg_parser()
    app = DashboardApp(**vars(parser.parse_args()))
    app.start(blocking=True)


if __name__ == "__main__":
    main()
