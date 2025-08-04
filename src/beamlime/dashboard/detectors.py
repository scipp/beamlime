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
from beamlime.config import keys

from . import plots
from .controller_factory import RangeController
from .dashboard import DashboardBase
from .widgets.start_time_widget import StartTimeWidget
from .widgets.toa_range_widget import TOARangeWidget

pn.extension('holoviews', template='material')
hv.extension('bokeh')


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

        self._setup_detector_streams()
        self._view_toggle = pn.widgets.RadioBoxGroup(
            value='Current', options=["Current", "Cumulative"], inline=True
        )
        self._view_toggle_group = pn.Column(
            pn.pane.Markdown("### View Mode"), self._view_toggle
        )

        self._logger.info("Detector dashboard initialized")
        self._toa_controller = self._controller_factory.create(
            config_key=keys.DETECTOR_TOA_RANGE.create_key(),
            controller_cls=RangeController,
        )
        self._reset_controller = self._controller_factory.create(
            config_key=keys.DETECTOR_START_TIME.create_key()
        )

    def _setup_detector_streams(self):
        """Initialize streams for detector data."""
        self._bw_pipe = self._detector_stream_manager.get_stream(
            'endcap_backward_detector', 'endcap_backward'
        )
        self._hr_pipe = self._detector_stream_manager.get_stream(
            'high_resolution_detector', 'High-Res'
        )
        self._mantle_pipe = self._detector_stream_manager.get_stream(
            'mantle_detector', 'mantle_projection'
        )
        self._fw_pipe = self._detector_stream_manager.get_stream(
            'endcap_forward_detector', 'endcap_forward'
        )

    def create_sidebar_content(self) -> pn.viewable.Viewable:
        """Create the sidebar content with status and controls."""

        return pn.Column(
            pn.pane.Markdown("## Status"),
            pn.pane.Markdown("## Controls"),
            self._view_toggle_group,
            StartTimeWidget(self._reset_controller).panel,
            TOARangeWidget(self._toa_controller).panel,
        )

    def create_main_content(self) -> pn.viewable.Viewable:
        """Create the main monitor plots content."""

        return pn.Column(
            pn.Row(
                self._setup_holoviews(
                    self._bw_pipe, extra_image_opts={'title': 'Endcap Backward'}
                ),
                self._setup_holoviews(
                    self._hr_pipe, extra_image_opts={'title': 'High Resolution'}
                ),
            ),
            pn.Row(
                self._setup_holoviews(
                    self._mantle_pipe, extra_image_opts={'title': 'Mantle'}
                ),
                self._setup_holoviews(
                    self._fw_pipe, extra_image_opts={'title': 'Endcap Forward'}
                ),
            ),
        )

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
