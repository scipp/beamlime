# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Preliminary example of a monitor dashboard application using Beamlime.
"""

import holoviews as hv
import panel as pn

from beamlime import Service

from . import plots
from .dashboard import DashboardBase
from .monitor_params import TOAEdgesParam

pn.extension('holoviews', template='material')
hv.extension('bokeh')


class DashboardApp(DashboardBase):
    """Monitor dashboard application."""

    def __init__(self, *, instrument: str = 'dummy', dev: bool = False, log_level: int):
        super().__init__(
            instrument=instrument,
            dev=dev,
            log_level=log_level,
            dashboard_name='monitors_dashboard',
        )

        self.toa_edges = TOAEdgesParam()
        self._setup_monitor_streams()
        self._view_toggle = pn.widgets.RadioBoxGroup(
            name="View Mode",
            value='Current',
            options=["Current", "Cumulative"],
            inline=True,
            margin=(10, 0),
        )

        self.toa_edges.subscribe(self._config_service)
        self._logger.info("Monitor dashboard initialized")

    def _setup_monitor_streams(self):
        """Initialize streams for monitor data."""
        self._monitor1_pipe = self._stream_manager.get_monitor('monitor1')
        self._monitor2_pipe = self._stream_manager.get_monitor('monitor2')

    def create_sidebar_content(self) -> pn.viewable.Viewable:
        """Create the sidebar content with status and controls."""
        status_dmap = hv.DynamicMap(
            plots.monitor_total_counts_bar_chart,
            streams={'monitor1': self._monitor1_pipe, 'monitor2': self._monitor2_pipe},
        ).opts(shared_axes=False)

        return pn.Column(
            pn.pane.Markdown("## Status"),
            pn.pane.HoloViews(status_dmap),
            pn.pane.Markdown("## Controls"),
            self._view_toggle,
            pn.Param(self.toa_edges.panel()),
            pn.layout.Spacer(height=20),
        )

    def create_main_content(self) -> pn.viewable.Viewable:
        """Create the main monitor plots content."""

        def _with_toggle(plot_fn):
            return pn.bind(plot_fn, view_mode=self._view_toggle.param.value)

        mon1 = hv.DynamicMap(
            _with_toggle(plots.plot_monitor1), streams=[self._monitor1_pipe]
        ).opts(shared_axes=False)
        mon2 = hv.DynamicMap(
            _with_toggle(plots.plot_monitor2), streams=[self._monitor2_pipe]
        ).opts(shared_axes=False)
        mons = hv.DynamicMap(
            _with_toggle(plots.plot_monitors_combined),
            streams={'monitor1': self._monitor1_pipe, 'monitor2': self._monitor2_pipe},
        ).opts(shared_axes=False)

        return pn.FlexBox(
            pn.pane.HoloViews(mons),
            pn.pane.HoloViews(mon1),
            pn.pane.HoloViews(mon2),
        )


def main() -> None:
    parser = Service.setup_arg_parser(description='Beamlime Dashboard')
    app = DashboardApp(**vars(parser.parse_args()))
    app.start(blocking=True)


if __name__ == "__main__":
    main()
