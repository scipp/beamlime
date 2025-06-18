# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import argparse
import logging

import holoviews as hv
import panel as pn

from beamlime import Service
from beamlime.dashboard.dashboard import DashboardBase
from beamlime.dashboard.data_service import DataService
from beamlime.dashboard.data_streams import MonitorStreamManager
from beamlime.dashboard.monitors_params import TOAEdgesParam

from . import plots

pn.extension('holoviews', template='material')
hv.extension('bokeh')


class DashboardApp(DashboardBase):
    """Monitor dashboard application."""

    def __init__(
        self,
        *,
        instrument: str = 'dummy',
        dev: bool = False,
        debug: bool = False,
        log_level: int = logging.INFO,
    ):
        super().__init__(
            instrument=instrument,
            dev=dev,
            debug=debug,
            log_level=log_level,
            dashboard_name='dashboard',
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

        # Subscribe TOA edges to config service
        self.toa_edges.subscribe(self._config_service)

        self._logger.info("Monitor dashboard initialized")

    def _create_data_services(self) -> dict[str, DataService]:
        """Create data services for monitor dashboard."""
        return {'monitor_data': DataService()}

    def _setup_monitor_streams(self):
        """Initialize streams for monitor data."""
        monitor_data_service = self._data_services['monitor_data']
        self._monitor_stream_manager = MonitorStreamManager(monitor_data_service)

        # Get streams for both monitors
        self._monitor1_pipe = self._monitor_stream_manager.get_stream('monitor1')
        self._monitor2_pipe = self._monitor_stream_manager.get_stream('monitor2')

        self._logger.info("Monitor streams setup complete")

    def _step(self):
        """Step function for periodic updates."""
        self._orchestrator.update()

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

    def get_dashboard_title(self) -> str:
        """Get the dashboard title."""
        return "DREAM — Live Data"


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Beamlime Dashboard')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    return parser


def main() -> None:
    parser = setup_arg_parser()
    app = DashboardApp(**vars(parser.parse_args()))
    app.start(blocking=True)


if __name__ == "__main__":
    main()
