import holoviews as hv
import numpy as np
import panel as pn
import param
from holoviews import streams

from beamlime.services.dashboard import DashboardApp as LegacyDashboard

pn.extension('holoviews', template='material')
hv.extension('bokeh')

legacy_app = LegacyDashboard(instrument='dream', dev=True)


class DashboardApp(param.Parameterized):
    """Main dashboard application with tab-dependent sidebar controls."""

    def __init__(self, **params):
        super().__init__(**params)
        self.active_tab = "Detectors"
        self._setup_monitor_streams()
        self._view_toggle = pn.widgets.RadioBoxGroup(
            name="View Mode",
            value='Current',
            options=["Current", "Cumulative"],
            inline=True,
            margin=(10, 0),
        )
        self._callback = None

    def _setup_monitor_streams(self):
        """Initialize streams for monitor data."""
        self._monitor1_pipe = streams.Pipe(data=None)
        self._monitor2_pipe = streams.Pipe(data=None)

        # Initialize with default data
        self._update_monitor_streams()

    def _update_monitor_streams(self):
        """Update the streams for monitor visualizations."""
        toa = np.linspace(-4, 6, 100)
        counts = 1.5 * np.exp(-(toa**2) / 2) + 0.05 * np.random.randn(100)
        toa += 4
        self._monitor1_pipe.send({'toa': toa, 'counts': counts})

        toa = np.linspace(-5, 5, 100)
        counts = np.exp(-(toa**2) / 2) + 0.05 * np.random.randn(100)
        toa += 5
        self._monitor2_pipe.send({'toa': toa, 'counts': counts})

    def _create_monitor_timeseries_plot(self, data):
        """Create monitor time series plot from stream data."""
        if not data:
            return hv.Curve([])

        curve = hv.Curve((data['toa'], data['counts']))
        return curve.opts(
            title="Monitor 1",
            width=400,
            height=300,
            xlabel="TOA",
            ylabel="Counts",
            color='blue',
            line_width=2,
        )

    def _create_monitor_profile_plot(self, data):
        """Create monitor beam profile plot from stream data."""
        if not data:
            return hv.Curve([])

        curve = hv.Curve((data['toa'], data['counts']))
        return curve.opts(
            title="Monitor 2",
            width=400,
            height=300,
            xlabel="TOA",
            ylabel="Counts",
            color='red',
            line_width=2,
        )

    def _create_status_plot(self, monitor1, monitor2):
        """Create status bar chart showing total counts from both monitors."""
        if not monitor1 or not monitor2:
            return hv.Bars([])

        monitor1_total = np.sum(monitor1['counts'])
        monitor2_total = np.sum(monitor2['counts'])

        data = [('Monitor 1', monitor1_total), ('Monitor 2', monitor2_total)]
        bars = hv.Bars(reversed(data), kdims='Monitor', vdims='Total Counts')

        return bars.opts(
            title="",
            width=300,
            height=100,
            color='lightblue',
            ylabel="Total Counts",
            xlabel="",
            invert_axes=True,
            show_legend=False,
            toolbar=None,
        )

    def create_monitor_plots(self) -> list:
        """Create plots for the Monitors tab."""
        timeseries_dmap = hv.DynamicMap(
            self._create_monitor_timeseries_plot,
            streams=[self._monitor1_pipe],
        ).opts(shared_axes=False)

        profile_dmap = hv.DynamicMap(
            self._create_monitor_profile_plot, streams=[self._monitor2_pipe]
        ).opts(shared_axes=False)

        return [pn.pane.HoloViews(timeseries_dmap), pn.pane.HoloViews(profile_dmap)]

    def create_status_plot(self):
        """Create status plot for the sidebar."""
        status_dmap = hv.DynamicMap(
            self._create_status_plot,
            streams={'monitor1': self._monitor1_pipe, 'monitor2': self._monitor2_pipe},
        ).opts(shared_axes=False)

        return pn.pane.HoloViews(status_dmap)

    def start_periodic_updates(self):
        """Start periodic updates for monitor streams."""
        if self._callback is None:
            self._callback = pn.state.add_periodic_callback(
                self._update_monitor_streams, period=1000
            )


def create_dashboard():
    """Create and configure the main dashboard."""
    dashboard = DashboardApp()

    monitor_plots = pn.FlexBox(*dashboard.create_monitor_plots())

    sidebar = pn.Column(
        pn.pane.Markdown("## Status"),
        dashboard.create_status_plot(),
        pn.pane.Markdown("## Controls"),
        pn.layout.Spacer(height=20),
    )

    # Configure template with dynamic sidebar
    template = pn.template.MaterialTemplate(
        title="DREAM — Live Data",
        sidebar=sidebar,
        main=monitor_plots,
        header_background='#2596be',
    )
    dashboard.start_periodic_updates()

    return template


if __name__ == "__main__":
    pn.serve(create_dashboard, port=5007, show=False, autoreload=True, dev=True)
