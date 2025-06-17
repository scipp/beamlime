import logging
import threading
from contextlib import ExitStack

import holoviews as hv
import numpy as np
import panel as pn
import param
import scipp as sc
from holoviews import streams

from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.config.streams import stream_kind_to_topic
from beamlime.core.message import StreamKind
from beamlime.dashboard.config_service import ConfigService
from beamlime.dashboard.kafka_bridge import KafkaBridge
from beamlime.dashboard.monitors_params import TOAEdgesParam
from beamlime.kafka import consumer as kafka_consumer
from beamlime.services.dashboard import DashboardApp as LegacyDashboard

from .data_forwarder import DataForwarder
from .data_key import MonitorDataKey
from .data_service import DataService
from .orchestrator import Orchestrator
from .scipp_to_holoviews import to_holoviews
from .subscribers import ComponentDataSubscriber

pn.extension('holoviews', template='material')
hv.extension('bokeh')

legacy_app = LegacyDashboard(instrument='dream', dev=True)


def remove_bokeh_logo(plot, element):
    plot.state.toolbar.logo = None


class DashboardApp(param.Parameterized):
    """Main dashboard application with tab-dependent sidebar controls."""

    logscale = param.Boolean(default=False, doc="Enable log scale for monitor plots")

    def __init__(self, **params):
        super().__init__(**params)
        self._instrument = 'dream'
        self._logger = logging.getLogger(__name__)
        self.toa_edges = TOAEdgesParam()
        self._num_edges = self.toa_edges.num_edges
        self._setup_monitor_streams()
        self._view_toggle = pn.widgets.RadioBoxGroup(
            name="View Mode",
            value='Current',
            options=["Current", "Cumulative"],
            inline=True,
            margin=(10, 0),
        )
        self._callback = None
        self._exit_stack = ExitStack()
        self._exit_stack.__enter__()
        self._setup_config_service()

    def _setup_config_service(self) -> None:
        kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
        _, consumer = self._exit_stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=self._instrument)
        )
        self._kafka_bridge = KafkaBridge(
            topic=stream_kind_to_topic(
                instrument=self._instrument, kind=StreamKind.BEAMLIME_CONFIG
            ),
            kafka_config=kafka_downstream_config,
            consumer=consumer,
            logger=self._logger,
        )
        self._config_service = ConfigService(message_bridge=self._kafka_bridge)
        self.toa_edges.subscribe(self._config_service)
        # Second subscription for fake data creation
        # self._config_service.subscribe(
        #    key=self.toa_edges.config_key, callback=self._on_toa_edges_update
        # )

        self._kafka_bridge_thread = threading.Thread(target=self._kafka_bridge.start)

    def _setup_monitor_streams(self):
        """Initialize streams for monitor data."""
        self._monitor1_pipe = streams.Pipe(data=None)
        self._monitor2_pipe = streams.Pipe(data=None)

        monitor_data_service = DataService()
        subscriber1 = ComponentDataSubscriber(
            component_key=MonitorDataKey(component_name='monitor1', view_name=''),
            pipe=self._monitor1_pipe,
        )
        subscriber2 = ComponentDataSubscriber(
            component_key=MonitorDataKey(component_name='monitor2', view_name=''),
            pipe=self._monitor2_pipe,
        )
        monitor_data_service.register_subscriber(subscriber1)
        monitor_data_service.register_subscriber(subscriber2)
        data_services = {'monitor_data': monitor_data_service}
        forwarder = DataForwarder(data_services=data_services)
        self._orchestrator = Orchestrator(legacy_app._source, forwarder)

        # Initialize with default data
        self._update_monitor_streams()

    def _update_monitor_streams(self):
        """Update the streams for monitor visualizations."""
        self._orchestrator.update()
        return
        toa = np.linspace(-4, 6, self._num_edges)
        counts = 1.5 * np.exp(-(toa**2) / 2) + 0.05 * np.random.randn(self._num_edges)
        toa += 4
        self._monitor1_pipe.send({'toa': toa, 'counts': counts})

        toa = np.linspace(-5, 5, self._num_edges)
        counts = np.exp(-(toa**2) / 2) + 0.05 * np.random.randn(self._num_edges)
        toa += 5
        self._monitor2_pipe.send({'toa': toa, 'counts': counts})

    def _on_toa_edges_update(self, model) -> None:
        """Callback for TOA edges config updates."""
        kwargs = model.model_dump()
        if 'num_edges' in kwargs:
            self._num_edges = kwargs['num_edges']
            self._update_monitor_streams()

    def _plot_monitor1(self, data) -> hv.Curve:
        """Create monitor 1 plot."""
        view_mode = 'Current'
        if not data:
            return hv.Curve([])

        if isinstance(data, dict):
            curve = hv.Curve((data['toa'], data['counts']), label='Monitor 1')
        else:
            data = data.cumulative if view_mode == 'Cumulative' else data.current
            data = data.assign_coords(
                time_of_arrival=sc.midpoints(data.coords['time_of_arrival'])
            )
            curve = to_holoviews(data)
        return curve.opts(
            title="Monitor 1",
            xlabel="TOA",
            ylabel="Counts",
            color='blue',
            line_width=2,
            responsive=True,
            height=400,
            hooks=[remove_bokeh_logo],
        )

    def _plot_monitor2(self, data) -> hv.Curve:
        """Create monitor 2 plot."""
        view_mode = 'Current'
        if not data:
            return hv.Curve([])

        if isinstance(data, dict):
            curve = hv.Curve((data['toa'], data['counts']), label='Monitor 2')
        else:
            data = data.cumulative if view_mode == 'Cumulative' else data.current
            data = data.assign_coords(
                time_of_arrival=sc.midpoints(data.coords['time_of_arrival'])
            )
            curve = to_holoviews(data)
        return curve.opts(
            title="Monitor 2",
            xlabel="TOA",
            ylabel="Counts",
            color='red',
            line_width=2,
            responsive=True,
            height=400,
            hooks=[remove_bokeh_logo],
        )

    def _plot_monitors(self, monitor1, monitor2, logscale: bool = False):
        """Combined plot of monitor1 and 2."""
        mon1 = self._plot_monitor1(monitor1)
        mon2 = self._plot_monitor2(monitor2)
        mons = mon1 * mon2
        # DynamicMap does not support changes the scale after creation. Need to
        # find a different solution. Recreate the dmaps?
        return mons.opts(title="Monitors", logy=logscale)

    def _plot_monitor_total_counts(self, monitor1, monitor2):
        """Create status bar chart showing total counts from both monitors."""
        if not monitor1 or not monitor2:
            return hv.Bars([])

        if isinstance(monitor1, dict):
            monitor1_total = np.sum(monitor1['counts'])
            monitor2_total = np.sum(monitor2['counts'])
        else:
            monitor1_total = np.sum(monitor1.current.values)
            monitor2_total = np.sum(monitor2.current.values)

        data = [('Monitor 1', monitor1_total), ('Monitor 2', monitor2_total)]
        bars = hv.Bars(reversed(data), kdims='Monitor', vdims='Total Counts')

        return bars.opts(
            title="",
            height=100,
            color='lightblue',
            ylabel="Total Counts",
            xlabel="",
            invert_axes=True,
            show_legend=False,
            toolbar=None,
            responsive=True,
        )

    def create_monitor_plots(self) -> list:
        """Create plots for the Monitors tab."""
        mon1 = hv.DynamicMap(self._plot_monitor1, streams=[self._monitor1_pipe]).opts(
            shared_axes=False
        )
        mon2 = hv.DynamicMap(self._plot_monitor2, streams=[self._monitor2_pipe]).opts(
            shared_axes=False
        )
        plot_with_scale = pn.bind(self._plot_monitors, logscale=self.param.logscale)
        mons = hv.DynamicMap(
            plot_with_scale,
            streams={
                'monitor1': self._monitor1_pipe,
                'monitor2': self._monitor2_pipe,
            },
        ).opts(shared_axes=False)

        return [
            pn.pane.HoloViews(mons),
            pn.pane.HoloViews(mon1),
            pn.pane.HoloViews(mon2),
        ]

    def create_status_plot(self):
        """Create status plot for the sidebar."""
        status_dmap = hv.DynamicMap(
            self._plot_monitor_total_counts,
            streams={'monitor1': self._monitor1_pipe, 'monitor2': self._monitor2_pipe},
        ).opts(shared_axes=False)

        return pn.pane.HoloViews(status_dmap)

    def _step(self):
        """Step function for periodic updates."""
        self._update_monitor_streams()
        self._config_service.process_incoming_messages()

    def start_periodic_updates(self):
        """Start periodic updates for monitor streams."""
        if self._callback is None:
            self._callback = pn.state.add_periodic_callback(self._step, period=1000)


def create_dashboard():
    """Create and configure the main dashboard."""
    dashboard = DashboardApp()
    dashboard._kafka_bridge_thread.start()

    monitor_plots = pn.FlexBox(*dashboard.create_monitor_plots())

    sidebar = pn.Column(
        pn.pane.Markdown("## Status"),
        dashboard.create_status_plot(),
        pn.pane.Markdown("## Controls"),
        pn.Param(dashboard.toa_edges.panel()),
        pn.Param(
            dashboard,
            parameters=['logscale'],
            show_name=False,
            width=300,
            margin=(10, 0),
        ),
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
