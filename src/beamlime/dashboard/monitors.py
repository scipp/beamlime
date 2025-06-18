import argparse
import logging
import threading
from contextlib import ExitStack

import holoviews as hv
import numpy as np
import panel as pn

from beamlime import Service, ServiceBase
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.config.streams import stream_kind_to_topic
from beamlime.core.message import StreamKind
from beamlime.dashboard.config_service import ConfigService
from beamlime.dashboard.kafka_bridge import KafkaBridge
from beamlime.dashboard.monitors_params import TOAEdgesParam
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    KafkaToDa00Adapter,
)
from beamlime.kafka.source import KafkaMessageSource

from . import plots
from .data_forwarder import DataForwarder
from .data_service import DataService
from .data_streams import MonitorStreamManager
from .orchestrator import Orchestrator

pn.extension('holoviews', template='material')
hv.extension('bokeh')


class DashboardApp(ServiceBase):
    """Main dashboard application with tab-dependent sidebar controls."""

    # logscale = param.Boolean(default=False, doc="Enable log scale for monitor plots")

    def __init__(
        self,
        *,
        instrument: str = 'dummy',
        dev: bool,
        debug: bool = False,
        log_level: int = logging.INFO,
    ):
        name = f'{instrument}_dashboard'
        super().__init__(name=name, log_level=log_level)
        self._instrument = instrument

        self._exit_stack = ExitStack()
        self._exit_stack.__enter__()

        self.toa_edges = TOAEdgesParam()
        self._num_edges = self.toa_edges.num_edges
        self._source = self._setup_kafka_consumer()
        self._setup_monitor_streams()
        self._view_toggle = pn.widgets.RadioBoxGroup(
            name="View Mode",
            value='Current',
            options=["Current", "Cumulative"],
            inline=True,
            margin=(10, 0),
        )
        self._callback = None
        self._setup_config_service()
        self._logger.info("DashboardApp initialized")

    def _setup_config_service(self) -> None:
        kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
        _, consumer = self._exit_stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=self._instrument)
        )
        self._kafka_bridge = KafkaBridge(
            kafka_config=kafka_downstream_config, consumer=consumer, logger=self._logger
        )
        self._config_service = ConfigService(message_bridge=self._kafka_bridge)
        self.toa_edges.subscribe(self._config_service)
        # Second subscription for fake data creation
        # self._config_service.subscribe(
        #    key=self.toa_edges.config_key, callback=self._on_toa_edges_update
        # )

        self._kafka_bridge_thread = threading.Thread(
            target=self._kafka_bridge.start, daemon=True
        )
        self._logger.info("Config service setup complete")

    def _setup_kafka_consumer(self) -> AdaptingMessageSource:
        consumer_config = load_config(
            namespace=config_names.reduced_data_consumer, env=''
        )
        kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
        data_topic = stream_kind_to_topic(
            instrument=self._instrument, kind=StreamKind.BEAMLIME_DATA
        )
        consumer = self._exit_stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=[data_topic],
                config={**consumer_config, **kafka_downstream_config},
                group='dashboard',
            )
        )
        return AdaptingMessageSource(
            source=KafkaMessageSource(consumer=consumer, num_messages=1000),
            adapter=ChainedAdapter(
                first=KafkaToDa00Adapter(stream_kind=StreamKind.BEAMLIME_DATA),
                second=Da00ToScippAdapter(),
            ),
        )

    def _setup_monitor_streams(self):
        """Initialize streams for monitor data."""
        monitor_data_service = DataService()
        self._monitor_stream_manager = MonitorStreamManager(monitor_data_service)

        # Get streams for both monitors
        self._monitor1_pipe = self._monitor_stream_manager.get_stream('monitor1')
        self._monitor2_pipe = self._monitor_stream_manager.get_stream('monitor2')

        data_services = {'monitor_data': monitor_data_service}
        forwarder = DataForwarder(data_services=data_services)
        self._orchestrator = Orchestrator(self._source, forwarder)

        # Initialize with default data
        self._update_monitor_streams()
        self._logger.info("Monitor streams setup complete")

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
        view_mode = self._view_toggle.value
        return plots.plot_monitor1(data, view_mode=view_mode)

    def _plot_monitor2(self, data) -> hv.Curve:
        """Create monitor 2 plot."""
        view_mode = self._view_toggle.value
        return plots.plot_monitor2(data, view_mode=view_mode)

    def _plot_monitors(self, monitor1, monitor2, logscale: bool = False):
        """Combined plot of monitor1 and 2."""
        view_mode = self._view_toggle.value
        return plots.plot_monitors_combined(
            monitor1, monitor2, logscale=logscale, view_mode=view_mode
        )

    def create_monitor_plots(self) -> list:
        """Create plots for the Monitors tab."""
        mon1 = hv.DynamicMap(self._plot_monitor1, streams=[self._monitor1_pipe]).opts(
            shared_axes=False
        )
        mon2 = hv.DynamicMap(self._plot_monitor2, streams=[self._monitor2_pipe]).opts(
            shared_axes=False
        )
        # plot_with_scale = pn.bind(self._plot_monitors, logscale=self.param.logscale)
        plot_with_scale = pn.bind(self._plot_monitors, logscale=False)
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
            plots.monitor_total_counts_bar_chart,
            streams={'monitor1': self._monitor1_pipe, 'monitor2': self._monitor2_pipe},
        ).opts(shared_axes=False)

        return pn.pane.HoloViews(status_dmap)

    def _step(self):
        """Step function for periodic updates."""
        try:
            self._update_monitor_streams()
            self._config_service.process_incoming_messages()
        except Exception as e:
            self._logger.error("Error in periodic update step: %s", e)

    def start_periodic_updates(self):
        """Start periodic updates for monitor streams."""
        if self._callback is None:
            self._callback = pn.state.add_periodic_callback(self._step, period=1000)
            self._logger.info("Periodic updates started")

    def create_layout(self) -> pn.template.MaterialTemplate:
        monitor_plots = pn.FlexBox(*self.create_monitor_plots())
        sidebar = pn.Column(
            pn.pane.Markdown("## Status"),
            self.create_status_plot(),
            pn.pane.Markdown("## Controls"),
            pn.Param(self.toa_edges.panel()),
            # pn.Param(
            #    dashboard,
            #    parameters=['logscale'],
            #    show_name=False,
            #    width=300,
            #    margin=(10, 0),
            # ),
            pn.layout.Spacer(height=20),
        )

        # Configure template with dynamic sidebar
        template = pn.template.MaterialTemplate(
            title="DREAM — Live Data",
            sidebar=sidebar,
            main=monitor_plots,
            header_background='#2596be',
        )
        self.start_periodic_updates()

        return template

    def _start_impl(self) -> None:
        self._kafka_bridge_thread.start()

    def run_forever(self) -> None:
        import atexit

        atexit.register(self.stop)
        try:
            pn.serve(
                self.create_layout, port=5007, show=False, autoreload=True, dev=True
            )
        except KeyboardInterrupt:
            self._logger.info("Keyboard interrupt received, shutting down...")
            self.stop()

    def _stop_impl(self) -> None:
        """Clean shutdown of all components."""
        self._kafka_bridge.stop()
        self._kafka_bridge_thread.join()
        self._exit_stack.__exit__(None, None, None)


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
