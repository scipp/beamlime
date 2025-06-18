import logging
import signal
import sys
import threading
from contextlib import ExitStack

import holoviews as hv
import numpy as np
import panel as pn
import param

from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.config.streams import stream_kind_to_topic
from beamlime.core.message import StreamKind
from beamlime.dashboard.config_service import ConfigService
from beamlime.dashboard.kafka_bridge import KafkaBridge
from beamlime.dashboard.monitors_params import TOAEdgesParam
from beamlime.kafka import consumer as kafka_consumer
from beamlime.services.dashboard import DashboardApp as LegacyDashboard

from . import plots
from .data_forwarder import DataForwarder
from .data_service import DataService
from .data_streams import MonitorStreamManager
from .orchestrator import Orchestrator

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
        self._is_shutting_down = False
        self._shutdown_lock = threading.Lock()
        self._logger.info("DashboardApp initialized")

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

        self._kafka_bridge_thread = threading.Thread(
            target=self._kafka_bridge.start, daemon=True
        )
        self._logger.info("Config service setup complete")

    def _setup_monitor_streams(self):
        """Initialize streams for monitor data."""
        monitor_data_service = DataService()
        self._monitor_stream_manager = MonitorStreamManager(monitor_data_service)

        # Get streams for both monitors
        self._monitor1_pipe = self._monitor_stream_manager.get_stream('monitor1')
        self._monitor2_pipe = self._monitor_stream_manager.get_stream('monitor2')

        data_services = {'monitor_data': monitor_data_service}
        forwarder = DataForwarder(data_services=data_services)
        self._orchestrator = Orchestrator(legacy_app._source, forwarder)

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
            plots.monitor_total_counts_bar_chart,
            streams={'monitor1': self._monitor1_pipe, 'monitor2': self._monitor2_pipe},
        ).opts(shared_axes=False)

        return pn.pane.HoloViews(status_dmap)

    def _step(self):
        """Step function for periodic updates."""
        if self._is_shutting_down:
            return

        try:
            self._update_monitor_streams()
            self._config_service.process_incoming_messages()
        except Exception as e:
            if not self._is_shutting_down:
                self._logger.error("Error in periodic update step: %s", e)

    def start_periodic_updates(self):
        """Start periodic updates for monitor streams."""
        if self._callback is None:
            self._callback = pn.state.add_periodic_callback(self._step, period=1000)
            self._logger.info("Periodic updates started")

    def shutdown(self) -> None:
        """Shutdown the dashboard and all its components."""
        with self._shutdown_lock:
            if self._is_shutting_down:
                self._logger.info("Shutdown already in progress, skipping")
                return

            self._is_shutting_down = True
            self._logger.info("Starting DashboardApp shutdown...")

        # Stop periodic callback
        if self._callback is not None:
            self._logger.info("Stopping periodic callback...")
            try:
                pn.state.remove_periodic_callback(self._callback)
                self._callback = None
                self._logger.info("Periodic callback stopped")
            except Exception as e:
                self._logger.error("Error stopping periodic callback: %s", e)

        # Shutdown orchestrator
        try:
            self._orchestrator.shutdown()
        except Exception as e:
            self._logger.error("Error shutting down orchestrator: %s", e)

        # Stop Kafka bridge
        try:
            self._kafka_bridge.stop()
        except Exception as e:
            self._logger.error("Error stopping Kafka bridge: %s", e)

        # Wait for Kafka bridge thread to finish
        if self._kafka_bridge_thread and self._kafka_bridge_thread.is_alive():
            self._logger.info("Waiting for Kafka bridge thread to finish...")
            self._kafka_bridge_thread.join(timeout=10.0)
            if self._kafka_bridge_thread.is_alive():
                self._logger.warning(
                    "Kafka bridge thread did not finish within timeout"
                )
            else:
                self._logger.info("Kafka bridge thread finished")

        # Close exit stack (this will close all Kafka resources)
        try:
            self._logger.info("Closing exit stack...")
            self._exit_stack.__exit__(None, None, None)
            self._logger.info("Exit stack closed")
        except Exception as e:
            self._logger.error("Error closing exit stack: %s", e)

        self._logger.info("DashboardApp shutdown complete")


# Global reference to dashboard for signal handler
_dashboard_instance = None


def _signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger = logging.getLogger(__name__)
    logger.info("Received signal %s, initiating shutdown...", signum)

    if _dashboard_instance:
        _dashboard_instance.shutdown()

    logger.info("Shutdown complete, exiting...")
    sys.exit(0)


def create_dashboard():
    """Create and configure the main dashboard."""
    global _dashboard_instance

    logger = logging.getLogger(__name__)
    logger.info("Creating dashboard...")

    dashboard = DashboardApp()
    _dashboard_instance = dashboard

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    logger.info("Signal handlers registered")

    dashboard._kafka_bridge_thread.start()
    logger.info("Kafka bridge thread started")

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
    logger.info("Dashboard creation complete")

    return template


if __name__ == "__main__":
    # Configure logging for better visibility
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting dashboard application...")

    try:
        pn.serve(create_dashboard, port=5007, show=False, autoreload=True, dev=True)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        if _dashboard_instance:
            _dashboard_instance.shutdown()
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        if _dashboard_instance:
            _dashboard_instance.shutdown()
        sys.exit(1)
