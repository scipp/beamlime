# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Common functionality for implementing dashboards."""

import logging
import threading
from abc import ABC, abstractmethod
from contextlib import ExitStack

import panel as pn

from beamlime import ServiceBase
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.config.streams import stream_kind_to_topic
from beamlime.core.message import StreamKind
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    KafkaToDa00Adapter,
)
from beamlime.kafka.source import KafkaMessageSource

from .background_message_bridge import BackgroundMessageBridge
from .config_service import ConfigSchemaManager, ConfigService
from .data_forwarder import DataForwarder
from .data_service import DataService
from .data_streams import MonitorStreamManager, ReductionStreamManager
from .kafka_transport import KafkaTransport
from .orchestrator import Orchestrator

# Global throttling for sliders, etc.
pn.config.throttled = True


class DashboardBase(ServiceBase, ABC):
    """Base class for dashboard applications providing common functionality."""

    def __init__(
        self,
        *,
        instrument: str = 'dummy',
        dev: bool = False,
        log_level: int = logging.INFO,
        dashboard_name: str,
        port: int = 5007,
    ):
        name = f'{instrument}_{dashboard_name}'
        super().__init__(name=name, log_level=log_level)
        self._instrument = instrument
        self._port = port
        self._dev = dev

        self._exit_stack = ExitStack()
        self._exit_stack.__enter__()

        self._callback = None
        self._setup_config_service()
        self._setup_data_infrastructure()
        self._logger.info("%s initialized", self.__class__.__name__)

    @abstractmethod
    def create_sidebar_content(self) -> pn.viewable.Viewable:
        """Override this method to create the sidebar content."""
        pass

    @abstractmethod
    def create_main_content(self) -> pn.viewable.Viewable:
        """Override this method to create the main dashboard content."""
        pass

    def _setup_config_service(self) -> None:
        """Set up configuration service with Kafka bridge."""
        kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
        _, consumer = self._exit_stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=self._instrument)
        )
        kafka_transport = KafkaTransport(
            kafka_config=kafka_downstream_config, consumer=consumer, logger=self._logger
        )
        self._kafka_bridge = BackgroundMessageBridge(
            transport=kafka_transport, logger=self._logger
        )
        self._config_service = ConfigService(
            message_bridge=self._kafka_bridge, schema_validator=ConfigSchemaManager()
        )

        self._kafka_bridge_thread = threading.Thread(
            target=self._kafka_bridge.start, daemon=True
        )
        self._logger.info("Config service setup complete")

    def _setup_data_infrastructure(self) -> None:
        """Set up data services, forwarder, and orchestrator."""
        data_services = {
            'monitor_data': DataService(),
            'detector_data': DataService(),
            'data_reduction': DataService(),
        }
        self._monitor_stream_manager = MonitorStreamManager(
            data_services['monitor_data']
        )
        self._reduction_stream_manager = ReductionStreamManager(
            data_services['data_reduction']
        )

        self._orchestrator = Orchestrator(
            self._setup_kafka_consumer(), DataForwarder(data_services=data_services)
        )
        self._logger.info("Data infrastructure setup complete")

    def _setup_kafka_consumer(self) -> AdaptingMessageSource:
        """Set up Kafka consumer for data streams."""
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

    def _step(self):
        """Step function for periodic updates."""
        # We use hold() to ensure that the UI does not update repeatedly when multiple
        # messages are processed in a single step. This is important to avoid, e.g.,
        # multiple lines in the same plot, or different plots updating in short
        # succession, which is visually distracting.
        # Furthermore, this improves performance by reducing the number of re-renders.
        with pn.io.hold():
            self._orchestrator.update()

    def get_dashboard_title(self) -> str:
        """Get the dashboard title. Override for custom titles."""
        return f"{self._instrument.upper()} — Live Data"

    def get_header_background(self) -> str:
        """Get the header background color. Override for custom colors."""
        return '#2596be'

    def start_periodic_updates(self, period: int = 1000) -> None:
        """Start periodic updates for the dashboard."""
        if self._callback is None:

            def _safe_step():
                try:
                    self._step()
                    self._config_service.process_incoming_messages()
                except Exception:
                    self._logger.exception("Error in periodic update step: %s")

            self._callback = pn.state.add_periodic_callback(_safe_step, period=period)
            self._logger.info("Periodic updates started")

    def create_layout(self) -> pn.template.MaterialTemplate:
        """Create the basic dashboard layout."""
        main_content = self.create_main_content()
        sidebar_content = self.create_sidebar_content()

        template = pn.template.MaterialTemplate(
            title=self.get_dashboard_title(),
            sidebar=sidebar_content,
            main=main_content,
            header_background=self.get_header_background(),
        )
        self.start_periodic_updates()
        return template

    def _start_impl(self) -> None:
        """Start the dashboard service."""
        self._kafka_bridge_thread.start()

    def run_forever(self) -> None:
        """Run the dashboard server."""
        import atexit

        atexit.register(self.stop)
        try:
            pn.serve(
                self.create_layout,
                port=self._port,
                show=False,
                autoreload=True,
                dev=self._dev,
            )
        except KeyboardInterrupt:
            self._logger.info("Keyboard interrupt received, shutting down...")
            self.stop()

    def _stop_impl(self) -> None:
        """Clean shutdown of all components."""
        self._kafka_bridge.stop()
        self._kafka_bridge_thread.join()
        self._exit_stack.__exit__(None, None, None)
