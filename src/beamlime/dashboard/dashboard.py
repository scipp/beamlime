# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""Common functionality for implementing dashboards."""

import logging
import threading
from abc import ABC, abstractmethod
from contextlib import ExitStack

import panel as pn
import scipp as sc
from holoviews import Dimension, streams

from beamlime import ServiceBase
from beamlime.config import config_names, instrument_registry
from beamlime.config.config_loader import load_config
from beamlime.config.instruments import get_config
from beamlime.config.schema_registry import get_schema_registry
from beamlime.config.streams import stream_kind_to_topic
from beamlime.core.message import StreamKind
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    KafkaToDa00Adapter,
)
from beamlime.kafka.source import BackgroundMessageSource

from .config_service import ConfigService
from .controller_factory import ControllerFactory
from .data_forwarder import DataForwarder
from .data_key import DataKey
from .data_service import DataService
from .kafka_transport import KafkaTransport
from .message_bridge import BackgroundMessageBridge
from .orchestrator import Orchestrator
from .schema_validator import PydanticSchemaValidator
from .stream_manager import (
    DetectorStreamManager,
    MonitorStreamManager,
    ReductionStreamManager,
)
from .widgets.reduction_widget import ReductionWidget
from .workflow_controller import WorkflowController

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

        # Global unit format.
        Dimension.unit_format = ' [{unit}]'

        # Load the module to register the instrument's workflows.
        self._instrument_module = get_config(instrument)
        self._processor_factory = instrument_registry[
            self._instrument
        ].processor_factory

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
            message_bridge=self._kafka_bridge,
            schema_validator=PydanticSchemaValidator(
                schema_registry=get_schema_registry()
            ),
        )
        self._controller_factory = ControllerFactory(
            config_service=self._config_service,
            schema_registry=get_schema_registry(),
        )

        self._kafka_bridge_thread = threading.Thread(
            target=self._kafka_bridge.start, daemon=True
        )
        self._logger.info("Config service setup complete")

    def _setup_data_infrastructure(self) -> None:
        """Set up data services, forwarder, and orchestrator."""
        # da00 of backend services converted to scipp.DataArray
        ScippDataService = DataService[DataKey, sc.DataArray]
        self._data_services = {
            'monitor_data': ScippDataService(),
            'detector_data': ScippDataService(),
            'data_reduction': ScippDataService(),
        }
        self._monitor_stream_manager = MonitorStreamManager(
            data_service=self._data_services['monitor_data'], pipe_factory=streams.Pipe
        )
        self._detector_stream_manager = DetectorStreamManager(
            data_service=self._data_services['detector_data'], pipe_factory=streams.Pipe
        )
        self._reduction_stream_manager = ReductionStreamManager(
            data_service=self._data_services['data_reduction'],
            pipe_factory=streams.Pipe,
        )

        self._orchestrator = Orchestrator(
            self._setup_kafka_consumer(),
            DataForwarder(data_services=self._data_services),
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
        source = self._exit_stack.enter_context(
            BackgroundMessageSource(consumer=consumer)
        )
        return AdaptingMessageSource(
            source=source,
            adapter=ChainedAdapter(
                first=KafkaToDa00Adapter(stream_kind=StreamKind.BEAMLIME_DATA),
                second=Da00ToScippAdapter(),
            ),
        )

    def _setup_workflow_management(self, namespace: str) -> None:
        """Initialize workflow controller and reduction widget."""
        workflow_registry = {
            key: workflow
            for key, workflow in self._processor_factory.items()
            if workflow.namespace == namespace
        }
        self._workflow_controller = WorkflowController.from_config_service(
            config_service=self._config_service,
            source_names=sorted(self._processor_factory.source_names),
            workflow_registry=workflow_registry,
            data_service=self._data_services[namespace],
        )
        self._reduction_widget = ReductionWidget(controller=self._workflow_controller)

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

    def start_periodic_updates(self, period: int = 500) -> None:
        """
        Start periodic updates for the dashboard.

        Parameters
        ----------
        period:
            The period in milliseconds for the periodic update step.
            Default is 500 ms. Even if the backend produces updates, e.g., once per
            second, this default should reduce UI lag somewhat. If there are no new
            messages, the step function should not do anything.
        """
        if self._callback is not None:
            # Callback from previous session, e.g., before reloading the page. As far as
            # I can tell the garbage collector does clean this up eventually, but
            # let's be explicit.
            self._callback.stop()

        def _safe_step():
            try:
                self._step()
                self._config_service.process_incoming_messages()
            except Exception:
                self._logger.exception("Error in periodic update step.")

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

    @property
    def server(self):
        """Get the Panel server for WSGI deployment."""
        return pn.serve(
            self.create_layout,
            port=self._port,
            show=False,
            autoreload=False,
            dev=self._dev,
        )

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
