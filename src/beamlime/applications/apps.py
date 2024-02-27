# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from ..constructors import ProviderGroup, SingletonProvider
from .base import ApplicationInterface, DaemonInterface
from .daemons import DataStreamSimulator, MessageRouter
from .handlers import DataReductionHandler, HistogramUpdated, PlotSaver, RawDataSent


class DataReductionApp(ApplicationInterface):
    """Minimum data reduction application."""

    # Daemons
    data_stream_listener: DataStreamSimulator
    message_router: MessageRouter
    # Handlers
    data_reduction_handler: DataReductionHandler
    plot_handler: PlotSaver

    def register_handling_methods(self) -> None:
        # Message Router
        self.message_router.register_handler(
            self.message_router.StopRouting, self.message_router.break_routing_loop
        )
        # Data Reduction Handler
        self.message_router.register_awaitable_handler(
            RawDataSent, self.data_reduction_handler.process_message
        )
        # Plot Handler
        self.message_router.register_awaitable_handler(
            HistogramUpdated, self.plot_handler.save_histogram
        )

    @property
    def daemons(self) -> tuple[DaemonInterface, ...]:
        return (
            self.data_stream_listener,
            self.message_router,
        )

    @staticmethod
    def collect_default_providers() -> ProviderGroup:
        """Helper method to collect all default providers for this prototype."""
        from beamlime.applications._parameters import collect_default_param_providers
        from beamlime.applications._random_data_providers import random_data_providers
        from beamlime.applications._workflow import provide_pipeline
        from beamlime.applications.handlers import random_image_path
        from beamlime.constructors.providers import merge as merge_providers
        from beamlime.logging.providers import log_providers

        app_providers = ProviderGroup(
            SingletonProvider(DataReductionApp),
            DataStreamSimulator,
            DataReductionHandler,
            PlotSaver,
            provide_pipeline,
            random_image_path,
        )
        app_providers[MessageRouter] = SingletonProvider(MessageRouter)

        return merge_providers(
            collect_default_param_providers(),
            random_data_providers,
            app_providers,
            log_providers,
        )
