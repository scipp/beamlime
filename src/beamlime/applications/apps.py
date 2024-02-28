# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio
from typing import Any, Callable, List

from ..constructors import ProviderGroup, SingletonProvider
from ..logging import BeamlimeLogger
from ..logging.mixins import LogMixin
from .base import DaemonInterface, MessageProtocol
from .daemons import DataStreamSimulator, MessageRouter
from .handlers import DataReductionHandler, HistogramUpdated, PlotSaver, RawDataSent


class Application(LogMixin):
    """Base class for applications.

    Main Responsibilities:
        - Create/retrieve event loop.
        - Register handling methods if applicable.
        - Create/collect tasks of daemons
          (via :func:`beamlime.applications.daemons.BaseDaemon.run` method).

    """

    def __init__(self, logger: BeamlimeLogger, message_router: MessageRouter) -> None:
        import asyncio

        self.loop: asyncio.AbstractEventLoop
        self.tasks: List[asyncio.Task]
        self.logger = logger
        self.message_router = message_router
        self.daemons: List[DaemonInterface] = []
        super().__init__()

    def register_handling_method(
        self, event_tp: type[MessageProtocol], handler: Callable[[MessageProtocol], Any]
    ) -> None:
        """Register handlers to the application."""
        if asyncio.iscoroutinefunction(handler):
            self.message_router.register_awaitable_handler(event_tp, handler)

        self.message_router.register_handler(event_tp, handler)

    def register_daemon(self, daemon: DaemonInterface) -> None:
        """Register a daemon to the application."""
        self.daemons.append(daemon)

    def run(self):
        """
        Register all handling methods and run all daemons.

        It retrieves or creates an event loop
        and schedules all coroutines(run methods) of its daemons.

        Notes
        -----
        **Debugging log while running async daemons under various circumstances.**

        :func:`asyncio.get_event_loop` vs :func:`asyncio.new_event_loop`

        1. :func:`asyncio.get_event_loop`
            ``get_event_loop`` will always return the current event loop.
            If there is no event loop set in the thread, it will create a new one
            and set it as a current event loop of the thread, and return the loop.
            Many of ``asyncio`` free functions internally use ``get_event_loop``,
            i.e. :func:`asyncio.create_task`.

            :strong:`Things to consider while using` :func:`asyncio.get_event_loop`.

            * ``asyncio.create_task`` does not guarantee
                whether the current loop is/will be alive until the task is done.
                You may use ``run_until_complete`` to make sure the loop is not closed
                until the task is finished.
                When you need to throw multiple async calls to the loop,
                use ``asyncio.gather`` to merge all the tasks like in this method.
            * ``close`` or ``stop`` might accidentally destroy/interrupt
                other tasks running in the same event loop.
                i.e. You can accidentally destroy the event loop of a jupyter kernel.
            * *1* :class:`RuntimeError` if there has been an event loop set in the
                thread object before but it is now removed.

        2. :func:`asyncio.new_event_loop`
            ``asyncio.new_event_loop`` will always return the new event loop,
            but it is not set it as a current loop of the thread automatically.

            However, sometimes it is automatically handled within the thread,
            and it caused errors which was hard to debug under ``pytest`` session.
            For example,

            * The new event loop was not closed properly as it is destroyed.
            * The new event loop was never started until it is destroyed.


            ``Traceback`` of ``pytest`` did not show
            where exactly the error is from in those cases.
            It was resolved by using :func:`asyncio.get_event_loop`,
            or manually closing the event loop at the end of the test.

            :strong:`When to use` :func:`asyncio.new_event_loop`.

            * :func:`asyncio.get_event_loop` raises :class:`RuntimeError` *1*.
            * Multi-threads.

        Please note that the loop object might need to be **closed** manually.

        """
        import asyncio
        import time

        from beamlime.core.schedulers import temporary_event_loop

        self.info('Start running %s...', self.__class__.__qualname__)
        start = time.time()
        with temporary_event_loop() as loop:
            self.loop = loop
            daemon_coroutines = [daemon.run() for daemon in self.daemons]
            self.tasks = [loop.create_task(coro) for coro in daemon_coroutines]
            if not loop.is_running():
                loop.run_until_complete(asyncio.gather(*self.tasks))

        self.info('Finished running %s...', time.time() - start)


class DataReductionApp(Application):
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
