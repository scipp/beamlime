# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from abc import ABC, abstractmethod, abstractproperty
from typing import Any, List

from ..constructors import ProviderGroup, SingletonProvider
from ..logging import BeamlimeLogger
from ..logging.mixins import LogMixin
from ._parameters import ChunkSize, DataFeedingSpeed
from ._random_data_providers import RandomEvents
from ._workflow import Events
from .base import BaseDaemon, MessageRouter
from .handlers import DataReductionHandler, HistogramUpdated, PlotSaver, RawDataSent


class DataStreamSimulator(BaseDaemon):
    """Data that simulates the data streaming from the random generator."""

    random_events: RandomEvents
    chunk_size: ChunkSize
    messenger: MessageRouter
    data_feeding_speed: DataFeedingSpeed

    def slice_chunk(self) -> Events:
        chunk, self.random_events = (
            Events(self.random_events[: self.chunk_size]),
            RandomEvents(self.random_events[self.chunk_size :]),
        )
        return chunk

    async def run(self) -> None:
        import asyncio

        self.info("Data streaming started...")

        num_chunks = len(self.random_events) // self.chunk_size

        for i_chunk in range(num_chunks):
            chunk = self.slice_chunk()
            self.info("Sent %s th chunk, with %s pieces.", i_chunk + 1, len(chunk))
            await self.messenger.send_message_async(
                RawDataSent(
                    sender=DataStreamSimulator,
                    receiver=Any,
                    content=chunk,
                )
            )
            await asyncio.sleep(self.data_feeding_speed)

        await self.messenger.send_message_async(
            self.messenger.StopRouting(
                content=None,
                sender=DataStreamSimulator,
                receiver=self.messenger.__class__,
            )
        )

        self.info("Data streaming finished...")


class ApplicationInterface(LogMixin, ABC):
    """Base class for applications."""

    logger: BeamlimeLogger

    def __init__(self) -> None:
        import asyncio

        self.loop: asyncio.AbstractEventLoop
        self.tasks: List[asyncio.Task]
        super().__init__()

    @abstractmethod
    def register_handling_methods(self):
        """Register handlers to the application."""
        ...

    @abstractproperty
    def daemons(self) -> tuple[BaseDaemon, ...]:
        """Collection of daemons that are running in the application."""
        ...

    def run(self):
        """
        Collect all coroutines of daemons and schedule them into the event loop.

        Notes
        -----
        **Debugging log while running async daemons under various circumstances.**

        - ``asyncio.get_event_loop`` vs ``asyncio.new_event_loop``
        1. ``asyncio.get_event_loop``
        ``get_event_loop`` will always return the current event loop.
        If there is no event loop set in the thread, it will create a new one
        and set it as a current event loop of the thread, and return the loop.
        Many of ``asyncio`` free functions internally use ``get_event_loop``,
        i.e. ``asyncio.create_task``.

        **Things to be considered while using ``asyncio.get_event_loop``.
          - ``asyncio.create_task`` does not guarantee
            whether the current loop is/will be alive until the task is done.
            You may use ``run_until_complete`` to make sure the loop is not closed
            until the task is finished.
            When you need to throw multiple async calls to the loop,
            use ``asyncio.gather`` to merge all the tasks like in this method.
          - ``close`` or ``stop`` might accidentally destroy/interrupt
            other tasks running in the same event loop.
            i.e. You can accidentally destroy the main event loop of a jupyter kernel.
          - [1]``RuntimeError`` if there has been an event loop set in the
            thread object before but it is now removed.

        2. ``asyncio.new_event_loop``
        ``asyncio.new_event_loop`` will always return the new event loop,
        but it is not set it as a current loop of the thread automatically.

        However, sometimes it is automatically handled within the thread,
        and it caused errors which was hard to be debugged under ``pytest`` session.
        For example,
        - The new event loop was not closed properly as it is destroyed.
        - The new event loop was never started until it is destroyed.
        ``Traceback`` of ``pytest`` did not show
        where exactly the error is from in those cases.
        It was resolved by using ``get_event_loop``,
        or manually closing the event loop at the end of the test.

        **When to use ``asyncio.new_event_loop``.**
          - ``asyncio.get_event_loop`` raises ``RuntimeError``[1]
          - Multi-threads

        Please note that the loop object might need to be ``close``ed manually.
        """
        import asyncio
        import time

        from beamlime.core.schedulers import temporary_event_loop

        self.register_handling_methods()
        self.info('Start running %s...', self.__class__.__qualname__)
        start = time.time()
        with temporary_event_loop() as loop:
            self.loop = loop
            daemon_coroutines = [daemon.run() for daemon in self.daemons]
            self.tasks = [loop.create_task(coro) for coro in daemon_coroutines]
            if not loop.is_running():
                loop.run_until_complete(asyncio.gather(*self.tasks))
        self.info('Finished running %s...', time.time() - start)


class DataReductionApp(ApplicationInterface):
    """Data reduction daemon that handles all part of the data reduction process."""

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
    def daemons(self) -> tuple[BaseDaemon, ...]:
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
