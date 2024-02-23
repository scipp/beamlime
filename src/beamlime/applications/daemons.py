# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from abc import ABC
from typing import Any, List, NewType

from ._parameters import ChunkSize, DataFeedingSpeed
from ._random_data_providers import RandomEvents
from ._workflow import Events
from .base import BaseDaemon, MessageRouter
from .handlers import DataReductionHandler, PlotHandler, RawDataSent, StopWatch

DataStreamListener = NewType("DataStreamListener", BaseDaemon)


class DataReductionMessageRouter(MessageRouter):
    async def run(self) -> None:
        await super().run()
        await self.route(
            StopWatch.Stop(
                sender=DataReductionMessageRouter,
                receiver=StopWatch,
                content=None,
            )
        )


class DataStreamSimulator(BaseDaemon):
    raw_data_pipe: List[Events]
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
            self.raw_data_pipe.append(chunk)
            self.info("Sent %s th chunk, with %s pieces.", i_chunk + 1, len(chunk))
            await self.messenger.send_message_async(
                RawDataSent(
                    sender=DataStreamSimulator,
                    receiver=Any,
                    content=self.raw_data_pipe,
                )
            )
            await asyncio.sleep(self.data_feeding_speed)
            if i_chunk == 0:
                await self.messenger.send_message_async(
                    StopWatch.Start(
                        sender=DataStreamSimulator,
                        receiver=StopWatch,
                        content=None,
                    )
                )

        await self.messenger.send_message_async(
            self.messenger.StopRouting(
                sender=DataStreamSimulator, receiver=self.messenger, content=None
            )
        )

        self.info("Data streaming finished...")


class DataReductionDaemon(BaseDaemon, ABC):
    data_stream_listener: DataStreamListener
    message_router: MessageRouter
    data_reduction: DataReductionHandler
    plot_saver: PlotHandler
    stop_watch: StopWatch

    def collect_sub_daemons(self) -> list[BaseDaemon]:
        return [
            self.data_stream_listener,
            self.message_router,
        ]

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

        from beamlime.core.schedulers import temporary_event_loop

        self.info('Start running %s...', DataReductionDaemon.__qualname__)
        with temporary_event_loop() as loop:
            self.loop = loop
            daemon_coroutines = [daemon.run() for daemon in self.collect_sub_daemons()]
            self.tasks = [loop.create_task(coro) for coro in daemon_coroutines]
            if not loop.is_running():
                loop.run_until_complete(asyncio.gather(*self.tasks))
