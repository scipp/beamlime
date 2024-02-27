# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio
from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass
from typing import Any, List

from ..logging import BeamlimeLogger
from ..logging.mixins import LogMixin


class DaemonInterface(LogMixin, ABC):
    """Base class for daemons.

    Daemons have ``run`` async method that is expected to have a long life cycle.
    It may repeatedly monitor a data pipe, or listen to a message router.

    """

    logger: BeamlimeLogger

    def data_pipe_monitor(
        self,
        pipe: List[Any],
        timeout: float = 5,
        interval: float = 1 / 14,
        preferred_size: int = 1,
        target_size: int = 1,
    ):
        from beamlime.core.schedulers import async_retry

        @async_retry(
            TimeoutError, max_trials=int(timeout / interval), interval=interval
        )
        async def wait_for_preferred_size() -> None:
            if len(pipe) < preferred_size:
                raise TimeoutError

        async def is_pipe_filled() -> bool:
            try:
                await wait_for_preferred_size()
            except TimeoutError:
                await asyncio.sleep(0)  # Let other handlers use the event loop.
            return len(pipe) >= target_size

        return is_pipe_filled

    @abstractmethod
    async def run(self):
        ...


@dataclass
class BeamlimeMessage:
    """A message object that can be exchanged between daemons or handlers."""

    content: Any
    sender: type = Any
    receiver: type = Any


class HandlerInterface(LogMixin, ABC):
    """Base class for message handlers.

    Message handlers hold methods or callable property that can be called
    with one argument: :class:`~BeamlimeMessage` by other daemons.

    """

    logger: BeamlimeLogger


class ApplicationInterface(LogMixin, ABC):
    """Base class for applications.

    Main Responsibilities:
        - Create/retrieve event loop.
        - Register handling methods if applicable.
        - Create/collect tasks of daemons
          (via :func:`beamlime.applications.daemons.BaseDaemon.run` method).

    """

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
    def daemons(self) -> tuple[DaemonInterface, ...]:
        """Collection of daemons that are running in the application."""
        ...

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
