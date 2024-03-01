# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    List,
    Optional,
    Protocol,
    runtime_checkable,
)

from ..logging import BeamlimeLogger
from ..logging.mixins import LogMixin


@runtime_checkable
class MessageProtocol(Protocol):
    content: Any
    sender: type
    receiver: type


class DaemonInterface(LogMixin, ABC):
    """Base class for daemons.

    Daemons have ``run`` async method that is expected to have a long life cycle.
    It may repeatedly monitor a data pipe, or listen to a message router.

    """

    logger: BeamlimeLogger

    @abstractmethod
    async def run(
        self,
    ) -> AsyncGenerator[Optional[MessageProtocol], None]:
        ...


class HandlerInterface(LogMixin, ABC):
    """Base class for message handlers.

    Message handlers hold methods or callable property that can be called
    with one argument: :class:`~BeamlimeMessage` by other daemons.

    """

    logger: BeamlimeLogger


class MessageRouter(DaemonInterface):
    """A message router that routes messages to handlers."""

    def __init__(self):
        from queue import Queue

        self.handlers: dict[type, List[Callable[[MessageProtocol], Any]]] = dict()
        self.awaitable_handlers: dict[
            type, List[Callable[[MessageProtocol], Awaitable[Any]]]
        ] = dict()
        self.message_pipe = Queue()

    @contextmanager
    def _handler_wrapper(
        self, handler: Callable[..., Any], message: MessageProtocol
    ) -> Generator[None, None, None]:
        import warnings

        try:
            self.debug(f"Routing event {type(message)} to handler {handler}...")
            yield
        except Exception as e:
            warnings.warn(f"Failed to handle event {type(message)}", stacklevel=2)
            raise e
        else:
            self.debug(f"Routing event {type(message)} to handler {handler} done.")

    def _register(
        self,
        *,
        handler_list: dict[
            type[MessageProtocol], List[Callable[[MessageProtocol], Any]]
        ],
        event_tp: type[MessageProtocol],
        handler: Callable[[MessageProtocol], Any],
    ):
        if event_tp in handler_list:
            handler_list[event_tp].append(handler)
        else:
            handler_list[event_tp] = [handler]

    def register_handler(
        self,
        event_tp,
        handler: Callable[[MessageProtocol], Any]
        | Callable[[MessageProtocol], Coroutine[Any, Any, Any]],
    ):
        if asyncio.iscoroutinefunction(handler):
            handler_list = self.awaitable_handlers
        else:
            handler_list = self.handlers

        self._register(handler_list=handler_list, event_tp=event_tp, handler=handler)

    def _collect_results(self, result: Any) -> List[MessageProtocol]:
        """Append or extend ``result`` to ``self.message_pipe``.

        It filters out non-BeamlimeMessage objects from ``result``.
        """
        if isinstance(result, MessageProtocol):
            return [result]
        elif isinstance(result, tuple):
            return list(_msg for _msg in result if isinstance(_msg, MessageProtocol))
        else:
            return []

    async def route(self, message: MessageProtocol) -> None:
        # Synchronous handlers
        results = []
        for handler in (handlers := self.handlers.get(type(message), [])):
            await asyncio.sleep(0)  # Let others use the event loop.
            with self._handler_wrapper(handler, message):
                results.extend(self._collect_results(handler(message)))

        # Asynchronous handlers
        for handler in (
            awaitable_handlers := self.awaitable_handlers.get(type(message), [])
        ):
            with self._handler_wrapper(handler, message):
                results.extend(self._collect_results(await handler(message)))

        # No handlers
        if not (handlers or awaitable_handlers):
            import warnings

            warnings.warn(
                f"No handler for event {type(message)}. Ignoring...", stacklevel=2
            )

        # Re-route the results
        for result in results:
            self.message_pipe.put(result)

    async def run(
        self,
    ) -> AsyncGenerator[Optional[MessageProtocol], None]:
        """Message router daemon."""
        while True:
            await asyncio.sleep(0)
            if self.message_pipe.empty():
                await asyncio.sleep(0.1)
            while not self.message_pipe.empty():
                await self.route(self.message_pipe.get())
            yield

    async def send_message_async(self, message: MessageProtocol) -> None:
        self.message_pipe.put(message)
        await asyncio.sleep(0)


class Application(LogMixin):
    """Application class.

    Main Responsibilities:
        - Create/retrieve event loop.
        - Register handling methods if applicable.
        - Create/collect tasks of daemons
          (via :func:`beamlime.applications.daemons.BaseDaemon.run` method).

    """

    @dataclass
    class Stop:
        """A message to break the routing loop."""

        content: Optional[Any]
        sender: type
        receiver: type

    def __init__(self, logger: BeamlimeLogger, message_router: MessageRouter) -> None:
        import asyncio

        self.loop: asyncio.AbstractEventLoop
        self.tasks: List[asyncio.Task]
        self.logger = logger
        self.message_router = message_router
        self.daemons: List[DaemonInterface] = [self.message_router]
        self.register_handling_method(self.Stop, self.stop_tasks)
        self._break = False
        super().__init__()

    def stop_tasks(self, message: Optional[MessageProtocol] = None) -> None:
        if not isinstance(message, self.Stop):
            raise TypeError(
                f"Expected message of type {self.Stop}, got {type(message)}."
            )
        self._break = True

    def register_handling_method(
        self, event_tp: type[MessageProtocol], handler: Callable[[MessageProtocol], Any]
    ) -> None:
        """Register handlers to the application message router."""
        self._message_router.register_handler(event_tp, handler)

    def register_daemon(self, daemon: DaemonInterface) -> None:
        """Register a daemon to the application.

        Registered daemons will be scheduled in the event loop
        as :func:`~Application.run` method is called.
        The future of the daemon will be collected in the ``self.tasks`` list.
        """
        self.daemons.append(daemon)

    def _create_daemon_tasks(self) -> list[Coroutine]:
        async def run_daemon(daemon: DaemonInterface):
            async for message in daemon.run():
                if message is not None:
                    await self.message_router.send_message_async(message)
                if self._break:
                    break
                await asyncio.sleep(0)

        return [run_daemon(daemon) for daemon in self.daemons]

    def run(self):
        """
        Register all handling methods and run all daemons.

        It retrieves or creates an event loop
        and schedules all coroutines(run methods) of its daemons.

        See :doc:`/developer/async_programming` for more details about
        why it handles the event loop like this.

        """
        import asyncio
        import time

        from beamlime.core.schedulers import temporary_event_loop

        self.info('Start running %s...', self.__class__.__qualname__)
        start = time.time()
        with temporary_event_loop() as loop:
            self.loop = loop
            daemon_coroutines = self._create_daemon_tasks()
            self.tasks = [loop.create_task(coro) for coro in daemon_coroutines]
            if not loop.is_running():
                loop.run_until_complete(asyncio.gather(*self.tasks))

        self.info('Finished running %s...', time.time() - start)
