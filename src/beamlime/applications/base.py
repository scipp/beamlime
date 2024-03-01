# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
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


def _get_size_monitor_function(
    pipe: Any, size_monitor_func: Optional[Callable[..., int]] = None
) -> Callable[..., int]:
    if size_monitor_func is None and (isinstance(pipe, (list, tuple, set, dict))):
        from functools import partial

        return partial(len, pipe)
    elif size_monitor_func is not None:
        return size_monitor_func
    else:
        raise ValueError(
            "``size_monitor_func`` must be provided for non-iterable data pipes."
        )


class DaemonInterface(LogMixin, ABC):
    """Base class for daemons.

    Daemons have ``run`` async method that is expected to have a long life cycle.
    It may repeatedly monitor a data pipe, or listen to a message router.

    """

    logger: BeamlimeLogger

    def data_pipe_monitor(
        self,
        pipe: Any,
        timeout: float = 5,
        interval: float = 1 / 14,
        preferred_size: int = 1,
        target_size: int = 1,
        size_monitor_func: Optional[Callable[..., int]] = None,
    ):
        from beamlime.core.schedulers import async_retry

        size_monitor = _get_size_monitor_function(pipe, size_monitor_func)

        @async_retry(
            TimeoutError, max_trials=int(timeout / interval), interval=interval
        )
        async def wait_for_preferred_size() -> None:
            if size_monitor() < preferred_size:
                raise TimeoutError

        async def is_pipe_filled() -> bool:
            try:
                await wait_for_preferred_size()
            except TimeoutError:
                await asyncio.sleep(0)  # Let other handlers use the event loop.
            return size_monitor() >= target_size

        return is_pipe_filled

    @abstractmethod
    async def run(self):
        ...


@runtime_checkable
class MessageProtocol(Protocol):
    content: Any
    sender: type
    receiver: type


class HandlerInterface(LogMixin, ABC):
    """Base class for message handlers.

    Message handlers hold methods or callable property that can be called
    with one argument: :class:`~BeamlimeMessage` by other daemons.

    """

    logger: BeamlimeLogger


class MessageRouter(DaemonInterface):
    """A message router that routes messages to handlers."""

    @dataclass
    class StopRouting:
        """A message to break the routing loop."""

        content: Optional[Any]
        sender: type
        receiver: type

    def __init__(self):
        from queue import Queue

        self.handlers: dict[type, List[Callable[[MessageProtocol], Any]]] = dict()
        self.awaitable_handlers: dict[
            type, List[Callable[[MessageProtocol], Awaitable[Any]]]
        ] = dict()
        self.message_pipe = Queue()
        self._break_routing_loop = False  # Break the routing loop flag

    def break_routing_loop(self, message: Optional[MessageProtocol] = None) -> None:
        if not isinstance(message, self.StopRouting):
            raise TypeError(
                f"Expected message of type {self.StopRouting}, got {type(message)}."
            )
        self._break_routing_loop = True

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

    def register_awaitable_handler(
        self, event_tp, handler: Callable[[MessageProtocol], Coroutine[Any, Any, Any]]
    ):
        self._register(
            handler_list=self.awaitable_handlers, event_tp=event_tp, handler=handler
        )

    def register_handler(self, event_tp, handler: Callable[[MessageProtocol], Any]):
        self._register(handler_list=self.handlers, event_tp=event_tp, handler=handler)

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

    async def run(self) -> None:
        """Message router daemon."""
        data_monitor = self.data_pipe_monitor(
            self.message_pipe,
            timeout=0.1,
            interval=0.1,
            size_monitor_func=self.message_pipe.qsize,
        )

        while not self._break_routing_loop:
            if await data_monitor():
                message = self.message_pipe.get()
                await self.route(message)
                await asyncio.sleep(0)

        self.debug("Breaking routing loop. Routing the rest of the message...")
        while not self.message_pipe.empty():
            await self.route(self.message_pipe.get())
        self.debug("Routing the rest of the message done.")

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

    def __init__(self, logger: BeamlimeLogger, message_router: MessageRouter) -> None:
        import asyncio

        self.loop: asyncio.AbstractEventLoop
        self.tasks: List[asyncio.Task]
        self.logger = logger
        self.message_router = message_router
        self.daemons: List[DaemonInterface] = [self.message_router]
        self.register_handling_method(
            self.message_router.StopRouting, self.message_router.break_routing_loop
        )
        super().__init__()

    def register_handling_method(
        self, event_tp: type[MessageProtocol], handler: Callable[[MessageProtocol], Any]
    ) -> None:
        """Register handlers to the application message router."""
        if asyncio.iscoroutinefunction(handler):
            self.message_router.register_awaitable_handler(event_tp, handler)
        else:
            self.message_router.register_handler(event_tp, handler)

    def register_daemon(self, daemon: DaemonInterface) -> None:
        """Register a daemon to the application.

        Registered daemons will be scheduled in the event loop
        as :func:`~Application.run` method is called.
        The future of the daemon will be collected in the ``self.tasks`` list.
        """
        self.daemons.append(daemon)

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
            daemon_coroutines = [daemon.run() for daemon in self.daemons]
            self.tasks = [loop.create_task(coro) for coro in daemon_coroutines]
            if not loop.is_running():
                loop.run_until_complete(asyncio.gather(*self.tasks))

        self.info('Finished running %s...', time.time() - start)
