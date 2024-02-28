# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Coroutine, Generator, List, Optional

from ._parameters import ChunkSize, DataFeedingSpeed
from ._random_data_providers import RandomEvents
from ._workflow import Events
from .base import DaemonInterface, MessageProtocol
from .handlers import RawDataSent


@dataclass
class BeamlimeMessage:
    """A message object that can be exchanged between daemons or handlers."""

    content: Any
    sender: type = Any
    receiver: type = Any


class MessageRouter(DaemonInterface):
    """A message router that routes messages to handlers."""

    class StopRouting(BeamlimeMessage):
        ...

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


class DataStreamSimulator(DaemonInterface):
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
