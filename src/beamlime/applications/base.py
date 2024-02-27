# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Coroutine, Generator, List, Optional

from beamlime.logging import BeamlimeLogger

from ..logging.mixins import LogMixin


class BaseDaemon(LogMixin, ABC):
    """Base class for daemons.

    Daemons are long-running processes that handle events/messages.
    It is expected that the ``run`` method is called in an event loop.
    Daemons handle events/messages within the ``run`` method.
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
    """A message object that can be sent through a message router."""

    content: Any
    sender: type = Any
    receiver: type = Any


class MessageRouter(BaseDaemon):
    """A message router that routes messages to handlers."""

    message_pipe: List[BeamlimeMessage]

    class StopRouting(BeamlimeMessage):
        ...

    def __init__(self):
        self.handlers: dict[type, List[Callable[[BeamlimeMessage], Any]]] = dict()
        self.awaitable_handlers: dict[
            type, List[Callable[[BeamlimeMessage], Awaitable[Any]]]
        ] = dict()
        self.message_pipe = list()
        self._break_routing_loop = False  # Break the routing loop flag

    def break_routing_loop(self, message: Optional[BeamlimeMessage] = None) -> None:
        if not isinstance(message, self.StopRouting):
            raise TypeError(
                f"Expected message of type {self.StopRouting}, got {type(message)}."
            )
        self._break_routing_loop = True

    @contextmanager
    def _handler_wrapper(
        self, handler: Callable[..., Any], message: BeamlimeMessage
    ) -> Generator[None, None, None]:
        import warnings

        try:
            self.debug(f"Routing event {type(message)} to handler {handler}...")
            yield
        except Exception as e:
            warnings.warn(f"Failed to handle event {type(message)}: {e}", stacklevel=2)
        else:
            self.debug(f"Routing event {type(message)} to handler {handler} done.")

    def _register(
        self,
        *,
        handler_list: dict[
            type[BeamlimeMessage], List[Callable[[BeamlimeMessage], Any]]
        ],
        event_tp: type[BeamlimeMessage],
        handler: Callable[[BeamlimeMessage], Any],
    ):
        if event_tp in handler_list:
            handler_list[event_tp].append(handler)
        else:
            handler_list[event_tp] = [handler]

    def register_awaitable_handler(
        self, event_tp, handler: Callable[[BeamlimeMessage], Coroutine[Any, Any, Any]]
    ):
        self._register(
            handler_list=self.awaitable_handlers, event_tp=event_tp, handler=handler
        )

    def register_handler(self, event_tp, handler: Callable[[BeamlimeMessage], Any]):
        self._register(handler_list=self.handlers, event_tp=event_tp, handler=handler)

    def _check_result(self, result: Any) -> None:
        """Append or extend ``result`` to ``self.message_pipe``.

        It filters out non-BeamlimeMessage objects from ``result``.
        """
        if isinstance(result, BeamlimeMessage):
            self.message_pipe.append(result)
        elif isinstance(result, tuple):
            msgs = (_msg for _msg in result if isinstance(_msg, BeamlimeMessage))
            self.message_pipe.extend(msgs)

    async def route(self, message: BeamlimeMessage) -> None:
        # Synchronous handlers
        if handlers := self.handlers.get(type(message), []):
            for handler in handlers:
                await asyncio.sleep(0)  # Let others use the event loop.
                with self._handler_wrapper(handler, message):
                    self._check_result(handler(message))

        # Asynchronous handlers
        if awaitable_handlers := self.awaitable_handlers.get(type(message), []):
            for handler in awaitable_handlers:
                with self._handler_wrapper(handler, message):
                    self._check_result(await handler(message))

        # No handlers
        if not (handlers or awaitable_handlers):
            import warnings

            warnings.warn(
                f"No handler for event {type(message)}. Ignoring...", stacklevel=2
            )

    async def run(self) -> None:
        """Message router daemon."""
        data_monitor = self.data_pipe_monitor(
            self.message_pipe,
            timeout=0.1,
            interval=0.1,
        )

        while not self._break_routing_loop:
            if await data_monitor():
                message = self.message_pipe.pop(0)
                await self.route(message)

        self.debug("Breaking routing loop. Routing the rest of the message...")
        while self.message_pipe:
            await self.route(self.message_pipe.pop(0))
        self.debug("Routing the rest of the message done.")

    async def send_message_async(self, message: BeamlimeMessage) -> None:
        self.message_pipe.append(message)
        await asyncio.sleep(0)


class BaseHandler(LogMixin, ABC):
    """Base class for message handlers.

    Message handlers are expected to register methods to a message router,
    so that the message router can trigger the methods
    with the certain type of message.

    Registered methods should accept a message as the only positional argument.
    """

    logger: BeamlimeLogger
