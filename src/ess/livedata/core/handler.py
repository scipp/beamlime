# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Any, Generic, Protocol, TypeVar

from ..config.instrument import Instrument
from .message import Message, StreamId, Tin, Tout


class Config(Protocol):
    def get(self, key: str, default: Any | None = None) -> Any: ...


class Handler(Generic[Tin, Tout]):
    """
    Base class for message handlers.

    Handlers are used by :py:class:`StreamProcessor` to process messages. Since each
    stream of messages will typically need multiple handlers (one per topic and message
    source), handlers are typically created by a :py:class:`HandlerRegistry` and not
    directly.
    """

    def __init__(self, *, logger: logging.Logger | None = None, config: Config):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config

    def handle(self, messages: list[Message[Tin]]) -> list[Message[Tout]]:
        """Handle a list of messages. There is no 1:1 mapping to the output list."""
        raise NotImplementedError


class HandlerFactory(Protocol, Generic[Tin, Tout]):
    def make_handler(self, key: StreamId) -> Handler[Tin, Tout] | None:
        pass

    def make_preprocessor(self, key: StreamId) -> Accumulator | None:
        return None


class JobBasedHandlerFactoryBase(HandlerFactory[Tin, Tout]):
    """Factory base used by job-based backend services."""

    def __init__(
        self, *, instrument: Instrument, logger: logging.Logger | None = None
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._instrument = instrument

    @property
    def instrument(self) -> Instrument:
        return self._instrument


class CommonHandlerFactory(HandlerFactory[Tin, Tout]):
    """
    Factory for using (multiple instances of) a common handler class.
    """

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        handler_cls: type[Handler[Tin, Tout]],
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._handler_cls = handler_cls

    def make_handler(self, key: StreamId) -> Handler[Tin, Tout]:
        return self._handler_cls(logger=self._logger, config={})


class HandlerRegistry(Generic[Tin, Tout]):
    """
    Registry for handlers.

    Handlers are created on demand from a factory and cached based on the message key.
    If the factory returns None for a key, messages with that key will be skipped.
    """

    def __init__(self, *, factory: HandlerFactory[Tin, Tout]):
        self._factory = factory
        self._handlers: dict[StreamId, Handler[Tin, Tout] | None] = {}

    def __len__(self) -> int:
        return sum(1 for handler in self._handlers.values() if handler is not None)

    def register_handler(self, key: StreamId, handler: Handler[Tin, Tout]) -> None:
        self._handlers[key] = handler

    def get(self, key: StreamId) -> Handler[Tin, Tout] | None:
        if key not in self._handlers:
            self._handlers[key] = self._factory.make_handler(key)
        return self._handlers[key]


T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class Accumulator(Protocol, Generic[T, U]):
    """
    Protocol for an accumulator that accumulates data over time.

    Accumulators are used by handlers such as :py:class:`PeriodicAccumulatingHandler` as
    (1) preprocessors and (2) accumulators for the final data.
    """

    def add(self, timestamp: int, data: T) -> None: ...

    def get(self) -> U: ...

    def clear(self) -> None: ...
