# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, Generic, Protocol, TypeVar

import scipp as sc

from .message import Message, MessageKey, Tin, Tout


class Config(Protocol):
    def get(self, key: str, default: Any | None = None) -> Any:
        pass


class ConfigProxy:
    """Proxy for accessing configuration, prefixed with a namespace."""

    def __init__(self, config: Config, *, namespace: str):
        self._config = config
        self._namespace = namespace

    def get(self, key: str, default: Any | None = None) -> Any:
        """
        Look for the key with the namespace prefix, and fall back to the key without
        the prefix if not found. If neither is found, return the default.
        """
        return self._config.get(
            f'{self._namespace}.{key}', self._config.get(key, default)
        )


class Handler(Generic[Tin, Tout]):
    """
    Base class for message handlers.

    Handlers are used by :py:class:`StreamProcessor` to process messages. Since each
    stream of messages will typically need multiple handlers (one per topic and message
    source), handlers are typically created by a :py:class:`HandlerRegistry` and not
    directly.
    """

    def __init__(self, *, logger: logging.Logger | None, config: Config):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config

    # TODO It is not clear how to handle output topic naming. Should the handler
    # take care of this explicitly? Can there be automatic prefixing as with
    # the config?
    def handle(self, message: Message[Tin]) -> list[Message[Tout]]:
        raise NotImplementedError


class HandlerRegistry(Generic[Tin, Tout]):
    """
    Registry for handlers.

    Handlers are created on demand and cached based on the message key.
    """

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        config: Config,
        handler_cls: type[Handler[Tin, Tout]],
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._handler_cls = handler_cls
        self._handlers: dict[MessageKey, Handler] = {}

    def get(self, key: str) -> Handler:
        if key not in self._handlers:
            self._handlers[key] = self._handler_cls(
                logger=self._logger, config=ConfigProxy(self._config, namespace=key)
            )
        return self._handlers[key]


T = TypeVar('T')
U = TypeVar('U')


class Accumulator(Protocol, Generic[T, U]):
    """
    Protocol for an accumulator that accumulates data over time.

    Accumulators are used by handlers such as :py:class:`PeriodicAccumulatingHandler` as
    (1) preprocessors and (2) accumulators for the final data.
    """

    def add(self, timestamp: int, data: T) -> None:
        pass

    def get(self) -> U:
        pass

    def clear(self) -> None:
        pass


class PeriodicAccumulatingHandler(Handler[T, U]):
    """
    Handler that accumulates data over time and emits the accumulated data periodically.
    """

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        config: Config,
        preprocessor: Accumulator[T, U],
        accumulators: dict[str, Accumulator[U, U]],
    ):
        super().__init__(logger=logger, config=config)
        self._preprocessor = preprocessor
        self._accumulators = accumulators
        self._next_update = 0
        self._last_clear = 0

    @property
    def update_every(self) -> int:
        """Update interval in nanoseconds, based on dynamic config value."""
        raw = self._config.get('update_every', {'value': 1.0, 'unit': 's'})
        return int(sc.scalar(**raw).to(unit='ns', copy=False).value)

    @property
    def start_time(self) -> int:
        """Start time in nanoseconds, based on dynamic config value."""
        raw = self._config.get('start_time', {'value': 0, 'unit': 'ns'})
        return int(sc.scalar(**raw).to(unit='ns', copy=False).value)

    def handle(self, message: Message[T]) -> list[Message[U]]:
        if self.start_time > self._last_clear:
            self._preprocessor.clear()
            for accumulator in self._accumulators.values():
                accumulator.clear()
            self._last_clear = self.start_time
            # Set next update to current message to avoid lag in user experience.
            self._next_update = message.timestamp
        self._preprocessor.add(message.timestamp, message.value)
        if message.timestamp < self._next_update:
            return []
        data = self._preprocessor.get()
        for accumulator in self._accumulators.values():
            accumulator.add(timestamp=message.timestamp, data=data)
        # If there were no pulses for a while we need to skip several updates.
        # Note that we do not simply set _next_update based on reference_time
        # to avoid drifts.
        self._next_update += (
            (message.timestamp - self._next_update) // self.update_every + 1
        ) * self.update_every
        key = message.key
        return [
            Message(
                timestamp=message.timestamp,
                key=replace(key, topic=f'{key.topic}_{name}'),
                value=accumulator.get(),
            )
            for name, accumulator in self._accumulators.items()
        ]
