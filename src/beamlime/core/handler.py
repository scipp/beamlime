# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any, Generic, Protocol, TypeVar

from ..config import models
from ..kafka.helpers import source_name
from .message import Message, MessageKey, Tin, Tout


class Config(Protocol):
    def get(self, key: str, default: Any | None = None) -> Any:
        pass


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
    def make_handler(self, key: MessageKey) -> Handler[Tin, Tout]:
        pass


class CommonHandlerFactory(HandlerFactory[Tin, Tout]):
    """
    Factory for using (multiple instances of) a common handler class.
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

    def make_handler(self, key: MessageKey) -> Handler[Tin, Tout]:
        return self._handler_cls(logger=self._logger, config=self._config)

    @staticmethod
    def from_handler(
        handler_cls: type[Handler[Tin, Tout]],
    ) -> Callable[[logging.Logger | None, Config], CommonHandlerFactory[Tin, Tout]]:
        def make(
            *, logger: logging.Logger | None = None, config: Config
        ) -> CommonHandlerFactory[Tin, Tout]:
            return CommonHandlerFactory(
                logger=logger, config=config, handler_cls=handler_cls
            )

        return make


class HandlerRegistry(Generic[Tin, Tout]):
    """
    Registry for handlers.

    Handlers are created on demand from a factory and cached based on the message key.
    """

    def __init__(self, *, factory: HandlerFactory[Tin, Tout]):
        self._factory = factory
        self._handlers: dict[MessageKey, Handler] = {}

    def __len__(self) -> int:
        return len(self._handlers)

    def get(self, key: MessageKey) -> Handler:
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

    def add(self, timestamp: int, data: T) -> None:
        pass

    def get(self) -> U:
        pass

    def clear(self) -> None:
        pass


class ConfigModelAccessor(Generic[T, U]):
    """
    Access a dynamic configuration value and convert it on demand.

    Avoids unnecessary conversions if the value has not changed.

    Parameters
    ----------
    config:
        Configuration object.
    key:
        Key in the configuration.
    model:
        Pydantic model to validate the raw value. Also defines the default value.
    convert:
        Optional function to convert the raw value to the desired type or perform other
        actions on config value updates.
    """

    def __init__(
        self,
        config: Config,
        key: str,
        model: type[T],
        convert: Callable[[T], U] = lambda x: x,
    ) -> None:
        self._config = config
        self._key = key
        self._model_cls = model
        self._convert = convert
        self._raw_value: dict[str, Any] | None = None
        self._value: U | None = None
        self._default = self._model_cls().model_dump()

    def __call__(self) -> U:
        raw_value = self._config.get(self._key, self._default)
        if raw_value != self._raw_value:
            self._raw_value = raw_value
            self._value = self._convert(self._model_cls.model_validate(raw_value))
        return self._value


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
        accumulators: Mapping[str, Accumulator[U, V]],
    ):
        super().__init__(logger=logger, config=config)
        self._preprocessor = preprocessor
        self._accumulators = accumulators
        self._next_update = 0
        self._last_clear = 0
        self.start_time = ConfigModelAccessor(
            config=config, key='start_time', model=models.StartTime
        )
        self.update_every = ConfigModelAccessor(
            config=config, key='update_every', model=models.UpdateEvery
        )
        self._logger.info('Setup handler with %s accumulators', len(accumulators))

    def handle(self, messages: list[Message[T]]) -> list[Message[V]]:
        # Note an issue here: We preprocess all messages, with a range of timestamps.
        # If one of the accumulators is a sliding-window accumulator, the cutoff time
        # will not be precise. I do not expect this to be a problem as long as the
        # processing time is low since then few messages at a time will be processed.
        # As event rates go up, however, we will be processing more and more messages
        # at a time, leading to a larger discrepancy. At this point we may experience
        # some time-dependent fluctuations in sliding-window results. The mechanism may
        # need to be revisited if this becomes a problem.
        for message in messages:
            self._preprocess(message)
        # Note that preprocess.get or accumulator.add may be expensive. We may thus ask
        # whether this should only be done when _produce_update is called. This would
        # however lead to extra latency and likely even a waste of time in waiting idly
        # for new messages. Instead, by processing more eagerly, the overall mechanism
        # is more responsive and will "converge" to a stable state of messages per call.
        # That is, if processing is too expensive for a small number of messages, this
        # delay will lead to more messages to be consumed in the next iteration, driving
        # down the number of calls to this method until equilibrium is reached. This can
        # be demonstrated using a simple model process with an accumulate call that has
        # a constant + linear-per-message cost.
        data = self._preprocessor.get()
        for accumulator in self._accumulators.values():
            accumulator.add(timestamp=messages[-1].timestamp, data=data)
        if messages[-1].timestamp < self._next_update:
            return []
        return self._produce_update(messages[-1].key, messages[-1].timestamp)

    def _preprocess(self, message: Message[T]) -> None:
        if self.start_time().value_ns > self._last_clear:
            self._preprocessor.clear()
            for accumulator in self._accumulators.values():
                accumulator.clear()
            self._last_clear = self.start_time().value_ns
            # Set next update to current message to avoid lag in user experience.
            self._next_update = message.timestamp
        self._preprocessor.add(message.timestamp, message.value)

    def _produce_update(self, key: MessageKey, timestamp: int) -> list[Message[V]]:
        # If there were no pulses for a while we need to skip several updates.
        # Note that we do not simply set _next_update based on reference_time
        # to avoid drifts.
        self._next_update += (
            (timestamp - self._next_update) // self.update_every().value_ns + 1
        ) * self.update_every().value_ns
        return [
            Message(
                timestamp=timestamp,
                key=replace(
                    key,
                    # Extract the instrument name from the topic name.
                    topic=f'{key.topic.split("_")[0]}_beamlime_data',
                    source_name=source_name(key.source_name, name),
                ),
                value=accumulator.get(),
            )
            for name, accumulator in self._accumulators.items()
        ]
