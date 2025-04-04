# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from contextlib import ExitStack
from typing import Any, Generic, TypeVar

from .core import MessageSink, StreamProcessor
from .core.handler import HandlerFactory, HandlerRegistry
from .core.message import MessageSource, StreamId
from .core.service import Service
from .kafka import consumer as kafka_consumer
from .kafka.message_adapter import (
    AdaptingMessageSource,
    IdentityAdapter,
    KafkaTopic,
    MessageAdapter,
    RouteByTopicAdapter,
)
from .kafka.routes import beamlime_config_route
from .kafka.source import KafkaConsumer, KafkaMessageSource, MultiConsumer

Traw = TypeVar("Traw")
Tin = TypeVar("Tin")
Tout = TypeVar("Tout")


class DataServiceBuilder(Generic[Traw, Tin, Tout]):
    def __init__(
        self,
        *,
        instrument: str,
        name: str,
        log_level: int = logging.INFO,
        routes: MessageAdapter | dict[KafkaTopic, MessageAdapter] | None = None,
        handler_factory: HandlerFactory[Tin, Tout],
    ):
        self._name = f'{instrument}_{name}'
        self._log_level = log_level
        self._topics: list[KafkaTopic] | None = None
        self._instrument = instrument
        if routes is None:
            self._adapter = IdentityAdapter()
        elif isinstance(routes, dict):
            self._topics = list(routes.keys())
            self._adapter = RouteByTopicAdapter(
                {**routes, **beamlime_config_route(instrument)}
            )
        else:
            self._adapter = routes
        self._handler_registry = HandlerRegistry(factory=handler_factory)
        self._exit_stack = ExitStack()

    @property
    def topics(self) -> list[KafkaTopic]:
        """Returns the list of topics to subscribe to."""
        if self._topics is None:
            raise ValueError('Topics not set. Use routes to set topics.')
        return self._topics

    def add_handler(self, key: StreamId, handler: HandlerFactory[Tin, Tout]) -> None:
        """Add specific handler to use for given key, instead of using the factory."""
        self._handler_registry.register_handler(key=key, handler=handler)

    def __enter__(self) -> DataServiceBuilder[Traw, Tin, Tout]:
        self._exit_stack.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

    def create_consumer(
        self, kafka_config: dict[str, Any], consumer_group: str
    ) -> KafkaConsumer:
        """Create and register consumer with the exit stack."""
        control_consumer = self._exit_stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=self._instrument)
        )
        data_consumer = self._exit_stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=self.topics, config=kafka_config, group=consumer_group
            )
        )
        return MultiConsumer([control_consumer, data_consumer])

    def from_consumer(
        self, consumer: KafkaConsumer, sink: MessageSink[Tout]
    ) -> Service:
        return self.from_source(source=KafkaMessageSource(consumer=consumer), sink=sink)

    def from_source(self, source: MessageSource, sink: MessageSink[Tout]) -> Service:
        processor = StreamProcessor(
            source=AdaptingMessageSource(source=source, adapter=self._adapter),
            sink=sink,
            handler_registry=self._handler_registry,
        )
        return Service(processor=processor, name=self._name, log_level=self._log_level)
