# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Generic, TypeVar

from .core import MessageSink, StreamProcessor
from .core.handler import HandlerFactory, HandlerRegistry
from .core.message import MessageSource, StreamId
from .core.service import Service
from .kafka.message_adapter import (
    AdaptingMessageSource,
    IdentityAdapter,
    MessageAdapter,
)
from .kafka.source import KafkaConsumer, KafkaMessageSource

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
        adapter: MessageAdapter[Traw, Tin] | None = None,
        handler_factory: HandlerFactory[Tin, Tout],
    ):
        self._name = f'{instrument}_{name}'
        self._log_level = log_level
        self._adapter = adapter or IdentityAdapter()
        self._handler_registry = HandlerRegistry(factory=handler_factory)

    def add_handler(self, key: StreamId, handler: HandlerFactory[Tin, Tout]) -> None:
        """Add specific handler to use for given key, instead of using the factory."""
        self._handler_registry.register_handler(key=key, handler=handler)

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
