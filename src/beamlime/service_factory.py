# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Generic, TypeVar

from .core import MessageSink, StreamProcessor
from .core.handler import HandlerRegistry
from .core.service import Service
from .kafka.message_adapter import AdaptingMessageSource, MessageAdapter
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
        adapter: MessageAdapter[Traw, Tin],
        handler_registry: HandlerRegistry[Tin, Tout],
    ):
        self._name = f'{instrument}_{name}'
        self._log_level = log_level
        self._adapter = adapter
        self._handler_registry = handler_registry

    def build(self, consumer: KafkaConsumer, sink: MessageSink[Tout]) -> Service:
        processor = StreamProcessor(
            source=AdaptingMessageSource(
                source=KafkaMessageSource(consumer=consumer), adapter=self._adapter
            ),
            sink=sink,
            handler_registry=self._handler_registry,
        )
        return Service(
            processor=processor,
            name=self._name,
            log_level=self._log_level,
        )
