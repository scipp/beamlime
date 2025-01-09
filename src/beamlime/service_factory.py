# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Generic, TypeVar

import confluent_kafka

from .core import ConfigSubscriber, HandlerRegistry, MessageSink, StreamProcessor
from .core.handler import Handler
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
        handler_cls: type[Handler[Tin, Tout]],
    ):
        self._name = f'{instrument}_{name}'
        self._log_level = log_level
        self._adapter = adapter
        self._handler_cls = handler_cls

    def build(
        self,
        control_consumer: confluent_kafka.Consumer,
        consumer: KafkaConsumer,
        sink: MessageSink[Tout],
    ) -> Service:
        config_subscriber = ConfigSubscriber(consumer=control_consumer, config={})
        processor = StreamProcessor(
            source=AdaptingMessageSource(
                source=KafkaMessageSource(consumer=consumer), adapter=self._adapter
            ),
            sink=sink,
            handler_registry=HandlerRegistry(
                config=config_subscriber, handler_cls=self._handler_cls
            ),
        )
        return Service(
            children=[config_subscriber],
            processor=processor,
            name=self._name,
            log_level=self._log_level,
        )
