# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Generic, Protocol

from .handler import Consumer, HandlerRegistry, Producer, Tin, Tout


class Processor(Protocol):
    def process(self) -> None:
        pass


class StreamProcessor(Generic[Tin, Tout]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        consumer: Consumer[Tin],
        producer: Producer[Tout],
        handler_registry: HandlerRegistry[Tin, Tout],
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._consumer = consumer
        self._producer = producer
        self._handler_registry = handler_registry

    def process(self) -> None:
        messages = self._consumer.get_messages()
        results = []
        for msg in messages:
            handler = self._handler_registry.get(msg.topic)
            results.extend(handler.handle(msg))
        self._producer.publish_messages(results)
