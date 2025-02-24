# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Generic, Protocol

from .handler import HandlerFactory, HandlerRegistry
from .message import MessageSink, MessageSource, Tin, Tout


class Processor(Protocol):
    """
    Protocol for a processor that processes messages. Used by :py:class:`Service`.
    """

    def process(self) -> None:
        pass


class StreamProcessor(Generic[Tin, Tout]):
    """
    Processor messages from a source using a handler and send results to a sink.

    The source, handler registry, and sink are injected at construction time.
    """

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        source: MessageSource[Tin],
        sink: MessageSink[Tout],
        handler_factory: HandlerFactory[Tin, Tout],
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._source = source
        self._sink = sink
        self._handler_registry = HandlerRegistry(factory=handler_factory)

    def process(self) -> None:
        messages = self._source.get_messages()
        messages_by_key = defaultdict(list)
        for msg in messages:
            messages_by_key[msg.key].append(msg)

        results = []
        for key, msgs in messages_by_key.items():
            handler = self._handler_registry.get(key)
            try:
                results.extend(handler.handle(msgs))
            except Exception:
                self._logger.exception('Error processing messages for key %s', key)
        self._sink.publish_messages(results)
