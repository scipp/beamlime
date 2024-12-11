# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Generic, Protocol

from .handler import HandlerRegistry, MessageSink, MessageSource, Tin, Tout


class Processor(Protocol):
    def process(self) -> None:
        pass


class StreamProcessor(Generic[Tin, Tout]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        source: MessageSource[Tin],
        sink: MessageSink[Tout],
        handler_registry: HandlerRegistry[Tin, Tout],
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._source = source
        self._sink = sink
        self._handler_registry = handler_registry

    def process(self) -> None:
        messages = self._source.get_messages()
        results = []
        # TODO There is a problem here. If there is no message we will never send any
        # updates to the sink. But sliding windows should run "empty" to show this to
        # the user.
        # TODO sort messages by timestamp
        for msg in messages:
            handler = self._handler_registry.get(msg.key)
            results.extend(handler.handle(msg))
        self._sink.publish_messages(results)
