# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

Tin = TypeVar('Tin')
Tout = TypeVar('Tout')


@dataclass(frozen=True, slots=True, kw_only=True)
class Message(Generic[Tin]):
    timestamp: int
    topic: str
    value: Tin

    def __lt__(self, other: Message[Tin]) -> bool:
        return self.timestamp < other.timestamp


class Config(Protocol):
    def get(self, key: str, default: Any | None = None) -> Any:
        pass


class Consumer(Protocol, Generic[Tin]):
    def get_messages(self) -> list[Message[Tin]]:
        pass


class Producer(Protocol, Generic[Tout]):
    def publish_messages(self, messages: list[Message[Tout]]) -> None:
        """
        Publish messages to the producer.

        Args:
            messages: A dictionary of messages to publish, where the key is the
                topic and the value is the message.
        """


class ConfigProxy:
    """Proxy for accessing configuration, prefixed with a namespace."""

    def __init__(self, config: Config, *, namespace: str):
        self._config = config
        self._namespace = namespace

    def get(self, key: str, default: Any | None = None) -> Any:
        return self._config.get(f'{self._namespace}.{key}', default)


class Handler(Generic[Tin, Tout]):
    def __init__(self, *, logger: logging.Logger | None, config: Config):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config

    def handle(self, message: Message[Tin]) -> list[Message[Tout]]:
        raise NotImplementedError


class HandlerRegistry(Generic[Tin, Tout]):
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
        self._handlers: dict[str, Handler] = {}

    def get(self, topic: str) -> Handler:
        if topic not in self._handlers:
            self._handlers[topic] = self._handler_cls(
                logger=self._logger, config=ConfigProxy(self._config, namespace=topic)
            )
        return self._handlers[topic]


class MessageHandlerLoop(Generic[Tin, Tout]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        config: Config,
        consumer: Consumer[Tin],
        producer: Producer[Tout],
        handler_registry: HandlerRegistry[Tin, Tout],
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._consumer = consumer
        self._producer = producer
        self._running = False
        self._handler_registry = handler_registry

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        self._logger.info('Starting handler...')
        self._running = True
        try:
            while self.is_running:
                self.handle()
        except KeyboardInterrupt:
            pass
        finally:
            self._logger.info('Stopping handler...')

    def stop(self) -> None:
        self._running = False

    def handle(self) -> None:
        messages = self._consumer.get_messages()
        results = []
        for msg in messages:
            handler = self._handler_registry.get(msg.topic)
            results.extend(handler.handle(msg))
        self._producer.publish_messages(results)
