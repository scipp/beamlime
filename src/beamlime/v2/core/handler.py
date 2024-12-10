from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

TIn = TypeVar('TIn')
TOut = TypeVar('TOut')


@dataclass(frozen=True, slots=True)
class Message(Generic[TIn]):
    timestamp: int
    value: TIn

    def __lt__(self, other: Message[TIn]) -> bool:
        return self.timestamp < other.timestamp


class Config(Protocol):
    def get_config(self, key: str, default: Any | None = None) -> Any:
        pass


class Consumer(Protocol, Generic[TIn]):
    def get_messages(self) -> list[Message[TIn]]:
        pass


class Producer(Protocol, Generic[TOut]):
    def publish_messages(self, messages: dict[str, TOut]) -> None:
        """
        Publish messages to the producer.

        Args:
            messages: A dictionary of messages to publish, where the key is the
                topic and the value is the message.
        """


class Handler(Generic[TIn, TOut]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        config: Config,
        consumer: Consumer[TIn],
        producer: Producer[TOut],
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._consumer = consumer
        self._producer = producer
