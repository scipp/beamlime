import logging
from typing import Any, Protocol


class Config(Protocol):
    def get_config(self, key: str, default: Any | None = None) -> Any:
        pass


class Consumer(Protocol):
    def get_messages(self) -> list[Any]:
        pass


class Producer(Protocol):
    def publish_messages(self, topic: str, messages: list[Any]) -> None:
        pass


class Handler:
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        config: Config,
        consumer: Consumer,
        producer: Producer,
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._consumer = consumer
        self._producer = producer
