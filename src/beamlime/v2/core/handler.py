import logging
from typing import Any, Generic, Protocol, TypeVar

TIn = TypeVar('TIn')
TOut = TypeVar('TOut')


class Config(Protocol):
    def get_config(self, key: str, default: Any | None = None) -> Any:
        pass


class Consumer(Protocol, Generic[TIn]):
    def get_messages(self) -> list[TIn]:
        pass


class Producer(Protocol, Generic[TOut]):
    def publish_messages(self, topic: str, messages: list[TOut]) -> None:
        pass


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
