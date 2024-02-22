from ..logging.mixins import LogMixin
from beamlime.logging import BeamlimeLogger
from abc import ABC, abstractmethod
from typing import NewType, List, Any, Callable, Awaitable, Generator
from dataclasses import dataclass
from contextlib import contextmanager
TargetCounts = NewType("TargetCounts", int)

class BaseDaemon(LogMixin, ABC):
    logger: BeamlimeLogger

    def data_pipe_monitor(
        self,
        pipe: List[Any],
        timeout: float = 5,
        interval: float = 1 / 14,
        preferred_size: int = 1,
        target_size: int = 1,
    ):
        from beamlime.core.schedulers import async_retry

        @async_retry(
            TimeoutError, max_trials=int(timeout / interval), interval=interval
        )
        async def wait_for_preferred_size() -> None:
            if len(pipe) < preferred_size:
                raise TimeoutError

        async def is_pipe_filled() -> bool:
            try:
                await wait_for_preferred_size()
            except TimeoutError:
                ...
            return len(pipe) >= target_size

        return is_pipe_filled

    @abstractmethod
    async def run(self):
        ...

@dataclass
class BeamlimeMessage:
    sender: Any
    receiver: Any
    content: Any


class MessageRouter(BaseDaemon):
    message_pipe: List[BeamlimeMessage]
    def __init__(self, message_pipe: List[BeamlimeMessage]):
        self.handlers: dict[type, List[Callable[[BeamlimeMessage], Any]]] = dict()
        self.awaitable_handlers: dict[type, List[Callable[[BeamlimeMessage], Awaitable[Any]]]] = dict()
        self.message_pipe = message_pipe
    
    @contextmanager
    def handler_wrapper(self, handler: Callable[..., Any], message: BeamlimeMessage) -> Generator[None, None, None]:
        import warnings
        try:
            self.debug(f"Routing event {type(message)} to handler {handler}...")
            yield
        except Exception as e:
            warnings.warn(f"Failed to handle event {type(message)}: {e}")
        else:
            self.debug(f"Routing event {type(message)} to handler {handler} done.")

    def register_awaitable_handler(self, event_tp, handler: Callable[[BeamlimeMessage], Awaitable[Any]]):
        if event_tp in self.awaitable_handlers:
            self.awaitable_handlers[event_tp].append(handler)
        else:
            self.awaitable_handlers[event_tp] = [handler]

    def register_handler(self, event_tp, handler: Callable[[BeamlimeMessage], Any]):
        if event_tp in self.handlers:
            self.handlers[event_tp].append(handler)
        else:
            self.handlers[event_tp] = [handler]

    async def route(self, message: BeamlimeMessage) -> None:
        import warnings

        if (handlers:=self.handlers.get(type(message), [])):
            for handler in handlers:
                with self.handler_wrapper(handler, message):
                    handler(message)
        if (awaitable_handlers:=self.awaitable_handlers.get(type(message), [])):
            for handler in awaitable_handlers:
                with self.handler_wrapper(handler, message):
                    await handler(message)
        if not (handlers or awaitable_handlers):
            warnings.warn(f"No handler for event {type(message)}. Ignoring...")

    async def run(self) -> None:
        data_monitor = self.data_pipe_monitor(
            self.message_pipe,
            target_size=1,
            timeout=5,
            interval=1 / 14,
        )

        while await data_monitor():
            message = self.message_pipe.pop(0)
            await self.route(message)

    def send_message(self, message: BeamlimeMessage) -> None:
        self.message_pipe.append(message)

    async def send_message_async(self, message: BeamlimeMessage) -> None:
        self.message_pipe.append(message)


class BaseHandler(LogMixin, ABC):
    logger: BeamlimeLogger
    messanger: MessageRouter

    def __init__(self, messanger: MessageRouter):
        self.messanger = messanger
        self.register_handlers()
    
    @abstractmethod
    def register_handlers(self):
        ...
