# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from asyncio import Task
from logging import Logger
from typing import Any, Protocol, Union, runtime_checkable

from ..communication.broker import CommunicationBroker


@runtime_checkable
class BeamlimeControlProtocol(Protocol):
    """Application Control Protocol"""

    @property
    def _started(self) -> bool:
        ...

    @property
    def _paused(self) -> bool:
        ...

    def start(self):
        ...

    def stop(self):
        ...

    def pause(self):
        ...

    def resume(self):
        ...


@runtime_checkable
class BeamlimeLoggingProtocol(Protocol):
    """Logging Protocol"""

    @property
    def logger(self) -> Logger:
        ...

    def _log(self, level: int, msg: str, args: tuple) -> None:
        ...

    def debug(self, msg: str, *args) -> None:
        ...

    def info(self, msg: str, *args) -> None:
        ...

    def warning(self, msg: str, *args) -> None:
        ...

    def error(self, msg: str, *args) -> None:
        ...


@runtime_checkable
class BeamlimeCoroutineProtocol(Protocol):
    """Coroutine Protocol"""

    async def should_start(self) -> bool:
        ...

    async def should_proceed(self) -> bool:
        ...

    def run(self) -> Any:
        """Run the application in a dependent event loop."""
        ...

    def create_task(self, /, name=None, context=None) -> Task:
        """Start the task in the currently running event loop."""
        ...


class BrokerCommunicationProtocol(Protocol):
    """Broker Communication Protocol"""

    def get(
        self,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float,
        wait_interval: float,
        **kwargs,
    ) -> Any:
        ...

    def put(
        self,
        data: Any,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float,
        wait_interval: float,
        **kwargs,
    ) -> Any:
        ...

    def consume(
        self,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float,
        wait_interval: float,
        **kwargs,
    ) -> Any:
        ...

    def produce(
        self,
        data: Any,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float,
        wait_interval: float,
        **kwargs,
    ) -> Any:
        ...


class BeamlimeBrokerProtocol(BrokerCommunicationProtocol, Protocol):
    """Broker Communication Protocol"""

    @property
    def app_channel_mapping(self) -> dict:
        ...

    @property
    def channels(self) -> dict:
        ...


class BeamlimeCommunicationProtocol(BrokerCommunicationProtocol, Protocol):
    """Application Communication Protocol"""

    @property
    def broker(self) -> CommunicationBroker:
        ...

    @broker.setter
    def broker(self, _broker: CommunicationBroker) -> None:
        ...


@runtime_checkable
class BeamlimeApplicationProtocol(
    BeamlimeControlProtocol,
    BeamlimeLoggingProtocol,
    BeamlimeCoroutineProtocol,
    BeamlimeCommunicationProtocol,
    Protocol,
):
    @property
    def timeout(self) -> float:
        ...

    @property
    def wait_interval(self) -> float:
        ...

    async def _run(self) -> Any:
        ...

    def __del__(self) -> None:
        ...
