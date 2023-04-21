# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BeamlimeApplicationControlProtocol(Protocol):
    """Application Control Protocol"""

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

    def _log(self, level: int, msg: str, args: tuple):
        ...

    def debug(self, msg: str, *args) -> None:
        ...

    def info(self, msg: str, *args) -> None:
        ...

    def warning(self, msg: str, *args) -> None:
        ...

    def exception(self, msg: str, *args) -> None:
        ...

    def error(self, msg: str, *args) -> None:
        ...


@runtime_checkable
class BeamlimeDaemonAppProtocol(Protocol):
    """Daemon Application Protocol"""

    def __del__(self):
        ...


@runtime_checkable
class BeamlimeApplicationProtocol(
    BeamlimeApplicationControlProtocol,
    BeamlimeLoggingProtocol,
    BeamlimeDaemonAppProtocol,
    Protocol,
):
    """Temporary Application Protocol until we have communication broker"""

    @property
    def input_channel(self) -> object:
        ...

    @property
    def output_channel(self) -> object:
        ...

    @input_channel.setter
    def input_channel(self, channel) -> None:
        ...

    @output_channel.setter
    def output_channel(self, channel) -> None:
        ...

    def parse_config(self, config: dict) -> None:
        ...


class BeamlimeDownstreamProtocol(Protocol):
    def receive_data(self, timeout: int = 1) -> Any:
        ...

    def send_data(self, timeout: int = 1) -> Any:
        ...


class BeamlimeUpstreamProtocol(Protocol):
    def request_data(self, timeout: int = 1) -> Any:
        ...

    def serve_data(self, timeout: int = 1) -> Any:
        ...


class BeamlimeTwoWayProtocol(
    BeamlimeDownstreamProtocol, BeamlimeUpstreamProtocol, Protocol
):
    ...
