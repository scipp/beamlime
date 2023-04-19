# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BeamLimeApplicationProtocol(Protocol):
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

    def start(self) -> None:
        ...

    def pause(self) -> None:
        ...

    def resume(self) -> None:
        ...

    def __del__(self) -> None:
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
