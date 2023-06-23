# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import Logger
from typing import Protocol, runtime_checkable


@runtime_checkable
class BeamlimeLoggingProtocol(Protocol):  # pragma: no cover
    """General Logging Protocol"""

    @property
    def logger(self) -> Logger:
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
class ControlProtocol(Protocol):
    """Application Control Protocol"""

    @property
    def started(self) -> bool:
        ...

    @property
    def paused(self) -> bool:
        ...

    def start(self):
        ...

    def stop(self):
        ...

    def pause(self):
        ...

    def resume(self):
        ...
