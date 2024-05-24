# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import Logger
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class LoggingProtocol(Protocol):  # pragma: no cover
    """General Logging Protocol"""

    @property
    def logger(self) -> Logger: ...

    def debug(self, msg: str, *args: Any) -> None: ...

    def info(self, msg: str, *args: Any) -> None: ...

    def warning(self, msg: str, *args: Any) -> None: ...

    def error(self, msg: str, *args: Any) -> None: ...
