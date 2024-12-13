# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any


def _compose_msg(application: str, message: str) -> str:
    from .formatters import BEAMLIME_MESSAGE_HEADERS

    return BEAMLIME_MESSAGE_HEADERS.fmt % (application, message)


class LogMixin:
    """Logging interfaces."""

    def debug(self, msg: str, *args: Any, stacklevel=2, **kwargs: Any) -> None:
        self.logger.debug(
            _compose_msg(self.__class__.__qualname__, msg),
            *args,
            stacklevel=stacklevel,
            **kwargs,
        )

    def info(self, msg: str, *args: Any, stacklevel=2, **kwargs: Any) -> None:
        self.logger.info(
            _compose_msg(self.__class__.__qualname__, msg),
            *args,
            stacklevel=stacklevel,
            **kwargs,
        )

    def warning(self, msg: str, *args: Any, stacklevel=2, **kwargs: Any) -> None:
        self.logger.warning(
            _compose_msg(self.__class__.__qualname__, msg),
            *args,
            stacklevel=stacklevel,
            **kwargs,
        )

    def error(self, msg: str, *args: Any, stacklevel=2, **kwargs: Any) -> None:
        self.logger.error(
            _compose_msg(self.__class__.__qualname__, msg),
            *args,
            stacklevel=stacklevel,
            **kwargs,
        )
