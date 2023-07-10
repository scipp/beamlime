# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from .. import LoggingProtocol


def _compose_msg(application: str, message: str) -> str:
    from .formatters import BEAMLIME_MESSAGE_HEADERS, BeamlimeLogMessage

    return BeamlimeLogMessage(BEAMLIME_MESSAGE_HEADERS.fmt % (application, message))


class LogMixin:
    """
    Logging interfaces.
    Mixin assumes the inheriting class meets the ``BeamlimeApplicationProtocol``.

    Protocol
    --------
    LoggingProtocol
    """

    def debug(self: LoggingProtocol, msg: str, *args, **kwargs) -> None:
        self.logger.debug(
            _compose_msg(self.__class__.__qualname__, msg), *args, **kwargs
        )

    def info(self: LoggingProtocol, msg: str, *args, **kwargs) -> None:
        self.logger.info(
            _compose_msg(self.__class__.__qualname__, msg), *args, **kwargs
        )

    def warning(self: LoggingProtocol, msg: str, *args, **kwargs) -> None:
        self.logger.warning(
            _compose_msg(self.__class__.__qualname__, msg), *args, **kwargs
        )

    def error(self: LoggingProtocol, msg: str, *args, **kwargs) -> None:
        self.logger.error(
            _compose_msg(self.__class__.__qualname__, msg), *args, **kwargs
        )
