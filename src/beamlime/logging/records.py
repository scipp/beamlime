# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import LogRecord
from types import TracebackType
from typing import Mapping, TypeAlias, final

_SysExcInfoType: TypeAlias = (
    tuple[type[BaseException], BaseException, TracebackType | None]
    | tuple[None, None, None]
)
_ArgsType: TypeAlias = tuple[object, ...] | Mapping[str, object]


class BeamlimeLogRecord(LogRecord):
    def __init__(
        self,
        name: str,
        level: int,
        pathname: str,
        lineno: int,
        msg: object,
        args: _ArgsType | None,
        exc_info: _SysExcInfoType | None,
        func: str | None = None,
        sinfo: str | None = None,
    ) -> None:
        if isinstance(msg, dict):
            if "app_name" in msg:
                self.app_name = msg.get("app_name")
                _msg = msg.get("msg")
            else:
                _msg = str(msg)
                self.exc_info = Warning(
                    "Unexpected form of message for BeamlimeLogRecord."
                )
        else:
            self.app_name = ""
            _msg = msg
        super().__init__(
            name, level, pathname, lineno, _msg, args, exc_info, func, sinfo
        )

    @final
    def getMessage(self) -> str:
        """
        Scipp objects are often logged as ``args``,
        which is formatted by ``%`` style in ``msg``.
        So if `getMessage` needs to be updated,
        it should still include the % formatting that ``logging.LogRecord`` has.

        ```
        msg = str(self.msg)
        if self.args:
            msg = msg % self.args
        ```
        """
        return super().getMessage()


class BeamlimeColorLogRecord(BeamlimeLogRecord):
    def __init__(
        self,
        name: str,
        level: int,
        pathname: str,
        lineno: int,
        msg: object,
        args: _ArgsType | None,
        exc_info: _SysExcInfoType | None,
        func: str | None = None,
        sinfo: str | None = None,
    ) -> None:
        from colorama import Style

        self.ansi_color = ""
        self.reset_color = Style.RESET_ALL
        super().__init__(
            name, level, pathname, lineno, msg, args, exc_info, func, sinfo
        )
