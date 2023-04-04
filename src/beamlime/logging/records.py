# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import LogRecord
from types import TracebackType
from typing import Mapping, TypeAlias

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
                self.msg = msg.get("msg")
            else:
                self.msg = str(msg)
                self.exc_info = Warning(
                    "Unexpected form of message" " for BeamlimeLogRecord."
                )
        else:
            self.app_name = ""
            self.msg = msg
        super().__init__(
            name, level, pathname, lineno, self.msg, args, exc_info, func, sinfo
        )


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
        self.ansi_color = ""
        self.reset_color = "\033[0m"
        super().__init__(
            name, level, pathname, lineno, msg, args, exc_info, func, sinfo
        )
