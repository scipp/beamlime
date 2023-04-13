# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import LogRecord
from types import TracebackType
from typing import Mapping, Union

_SysExcInfoType = Union[
    tuple[type[BaseException], BaseException, Union[TracebackType, None]],
    tuple[None, None, None],
]

_ArgsType = Union[tuple[object, ...], Mapping[str, object]]


class BeamlimeLogRecord(LogRecord):
    """
    Note
    ----
    First 8 Arguments cannot be keyword-only arguments
    because of the way ``logging`` creates an empty record.
    It uses positional arguments to create a record instance.
    """

    def __init__(
        self,
        *,
        name: str,
        level: int,
        msg: object,
        pathname: Union[str, None] = None,
        lineno: Union[int, None] = None,
        func: Union[str, None] = None,
        sinfo: Union[str, None] = None,
        args: Union[_ArgsType, None] = None,
        exc_info: Union[_SysExcInfoType, None] = None,
        app_name: str = "",
    ) -> None:
        self.app_name = app_name
        super().__init__(
            name, level, pathname, lineno, msg, args, exc_info, func, sinfo
        )


class BeamlimeColorLogRecord(BeamlimeLogRecord):
    def __init__(
        self,
        *,
        name: str,
        level: int,
        msg: object,
        pathname: Union[str, None] = None,
        lineno: Union[int, None] = None,
        func: Union[str, None] = None,
        sinfo: Union[str, None] = None,
        args: Union[_ArgsType, None] = None,
        exc_info: Union[_SysExcInfoType, None] = None,
        app_name: str = "",
    ) -> None:
        from colorama import Style

        self.ansi_color = ""
        self.reset_color = Style.RESET_ALL
        super().__init__(
            name=name,
            level=level,
            pathname=pathname,
            lineno=lineno,
            msg=msg,
            args=args,
            exc_info=exc_info,
            func=func,
            sinfo=sinfo,
            app_name=app_name,
        )
