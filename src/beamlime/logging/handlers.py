# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# flake8: noqa B010
from __future__ import annotations

from logging import FileHandler, LogRecord, StreamHandler
from typing import NewType, Optional

from colorama import Style

from ..empty_binders import IncompleteLoggingBinder
from .formatters import (
    BeamlimeHeaderFormatter,
    BeamlimeLogMessage,
    ColoredFormatter,
    DefaultFormatter,
)
from .resources import FileHandlerBasePath

HandlerHeaderFlag = NewType("HandlerHeaderFlag", bool)


class _HeaderMixin:
    """
    Log handler mixin with headers.
    It emits header column once a new formatter is set if available.
    Disable emitting headers by set the ``self.header`` to ``False``.
    """

    header: HandlerHeaderFlag = HandlerHeaderFlag(True)

    @property
    def formatter(self) -> BeamlimeHeaderFormatter:
        return self._formatter

    @formatter.setter
    def formatter(self, _formatter: BeamlimeHeaderFormatter) -> None:
        self._formatter: BeamlimeHeaderFormatter = _formatter
        if hasattr(_formatter, "header_fmt") and self.header:
            self.emit_header()

    def emit_header(self):
        msg = self.formatter.header_fmt + self.terminator
        self.stream.write(msg)
        self.flush()


BeamlimeFileFormatter = NewType("BeamlimeFileFormatter", BeamlimeHeaderFormatter)
DefaultBeamlimeFileFormatter = BeamlimeFileFormatter(DefaultFormatter)


@IncompleteLoggingBinder.provider
class BeamlimeFileHandler(_HeaderMixin, FileHandler):
    formatter: BeamlimeFileFormatter

    def __init__(
        self,
        filename: FileHandlerBasePath,
        *,
        mode="a",
        encoding=None,
        delay=False,
        errors=None,
    ) -> None:
        super().__init__(filename, mode, encoding, delay, errors)
        self.formatter = DefaultBeamlimeFileFormatter


class _ColorLogRecord(LogRecord):
    """
    Add ansi_color and reset_color to the record for color formatter.
    """

    def __init__(
        self,
        record: LogRecord,
        ansi_color: str = "",
        reset_color: str = Style.RESET_ALL,
    ) -> None:
        self.__dict__ = dict(record.__dict__)
        self.ansi_color = ansi_color
        self.reset_color = reset_color


BeamlimeStreamFormatter = NewType("BeamlimeStreamFormatter", BeamlimeHeaderFormatter)
DefaultBeamlimeStreamFormatter = BeamlimeStreamFormatter(ColoredFormatter)


@IncompleteLoggingBinder.provider
class BeamlimeStreamHandler(_HeaderMixin, StreamHandler):
    formatter: BeamlimeStreamFormatter

    def __init__(self):
        from itertools import cycle

        from colorama.ansi import Fore, Style

        super().__init__()
        self.palette = [
            Fore.GREEN,
            Fore.BLUE,
            Fore.RED,
            Fore.MAGENTA,
            Fore.LIGHTBLUE_EX,
        ]
        self.color_list = cycle(self.palette)
        self.color_map = {"": Style.RESET_ALL}
        self.formatter = DefaultBeamlimeStreamFormatter

    def set_application_color(
        self, app_name: str, ansi_color: Optional[str] = None
    ) -> None:
        self.color_map[app_name] = ansi_color or next(self.color_list)

    def get_application_color(self, app_name: str) -> str:
        if app_name not in self.color_map:
            self.set_application_color(app_name)
        return self.color_map[app_name]

    def handle(self, record: LogRecord) -> bool:
        if isinstance(record.msg, BeamlimeLogMessage) and (
            len((name_msg := str(record.msg).split("|"))) == 2
        ):
            app_name = name_msg[0]
        else:
            app_name = ""
        app_color = self.get_application_color(app_name)
        colored_record = _ColorLogRecord(record, ansi_color=app_color)
        return super().handle(colored_record)
