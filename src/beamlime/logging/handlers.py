# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import FileHandler, Formatter, StreamHandler
from typing import Optional, Union

from .formatters import BeamlimeAnsiColorFormatter, BeamlimeFormatter, _HeaderFormatter
from .records import BeamlimeColorLogRecord, BeamlimeLogRecord


class _HeaderMixin:
    def emit_header(self):
        try:
            msg = self.formatter.header
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except RecursionError:
            raise

    def setFormatter(self, fmt: Union[_HeaderFormatter, Formatter, None]) -> None:
        super().setFormatter(fmt)
        if hasattr(fmt, "header") and self.header:
            self.emit_header()


class BeamlimeFileHandler(_HeaderMixin, FileHandler):
    def __init__(
        self,
        filename: str,
        *,
        mode: str = "a",
        encoding: Union[str, None] = None,
        delay: bool = False,
        errors: Union[str, None] = None,
        header: bool = True,
    ) -> None:
        self.header = header
        super().__init__(filename, mode, encoding, delay, errors)
        self.setFormatter(BeamlimeFormatter())

    def handle(self, record: BeamlimeLogRecord) -> bool:
        if not isinstance(record, BeamlimeLogRecord):
            return super().handle(BeamlimeLogRecord(record))
        return super().handle(record)


class BeamlimeStreamHandler(_HeaderMixin, StreamHandler):
    def __init__(self, header: bool = False):
        from itertools import cycle

        from colorama.ansi import Fore, Style

        self.header = header
        self.palette = [
            Fore.BLACK,
            Fore.GREEN,
            Fore.BLUE,
            Fore.RED,
            Fore.MAGENTA,
            Fore.LIGHTBLUE_EX,
        ]
        self.color_list = cycle(self.palette)
        self.color_map = {"": Style.RESET_ALL}
        super().__init__()
        self.setFormatter(BeamlimeAnsiColorFormatter())

    def set_application_color(
        self, app_name: str, ansi_color: Optional[str] = None
    ) -> None:
        self.color_map.update({app_name: ansi_color or next(self.color_list)})

    def get_application_color(self, app_name: str) -> str:
        if app_name not in self.color_map:
            self.set_application_color(app_name)
        return self.color_map[app_name]

    def handle(self, record: BeamlimeColorLogRecord) -> bool:
        if not isinstance(record, BeamlimeColorLogRecord):
            bl_record = BeamlimeColorLogRecord(record)
        else:
            bl_record = record

        bl_record.ansi_color = self.get_application_color(app_name=bl_record.app_name)
        return super().handle(bl_record)
