# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import FileHandler, Formatter, StreamHandler

from .formatters import BeamlimeAnsiColorFormatter, BeamlimeFormatter
from .records import BeamlimeColorLogRecord, BeamlimeLogRecord


class _HeaderMixin:
    def add_header(self):
        try:
            msg = self.formatter.header
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except RecursionError:
            raise

    def setFormatter(self, fmt: Formatter | BeamlimeFormatter | None) -> None:
        super().setFormatter(fmt)
        if hasattr(fmt, "header") and self.header:
            self.add_header()


class BeamlimeFileHandler(_HeaderMixin, FileHandler):
    def __init__(
        self,
        filename: str,
        mode: str = "a",
        encoding: str | None = None,
        delay: bool = False,
        errors: str | None = None,
        header: bool = True,
    ) -> None:
        import logging

        if not issubclass(logging.getLogRecordFactory(), BeamlimeLogRecord):
            logging.setLogRecordFactory(BeamlimeLogRecord)
        self.header = header
        super().__init__(filename, mode, encoding, delay, errors)
        self.setFormatter(BeamlimeFormatter())


class BeamlimeStreamHandler(_HeaderMixin, StreamHandler):
    def __init__(self, header: bool = False):
        import logging

        from colorama.ansi import Fore

        if not issubclass(logging.getLogRecordFactory(), BeamlimeColorLogRecord):
            logging.setLogRecordFactory(BeamlimeLogRecord)
        self.header = header
        self.color_list = [
            Fore.BLACK,
            Fore.GREEN,
            Fore.BLUE,
            Fore.RED,
            Fore.MAGENTA,
            Fore.LIGHTBLUE_EX,
        ]
        self.color_map = {"": "\033[0m"}
        super().__init__()
        self.setFormatter(BeamlimeAnsiColorFormatter())

    def handle(self, record: BeamlimeColorLogRecord) -> bool:
        if record.app_name not in self.color_map:
            if self.color_list:
                self.color_map[record.app_name] = self.color_list.pop()
            else:
                # TODO: Update this if we need more colors
                self.color_map[record.app_name] = self.color_map[""]

        record.ansi_color = self.color_map[record.app_name]
        return super().handle(record)
