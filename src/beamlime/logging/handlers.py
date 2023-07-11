# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# flake8: noqa B010
from __future__ import annotations

from logging import FileHandler, LogRecord, StreamHandler
from typing import NewType, Optional

from colorama import Style

from ..empty_providers import log_providers
from .formatters import (
    BeamlimeHeaderFormatter,
    BeamlimeLogMessage,
    ColoredFormatter,
    DefaultFormatter,
)
from .resources import FileHandlerBasePath

HandlerHeaderFlag = NewType("HandlerHeaderFlag", bool)


# TODO: Remove type ignore tag.
# strict type check fails due to missing type-arg
# even though ``StreamHandler`` is not ``Generic``
# hence ``StreamHandler[T]`` is not possible in the runtime.
class _HeaderMixin(StreamHandler):  # type:ignore[type-arg]
    """
    Log handler mixin with headers.
    It emits header column once a new formatter is set if available.
    Disable emitting headers by set the ``self.header`` to ``False``.
    """

    header: HandlerHeaderFlag = HandlerHeaderFlag(True)

    @property  # type:ignore[override]
    def formatter(self) -> BeamlimeHeaderFormatter:
        # TODO: ``logging.Handler`` does not have a type-hint for formatter
        # and the default value is None,
        # so even if ``BeamlimeHeaderFormatter`` inherits ``logging.Formatter``
        # the static type is not compatible.
        # This may be resolved when ``Handler`` has ``formatter`` type-hinted
        # and mypy allows overriding property with a child class.
        return self._formatter

    @formatter.setter
    def formatter(self, _formatter: BeamlimeHeaderFormatter) -> None:
        self._formatter: BeamlimeHeaderFormatter = _formatter
        if hasattr(_formatter, "header_fmt") and self.header:
            self.emit_header()

    def emit_header(self) -> None:
        msg = self.formatter.header_fmt + self.terminator
        self.stream.write(msg)
        self.flush()


BeamlimeFileFormatter = NewType("BeamlimeFileFormatter", BeamlimeHeaderFormatter)
DefaultBeamlimeFileFormatter = BeamlimeFileFormatter(DefaultFormatter)
log_providers[BeamlimeFileFormatter] = lambda: DefaultBeamlimeFileFormatter


@log_providers.provider
class BeamlimeFileHandler(_HeaderMixin, FileHandler):
    __formatter: BeamlimeFileFormatter = DefaultBeamlimeFileFormatter

    def __init__(self, filename: FileHandlerBasePath) -> None:
        self.baseFilename = str(filename)
        super().__init__(self.baseFilename)
        self.formatter = self.__formatter


class _ColorLogRecord(LogRecord):
    """
    Add ``ansi_color`` and ``reset_color`` to the record for color formatter.
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


@log_providers.provider
class BeamlimeStreamHandler(_HeaderMixin):
    """
    Stream handler with ansi-color applied text for terminal display.

    Notes
    -----
    It will assign the color to the each application from the cyclic palette,
    therefore multiple applications may have the same color.

    """

    formatter: BeamlimeStreamFormatter

    def __init__(self) -> None:
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
        """Store ``ansi_color`` for ``app_name`` for future usage."""
        self.color_map[app_name] = ansi_color or next(self.color_list)

    def get_application_color(self, app_name: str) -> str:
        """Create or retrieve a color for ``app_name``."""
        if app_name not in self.color_map:
            self.set_application_color(app_name)
        return self.color_map[app_name]

    def handle(self, record: LogRecord) -> bool:
        """Add ``ansi_color`` field to an existing record and emit it."""
        if isinstance(record.msg, BeamlimeLogMessage) and (
            len((name_msg := str(record.msg).split("|"))) == 2
        ):
            app_name = name_msg[0]
        else:
            app_name = ""
        app_color = self.get_application_color(app_name)
        colored_record = _ColorLogRecord(record, ansi_color=app_color)
        return super().handle(colored_record)
