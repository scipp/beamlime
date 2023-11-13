# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from collections import OrderedDict
from dataclasses import dataclass
from logging import Formatter
from typing import Literal, NewType, Optional

from rich.highlighter import Highlighter
from rich.style import Style
from rich.text import Text

from ..empty_providers import log_providers

HeaderSep = "|"
_FormatStyle = Literal["{", "%"]


@dataclass
class LogColumn:
    """
    Single column of each log.

    Parameters
    ----------
    variable_name:
        Name of the variable in formatter.
    min_length:
        Minimum length of the column. Default is None.
    title:
        Title of the column in header line. Capitalized ``variable_name`` if None.
    style:
        Formatting style.

    Examples
    --------
    >>> time_column = LogColumn(variable_name='asctime',
    ... min_length=8, title='TIME', style='{')
    >>> time_column.formatter
    '{asctime:8}'
    >>> time_column.title
    'TIME'
    >>> app_name_column = LogColumn(variable_name='app_name',
    ... title='APPLICATION', style='{')
    >>> app_name_column.formatter
    '{app_name}'
    >>> app_name_column.title
    'APPLICATION'

    """

    variable_name: str
    min_length: Optional[int] = None
    title: Optional[str] = None
    style: _FormatStyle = "{"

    @property
    def formatter(self) -> str:
        if self.style == "{":
            return self._str_formatter()
        elif self.style == "%":
            return self._percent_formatter()
        raise ValueError(f"Invalid style {self.style}")

    def _str_formatter(self) -> str:
        if self.min_length is not None:
            length_desc = ":" + str(self.min_length)
        else:
            length_desc = ""

        return "{" + f"{self.variable_name}{length_desc}" + "}"

    def _percent_formatter(self) -> str:
        if self.min_length is not None:
            length_desc = "-" + str(self.min_length)
        else:
            length_desc = ""

        return "%" + f"{length_desc}" + "s"

    def format(self) -> str:
        if self.title is None:
            self.title = self.variable_name.capitalize()
        if self.style == "%":
            return self.formatter % self.title
        elif self.style == "{":
            return self.formatter.format(**{self.variable_name: self.title})
        raise ValueError(f"Invalid style {self.style}")


class LogHeader(OrderedDict[str, LogColumn]):
    """
    Formatter constructor from a group of ``LogColumn``s.

    Examples
    --------
    >>> time_column = LogColumn(variable_name='asctime',
    ... min_length=8, title='TIME', style='{')
    >>> app_name_column = LogColumn(variable_name='app_name',
    ... title='APPLICATION', style='{')
    >>> header = LogHeader(time_column, app_name_column)
    >>> header.fmt
    '{asctime:8} | {app_name}'
    >>> header.format().replace(' ', '_')
    'TIME_____|_APPLICATION'

    """

    def __init__(
        self,
        *args: LogColumn,
        padding: tuple[int, int] = (1, 1),
        sep: Literal["|", ","] = "|",
    ):
        self.padding = padding
        self.sep = sep
        if len(args) > 1 and all([isinstance(arg, LogColumn) for arg in args]):
            self._init_from_columns(*args)
        else:
            raise TypeError("All positional arguments should be ``LogColumn``.")
        self.style: _FormatStyle = self._retrieve_style(*args)

    def _retrieve_style(self, *columns: LogColumn) -> _FormatStyle:
        styles: set[_FormatStyle] = set([col.style for col in columns])
        if len(styles) > 1:
            raise ValueError("All columns should have the same style of formatting.")

        return styles.pop()

    def _init_from_columns(self, *columns: LogColumn) -> None:
        for column in columns:
            self[column.variable_name] = column

        sep: str = " " * self.padding[0] + self.sep + " " * self.padding[1]
        self._fmt = sep.join([col.formatter for col in columns])

    @property
    def fmt(self) -> str:
        return self._fmt

    def format(self) -> str:
        sep: str = " " * self.padding[0] + self.sep + " " * self.padding[1]
        return sep.join(column.format() for column in self.values())


BEAMLIME_MESSAGE_HEADERS = LogHeader(
    LogColumn("application", min_length=15, style="%"),
    LogColumn("message", style="%"),
    padding=(1, 1),
    sep="|",
)

BeamlimeDefaultHeader = NewType("BeamlimeDefaultHeader", LogHeader)


@log_providers.provider
def provide_default_headers() -> BeamlimeDefaultHeader:
    return BeamlimeDefaultHeader(
        LogHeader(
            LogColumn("asctime", title="TIME", min_length=23),
            LogColumn("levelname", title="LEVEL", min_length=8),
            LogColumn("message", title=BEAMLIME_MESSAGE_HEADERS.format()),
            padding=(1, 1),
            sep="|",
        )
    )


BeamlimeFileFormatter = NewType("BeamlimeFileFormatter", Formatter)


@log_providers.provider
def provide_file_formatter(log_header: BeamlimeDefaultHeader) -> BeamlimeFileFormatter:
    return BeamlimeFileFormatter(Formatter(log_header.fmt, style=log_header.style))


@log_providers.provider
class BeamlimeStreamHighlighter(Highlighter):
    def __init__(self) -> None:
        from itertools import cycle

        super().__init__()

        self.palette = [
            Style(color='green'),
            Style(color='blue'),
            Style(color='red'),
            Style(color='magenta'),
            Style(color='bright_blue'),
        ]
        self.style_list = cycle(self.palette)
        self.style_map = {"": Style()}

    def populate_app_style(self, app_name: str) -> None:
        """Store a style for ``app_name`` for future usage."""
        self.style_map[app_name] = next(self.style_list)

    def get_application_style(self, app_name: str) -> Style:
        """Create or retrieve a style for ``app_name``."""
        if app_name not in self.style_map:
            self.populate_app_style(app_name)
        return self.style_map[app_name]

    def _retrieve_app_name(self, text: Text) -> str:
        if len((name_msg := str(text).split("|"))) == 2:
            return name_msg[0]
        else:
            return ""

    def highlight(self, text: Text) -> None:
        """Add ``ansi_color`` field to an existing record and emit it."""
        app_name = self._retrieve_app_name(text)
        text.stylize(self.get_application_style(app_name))
