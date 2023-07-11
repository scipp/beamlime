# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from collections import OrderedDict
from dataclasses import dataclass
from logging import Formatter
from typing import Any, Literal, Optional

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
    title:
        Title of the column in header line. Capitalized ``variable_name`` if None.
    min_length:
        Minimum length of the column. Default is None.
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


class LogHeader(OrderedDict[str, LogColumn]):
    """
    Formatter constructor from a group of ``LogColumn``s.

    Examples
    --------
    >>> # 1
    >>> time_column = LogColumn(variable_name='asctime',
    ... min_length=8, title='TIME', style='{')
    >>> app_name_column = LogColumn(variable_name='app_name',
    ... title='APPLICATION', style='{')
    >>> header = LogHeader(time_column, app_name_column)
    >>> header.fmt
    '{asctime:8} | {app_name}'
    >>> header.format().replace(' ', '_')
    'TIME_____|_APPLICATION'
    >>> # 2
    >>> message_column = LogColumn(variable_name='msg',
    ... title='MESSAGE', style='{')
    >>> header = LogHeader(time_column, app_name_column,
    ... message_column, ansi_colored=True)
    >>> header.fmt
    '{ansi_color}{asctime:8} | {app_name} | {msg}{reset_color}'

    """

    def __init__(
        self,
        *args: LogColumn,
        padding: tuple[int, int] = (1, 1),
        sep: Literal["|", ","] = "|",
        ansi_colored: bool = False,
    ):
        self.padding = padding
        self.sep = sep
        self.ansi_colored = ansi_colored
        if len(args) > 1 and isinstance(args[0], LogColumn):
            self._init_from_columns(*args)

    def _init_from_columns(self, *columns: LogColumn) -> None:
        for column in columns:
            self[column.variable_name] = column

        sep: str = " " * self.padding[0] + self.sep + " " * self.padding[1]
        _fmt = sep.join([col.formatter for col in columns])
        if self.ansi_colored:
            _ansi_color_col = LogColumn("ansi_color", title="")
            _reset_color_col = LogColumn("reset_color", title="")
            self._fmt = _ansi_color_col.formatter + _fmt + _reset_color_col.formatter
        else:
            self._fmt = _fmt

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


class BeamlimeLogMessage(str):
    ...


BEAMLIME_MESSAGE_TITLE = BEAMLIME_MESSAGE_HEADERS.format()

DEFAULT_HEADERS = LogHeader(
    LogColumn("asctime", title="TIME", min_length=23),
    LogColumn("levelname", title="LEVEL", min_length=8),
    LogColumn("message", title=BEAMLIME_MESSAGE_TITLE),
    padding=(1, 1),
    sep="|",
)

DEFAULT_COLOR_HEADERS = LogHeader(
    LogColumn("asctime", title="TIME", min_length=23),
    LogColumn("levelname", title="LEVEL", min_length=8),
    LogColumn("message", title=BEAMLIME_MESSAGE_TITLE),
    padding=(1, 1),
    sep="|",
    ansi_colored=True,
)

DateFmt = Optional[str]
Defaults = Optional[dict]


class BeamlimeHeaderFormatter(Formatter):
    """
    Log formatter with a header.
    """

    def __init__(
        self,
        /,
        headers: LogHeader = DEFAULT_HEADERS,
        datefmt: Optional[str] = None,
        style: _FormatStyle = "{",
        validate: bool = True,
        defaults: Optional[dict[str, Any]] = None,
    ) -> None:
        self.header_fmt = headers.format()
        super().__init__(headers.fmt, datefmt, style, validate)
        try:
            super().__init__(
                headers.fmt, datefmt, style, validate, defaults=defaults
            )  # type: ignore[call-arg]
        except TypeError:  # py39
            # ``defaults`` does not exist in ``logging.Formatter`` for python3.9
            super().__init__(headers.fmt, datefmt, style, validate)


DefaultFormatter = BeamlimeHeaderFormatter(headers=DEFAULT_HEADERS)
ColoredFormatter = BeamlimeHeaderFormatter(headers=DEFAULT_COLOR_HEADERS)
