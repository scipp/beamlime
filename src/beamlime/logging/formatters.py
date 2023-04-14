# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from collections import OrderedDict
from functools import partial
from logging import Formatter, LogRecord
from typing import Any, Literal, Mapping, Union, overload

_FormatStyle = Literal["{"]
_SepStyle = Literal["|", ","]


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
    ansi_colored:
        If the column is colored with ansi-code. Default is False.
        This property is only used when a LogHeader creates a formatter of a log.
        If the column is colored, it will be wrapped by
        pre-defined color-code columns (ansi_color, reset_color) in the formatter.

    Examples
    --------
    >>> time_column = LogColumn(variable_name='asctime',\
    >>> min_length=8, title='TIME', style='{', ansi_colored=False)
    >>> time_column.formatter
    {asctime:8}
    >>> time_column.title
    TIME
    >>> str(time_column).replace(' ', '_')
    TIME____
    >>> app_name_column = LogColumn(variable_name='app_name',\
    >>> title='APPLICATION', style='{', ansi_colored=True)
    >>> app_name_column.formatter
    {app_name}
    >>> app_name_column.title
    APPLICATION

    """

    def __init__(
        self,
        variable_name: str,
        *,
        min_length: int = None,
        title: str = None,
        style: _FormatStyle = "{",
        ansi_colored: bool = False,
    ) -> None:
        self.variable_name = variable_name
        self.title = variable_name.capitalize() if title is None else title
        self.min_length = min_length
        self.style = style
        self.ansi_colored = ansi_colored

    @property
    def formatter(self) -> str:
        if self.min_length is not None:
            length_desc = ":" + str(self.min_length)
        else:
            length_desc = ""

        return "{" + f"{self.variable_name}{length_desc}" + "}"

    def __str__(self) -> str:
        return self.formatter.format(**{self.variable_name: self.title})

    def __repr__(self) -> str:
        return (
            f"LogHeader(variable_name: {self.variable_name}, "
            f"title: {self.title}, minimum length: {self.min_length})"
        )


class LogHeader(OrderedDict):
    """
    Formatter constructor from a group of ``LogColumn``s.

    # TODO: Add link to the logging example here.

    Examples
    --------
    >>> # 1
    >>> time_column = LogColumn(variable_name='asctime',\
    >>> min_length=8, title='TIME', style='{', ansi_colored=False)
    >>> app_name_column = LogColumn(variable_name='app_name',\
    >>> title='APPLICATION', style='{', ansi_colored=True)
    >>> header = LogHeader(time_column, app_name_column)
    >>> header.fmt
    {asctime:8} | {ansi_color}{app_name}{reset_color}
    >>> header.format().replace(' ', '_')
    TIME_____|_APPLICATION
    >>> # 2
    >>> message_column = LogColumn(variable_name='msg',\
    >>> title='MESSAGE', style='{', ansi_colored=False)
    >>> header = LogHeader(time_column, app_name_column, message_column)
    >>> header.fmt
    {asctime:8} | {ansi_color}{app_name}{reset_color} | {msg}
    >>> # 3
    >>> colored_message_column = LogColumn(variable_name='msg',\
    >>> title='MESSAGE', style='{', ansi_colored=True)
    >>> header = LogHeader(time_column, app_name_column, colored_message_column)
    >>> header.fmt
    {asctime:8} | {ansi_color}{app_name} | {msg}{reset_color}

    """

    @overload
    def __init__(self, *fmt: str, padding: tuple, sep: _SepStyle):
        ...

    @overload
    def __init__(self, *columns: LogColumn, padding: tuple, sep: _SepStyle):
        ...

    def __init__(
        self,
        *args: Union[str, LogColumn],
        padding: tuple = (1, 1),
        sep: _SepStyle = "|",
    ):
        self.padding = padding
        self.sep = sep
        if len(args) > 1 and isinstance(args[0], LogColumn):
            self._init_from_columns(*args)

        elif len(args) > 1 and isinstance(args[0], str):
            self._init_from_literal_fmt(*args)

    def _init_from_columns(self, *columns: LogColumn) -> None:
        if len(columns) == 1:
            self._fmt = columns[0].formatter
        else:
            sep = " " * self.padding[0] + self.sep + " " * self.padding[1]
            _ansi_color_col = LogColumn("ansi_color", title="", ansi_colored=False)
            _reset_color_col = LogColumn("reset_color", title="", ansi_colored=False)

            _padded_columns = [_ansi_color_col] + list(columns) + [_reset_color_col]

            # Color starting/terminating flags
            _color_starts = [
                True if column.ansi_colored and not p_column.ansi_colored else False
                for column, p_column in zip(columns, _padded_columns[:-2])
            ]
            _color_ends = [
                True if column.ansi_colored and not n_column.ansi_colored else False
                for column, n_column in zip(columns, _padded_columns[2:])
            ]

            # Wrap columns with {ansi_color or color_key} and {reset_color}
            _color_started_fmts = [
                _ansi_color_col.formatter + column.formatter
                if _color_starts[icol]
                else column.formatter
                for icol, column in enumerate(columns)
            ]
            _colored_fmts = [
                fmt + _reset_color_col.formatter if _color_ends[icol] else fmt
                for icol, fmt in enumerate(_color_started_fmts)
            ]

            self._fmt = sep.join(_colored_fmts)

        for column in _padded_columns:
            self.__setitem__(column.variable_name, column)

    def _init_from_literal_fmt(self, *fmts: str) -> None:
        self._fmt = "".join(fmts)

        def wrap_header(fmt_piece: str) -> LogColumn:
            if ":" in fmt_piece:
                variable_name, min_length_str = fmt_piece.split(":")
                return LogColumn(variable_name, min_length=int(min_length_str))
            else:
                LogColumn(fmt_piece)

        chunks = self._fmt.split("{")
        fmt_pieces = [chunk.split("}")[0] for chunk in chunks]
        columns = [wrap_header(fmt_piece) for fmt_piece in fmt_pieces]

        for column in columns:
            self.__setitem__(column.variable_name, column)

    @property
    def fmt(self):
        return self._fmt

    def __getitem__(self, key: str) -> LogColumn:
        return super().__getitem__(key)

    def __setitem__(self, key: str, item: LogColumn) -> None:
        key = item.variable_name
        return super().__setitem__(key, item)

    def format(self) -> str:
        return self.fmt.format(
            **{column.variable_name: column.title for column in self.values()}
        )


DEFAULT_HEADERS = LogHeader(
    LogColumn("asctime", title="TIME", min_length=23),
    LogColumn("app_name", title="APPLICATION", min_length=15),
    LogColumn("levelname", title="LEVEL", min_length=8),
    LogColumn("message"),
    padding=(1, 1),
    sep="|",
)

DEFAULT_COLOR_HEADERS = LogHeader(
    LogColumn("asctime", title="TIME", min_length=23),
    LogColumn("app_name", title="APPLICATION", min_length=15, ansi_colored=True),
    LogColumn("levelname", title="LEVEL", min_length=8, ansi_colored=True),
    LogColumn("message", ansi_colored=True),
    padding=(1, 1),
    sep="|",
)


class _HeaderFormatter(Formatter):
    """
    Log formatter with a header.
    """

    def __init__(
        self,
        *,
        fmt: Union[str, None] = None,
        datefmt: Union[str, None] = None,
        style: _FormatStyle = "{",
        validate: bool = True,
        defaults: Union[Mapping[str, Any], None] = None,
        header_sep: _SepStyle = "|",
        headers: LogHeader = DEFAULT_HEADERS,
    ) -> None:
        if fmt is None and headers is not None:
            fmt = headers.fmt
        elif fmt is not None:
            headers = LogHeader(fmt, padding=(1, 1), sep=header_sep)

        self._header = headers.format()
        try:
            super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        except TypeError:  # py39
            # ``defaults`` does not exist in ``logging.Formatter`` for python3.9
            super().__init__(fmt, datefmt, style, validate)

    @property
    def header(self) -> str:
        return self._header

    def format(self, record: LogRecord) -> str:
        return super().format(record)


BeamlimeFormatter = partial(_HeaderFormatter, headers=DEFAULT_HEADERS)
BeamlimeAnsiColorFormatter = partial(_HeaderFormatter, headers=DEFAULT_COLOR_HEADERS)
