# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from collections import OrderedDict
from functools import partial
from logging import Formatter
from typing import Any, Literal, Mapping, Union, overload

_FormatStyle = Literal["{"]
_SepStyle = Literal["|", ","]


class LogColumn:
    def __init__(
        self,
        variable_name: str,
        *,
        title: str = None,
        min_length: int = None,
        style: _FormatStyle = "{",
        visible: bool = True,
    ) -> None:
        self.variable_name = variable_name
        self.title = variable_name.capitalize() if title is None else title
        self.min_length = min_length
        self.style = style
        self.visible = visible

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
            columns = args
            if len(columns) == 1:
                self._fmt = columns[0].formatter
            else:
                seps = [
                    " " * self.padding[0] + self.sep + " " * self.padding[1]
                    if column.visible and n_column.visible
                    else ""
                    for column, n_column in zip(columns[:-1], columns[1:])
                ]
                self._fmt = "".join(
                    [
                        column.formatter + sep
                        for column, sep in zip(columns, seps + [""])
                    ]
                )
        elif len(args) > 1 and isinstance(args[0], str):
            self._fmt = "".join(args)

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
    LogColumn("ansi_color", title="", visible=False),
    *DEFAULT_HEADERS.values(),
    LogColumn("reset_color", title="", visible=False),
    padding=(1, 1),
    sep="|",
)


class _HeaderFormatter(Formatter):
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
        # super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        super().__init__(fmt, datefmt, style, validate)

    @property
    def header(self):
        return self._header


BeamlimeFormatter = partial(_HeaderFormatter, headers=DEFAULT_HEADERS)
BeamlimeAnsiColorFormatter = partial(_HeaderFormatter, headers=DEFAULT_COLOR_HEADERS)
