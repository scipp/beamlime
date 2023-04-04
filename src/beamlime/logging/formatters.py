# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial
from logging import Formatter
from typing import Any, List, Literal, Mapping, TypeAlias

_FormatStyle: TypeAlias = Literal["{"]
_SepStyle: TypeAlias = Literal["|", ","]


class LogHeader:
    def __init__(
        self,
        variable_name: str,
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


def retrieve_headers_from_fmt(fmt: str) -> List[LogHeader]:
    def wrap_header(fmt_piece: str) -> LogHeader:
        if ":" in fmt_piece:
            variable_name, min_length_str = fmt_piece.split(":")
            return LogHeader(variable_name, min_length=int(min_length_str))
        else:
            LogHeader(fmt_piece)

    chunks = fmt.split("{")
    fmt_pieces = [chunk.split("}")[0] for chunk in chunks]
    return [wrap_header(fmt_piece) for fmt_piece in fmt_pieces]


def headers_to_fmt(*headers: LogHeader, padding: tuple = (1, 1), sep: _SepStyle = "|"):
    if len(headers) == 1:
        return headers[0].formatter

    seps = [
        " " * padding[0] + sep + " " * padding[1]
        if header.visible and n_header.visible
        else ""
        for header, n_header in zip(headers[:-1], headers[1:])
    ]
    return "".join(
        [header.formatter + sep for header, sep in zip(headers, seps + [""])]
    )


DEFAULT_HEADERS = [
    LogHeader("asctime", title="TIME", min_length=23),
    LogHeader("app_name", title="APPLICATION", min_length=15),
    LogHeader("levelname", title="LEVEL", min_length=8),
    LogHeader("message"),
]

DEFAULT_COLOR_HEADERS = (
    [LogHeader("ansi_color", title="", visible=False)]
    + DEFAULT_HEADERS
    + [LogHeader("reset_color", title="", visible=False)]
)


class _HeaderFormatter(Formatter):
    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: _FormatStyle = "{",
        validate: bool = True,
        *,
        defaults: Mapping[str, Any] | None = None,
        header_sep: _SepStyle = "|",
        headers: List[LogHeader] = DEFAULT_HEADERS,
    ) -> None:
        if fmt is None and headers is not None:
            fmt = headers_to_fmt(*headers, padding=(1, 1), sep=header_sep)

        elif fmt is not None:
            headers = retrieve_headers_from_fmt(fmt)

        self._header = fmt.format(
            **{header.variable_name: header.title for header in headers}
        )
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)

    @property
    def header(self):
        return self._header


BeamlimeFormatter = partial(_HeaderFormatter, headers=DEFAULT_HEADERS)
BeamlimeAnsiColorFormatter = partial(_HeaderFormatter, headers=DEFAULT_COLOR_HEADERS)
