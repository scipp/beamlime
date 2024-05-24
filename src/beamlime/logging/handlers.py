# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from logging import FileHandler

from rich.logging import RichHandler

from ..empty_providers import log_providers
from .formatters import BeamlimeFileFormatter, BeamlimeStreamHighlighter
from .resources import FileHandlerBasePath


class BeamlimeFileHandler(FileHandler):
    def __del__(self) -> None:
        self.close()


@log_providers.provider
def provide_beamlime_filehandler(
    filename: FileHandlerBasePath, formatter: BeamlimeFileFormatter
) -> BeamlimeFileHandler:
    """Returns a file handler."""
    handler = BeamlimeFileHandler(filename)
    handler.formatter = formatter
    return handler


class BeamlimeStreamHandler(RichHandler): ...


@log_providers.provider
def provide_beamlime_streamhandler(
    highlighter: BeamlimeStreamHighlighter,
) -> BeamlimeStreamHandler:
    """Returns a ``RichHandler`` with ``BeamlimeStreamHighlighter``."""

    return BeamlimeStreamHandler(highlighter=highlighter)
