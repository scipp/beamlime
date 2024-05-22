# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Literal, NewType, Optional

from ..empty_providers import log_providers
from .handlers import BeamlimeFileHandler, BeamlimeStreamHandler

LogLevels = Literal["DEBUG", "INFO", "WARNING", "ERROR"]
BeamlimeLogger = NewType("BeamlimeLogger", logging.Logger)


@log_providers.provider
def get_logger(
    stream_handler: Optional[BeamlimeStreamHandler] = None, verbose: bool = True
) -> BeamlimeLogger:
    """
    Retrieves a beamlime logger and add ``stream_handler`` if ``verbose``.

    """
    from beamlime import __name__ as beamlime_name

    logger = logging.getLogger(beamlime_name)
    if verbose and not logger.handlers:
        logger.addHandler(stream_handler or BeamlimeStreamHandler())

    return BeamlimeLogger(logger)


ScippWidgetFlag = NewType("ScippWidgetFlag", bool)
DefaultWidgetFlag = ScippWidgetFlag(True)
ScippLogger = NewType("ScippLogger", logging.Logger)


@log_providers.provider
def get_scipp_logger(
    log_level: Optional[LogLevels] = None,
    widget: ScippWidgetFlag = DefaultWidgetFlag,
) -> ScippLogger:
    from scipp.logging import get_logger, get_widget_handler

    scipp_logger = get_logger()

    if widget and not (widget_handler := get_widget_handler()):  # pragma: no cover
        # scipp widget is only available in the jupyter notebook.
        from scipp.logging import WidgetHandler, make_widget_handler

        widget_handler = make_widget_handler()
        if log_level and isinstance(widget_handler, WidgetHandler):
            scipp_logger.addHandler(widget_handler)
            widget_handler.setLevel(log_level)

    elif log_level:
        scipp_logger.setLevel(log_level)

    return ScippLogger(scipp_logger)


FileHandlerConfigured = NewType("FileHandlerConfigured", bool)


@log_providers.provider
def initialize_file_handler(
    logger: BeamlimeLogger, file_handler: BeamlimeFileHandler
) -> FileHandlerConfigured:
    """
    Add a file handler to the ``logger``.
    It creates the directory at ``dir_path`` if it does not exist.
    File name is automatically created.

    To adjust file name with different prefix or extension,
    see ``create_log_file_path``.
    """
    from .resources import check_file_handlers

    check_file_handlers(logger)
    _hdlrs = [hdlr for hdlr in logger.handlers if isinstance(hdlr, BeamlimeFileHandler)]
    file_paths = [hdlr.baseFilename for hdlr in _hdlrs]
    if any(file_paths):
        logger.warning(
            "Attempt to add a new file handler to the current logger, "
            "but a file handler is already configured. "
            "Log file base path is %s"
            "Aborting without changing the logger...",
            file_paths,
        )
        return FileHandlerConfigured(True)

    logger.info("Start collecting logs into %s", file_handler.baseFilename)
    logger.addHandler(file_handler)

    return FileHandlerConfigured(True)
