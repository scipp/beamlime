# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Literal, NewType, TypeVar

from ..empty_providers import empty_log_providers
from .handlers import BeamlimeFileHandler, BeamlimeStreamHandler

LOG_LEVELS = Literal["DEBUG", "INFO", "WARNING", "ERROR"]
LogLevel = TypeVar(
    "LogLevel",
    Literal["DEBUG"],
    Literal["INFO"],
    Literal["WARNING"],
    Literal["ERROR"],
    None,
)
BeamlimeLogger = NewType("BeamlimeLogger", logging.Logger)


@empty_log_providers.provider
def get_logger(verbose: bool = True) -> BeamlimeLogger:
    """
    Retrieves a logger by ``name``.
    If the logger does not exist, instantiate a new ``logger_class``.

    Parameters
    ----------
    name:
        The name of the logger.
        Default is ``beamlime``.

    logger_class:
        Class of the logger to instantiate with.
        Default is ``beamlime.logging.logger.BeamlimeLogger``.

    """
    from beamlime import __name__ as beamlime_name

    logger = logging.getLogger(beamlime_name)
    if verbose and not logger.handlers:
        logger.addHandler(BeamlimeStreamHandler())

    return BeamlimeLogger(logger)


ScippWidgetFlag = NewType("ScippWidgetFlag", bool)
DefaultWidgetFlag = ScippWidgetFlag(True)
ScippLogger = NewType("ScippLogger", logging.Logger)


@empty_log_providers.provider
def get_scipp_logger(
    log_level: LogLevel = "INFO",
    widget: ScippWidgetFlag = DefaultWidgetFlag,
) -> ScippLogger:
    import scipp as sc

    scipp_logger = sc.get_logger()

    if widget and not (
        widget_handler := sc.logging.get_widget_handler()
    ):  # pragma: no cover
        # scipp widget is only available in the jupyter notebook.
        from scipp.logging import WidgetHandler

        widget_handler = sc.logging.make_widget_handler()
        if log_level and isinstance(widget_handler, WidgetHandler):
            scipp_logger.addHandler(widget_handler)
            widget_handler.setLevel(log_level)

    elif log_level:
        scipp_logger.setLevel(log_level)

    return ScippLogger(scipp_logger)


FileHandlerConfigured = NewType("FileHandlerConfigured", bool)


@empty_log_providers.provider
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
    from .resources import cleanup_file_handlers

    cleanup_file_handlers(logger)
    _hdlrs = [hdlr for hdlr in logger.handlers if isinstance(hdlr, BeamlimeFileHandler)]
    file_paths = [hdlr.baseFilename for hdlr in _hdlrs]
    if any((file_paths)):
        logger.warning(
            "Attempt to add a new file handler to the current logger, "
            "but a file handler is already configured. "
            f"Log file base path is {file_paths}"
            "Aborting without changing the logger..."
        )
        return FileHandlerConfigured(True)

    file_handler.initialize()
    logger.info(f"Start collecting logs into {file_handler.baseFilename}")
    logger.addHandler(file_handler)

    return FileHandlerConfigured(True)
