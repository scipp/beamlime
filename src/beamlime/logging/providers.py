# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Literal, NewType, Optional

from ..constructors import Container, provider
from .resources import LogDirectoryPath

LOG_LEVELS = Literal["DEBUG", "INFO", "WARNING", "ERROR"]

BeamlimeLogger = NewType("BeamlimeLogger", logging.Logger)


@provider
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
        from .handlers import BeamlimeStreamHandler

        logger.addHandler(Container[BeamlimeStreamHandler])

    return BeamlimeLogger(logger)


ScippWidgetFlag = NewType("ScippWidgetFlag", bool)
DefaultWidgetFlag = ScippWidgetFlag(True)
ScippLogger = NewType("ScippLogger", logging.Logger)


@provider
def get_scipp_logger(
    log_level: Optional[LOG_LEVELS] = None,
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


def _create_log_root_dir(dir_path: LogDirectoryPath) -> None:
    """
    Create the directory if it does not exist.
    Raises an error if the desired path is an existing file.
    """
    import os

    if os.path.exists(dir_path) and not os.path.isdir(dir_path):
        raise FileExistsError(
            f"{dir_path} is a file. " "It should either not exist or be a directory."
        )
    elif not os.path.exists(dir_path):
        os.mkdir(dir_path)

    return None


FileHandlerConfigured = NewType("FileHandlerConfigured", bool)


@provider
def initialize_file_handler(
    logger: BeamlimeLogger, dir_path: LogDirectoryPath
) -> FileHandlerConfigured:
    """
    Add a file handler to the ``logger``.
    It creates the directory at ``dir_path`` if it does not exist.
    File name is automatically created.

    To adjust file name with different prefix or extension,
    see ``create_log_file_path``.
    """

    from ..constructors import Container, constant_provider
    from .handlers import BeamlimeFileHandler
    from .resources import cleanup_file_handlers

    cleanup_file_handlers(logger)
    if any(
        (
            file_paths := [
                hdlr.baseFilename
                for hdlr in logger.handlers
                if isinstance(hdlr, BeamlimeFileHandler)
            ]
        )
    ):
        logger.warning(
            "Attempt to add a new file handler to the current logger, "
            "but a file handler is already configured. "
            f"Log file base path is {file_paths}"
            "Aborting without changing the logger..."
        )
        return FileHandlerConfigured(True)

    with constant_provider(LogDirectoryPath, dir_path):
        _create_log_root_dir(dir_path)
        new_handler = Container[BeamlimeFileHandler]
        logger.info(f"Start collecting logs into {new_handler.baseFilename}")
        logger.addHandler(new_handler)

    return FileHandlerConfigured(True)
