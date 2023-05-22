# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import logging
from contextlib import contextmanager
from typing import AnyStr, Literal, Optional, Union

from .. import __name__ as beamlime_name
from .handlers import BeamlimeFileHandler
from .loggers import BeamlimeLogger

LOG_LEVELS = Literal["DEBUG", "INFO", "WARNING", "ERROR"]


def _create_log_file(parent_dir: AnyStr, prefix: AnyStr = "beamlime") -> str:
    """
    Create a new log file into the ``parent_dir`` and returns the path.
    The log file name is created based on the current ``timestamp``.
    It will add uuid if the ``time`` is not enough to create a unique name.

    Time based new log file name format
    -----------------------------------
    The name of the log file is formatted as
    ``{prefix}--{timestamp}--{human-readable-time}.log``.

    1. ``prefix`` is given as an argument.
    2. ``timestamp`` is retrieved from ``time.time()``.
    3. ``human-readable-time`` is the human readable time from the ``timestamp``.

    ``human-readable-time`` is as an extra information only for the ``human users``,
    and the ``timestamp`` should be used by the applications or modules.

    """

    import os
    import time
    from datetime import datetime

    cur_timestamp = round(time.time())
    h_time = datetime.fromtimestamp(cur_timestamp).strftime("%Y-%m-%d-%H-%M-%S")
    time_based = os.path.join(parent_dir, f"{prefix}--{cur_timestamp}--{h_time}.log")

    if os.path.exists(time_based):
        import uuid

        new_path = os.path.join(
            parent_dir,
            f"{prefix}--{cur_timestamp}--{h_time}" f"--{uuid.uuid4().hex}.log",
        )
    else:
        new_path = time_based

    return new_path


def _create_log_root_dir(dir_path: AnyStr) -> None:
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


def initialize_file_handler(
    log_dir: Optional[AnyStr] = None, logger: logging.Logger = None
) -> None:
    if logger is None:
        logger = get_logger()

    # Find the location where log files will be collected.
    if log_dir is None:
        from ..config.preset_options import LogDirectory

        log_dir = LogDirectory.default
        logger.info(
            "Path to the directory for log files is not specified. "
            "Trying to use default logging directory..."
        )

    from .handlers import FileHandler

    if any((issubclass(hdlr, FileHandler) for hdlr in logger.handlers)):
        logger.warning(
            "Attempt to add a new file handler to the current logger, "
            "but a file handler is already configured. "
            "Aborting without changing the logger..."
        )
        return

    _create_log_root_dir(log_dir)

    log_path = _create_log_file(parent_dir=log_dir)
    logger.info(f"Start collecting logs into {log_path}")

    file_handler = BeamlimeFileHandler(log_path, header=True)
    logger.addHandler(file_handler)


@contextmanager
def stash_original_logger_class() -> logging.Logger:
    """
    Save the original logger class to a temporary variable and restore the logger class.
    """
    _original_logger = logging.getLoggerClass()
    try:
        yield _original_logger
    finally:
        logging.setLoggerClass(_original_logger)


def get_logger(
    name: str = beamlime_name, logger_class: logging.Logger = BeamlimeLogger
) -> logging.Logger:
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
    if name not in logging.root.manager.loggerDict:
        with stash_original_logger_class():
            logging.setLoggerClass(logger_class)
            return logging.getLogger(name)
    else:
        return logging.getLogger(name)


def safe_get_logger(
    logger: Union[logging.Logger, None] = None, verbose: bool = True
) -> logging.Logger:
    """
    Do nothing if the logger si a subclass of ``logging.Logger``.

    If logger is not a subclass of ``logging.Logger``,
    create and return a new beamlime logger and if ``verbose``, add a stream handler.
    """
    if isinstance(logger, logging.Logger):
        return logger

    from ..logging import get_logger

    logger = get_logger()
    if verbose and not logger.hasHandlers():
        from ..logging.handlers import BeamlimeStreamHandler

        logger.addHandler(BeamlimeStreamHandler())

    return logger


def get_scipp_logger(
    log_level: Optional[LOG_LEVELS] = None, widget: bool = True
) -> logging.Logger:
    import scipp as sc

    scipp_logger = sc.get_logger()

    if widget and not (widget_handler := sc.logging.get_widget_handler()):
        scipp_logger.addHandler((widget_handler := sc.logging.make_widget_handler()))

    if widget and log_level:
        widget_handler.setLevel(log_level)
    elif log_level:
        scipp_logger.setLevel(log_level)

    return scipp_logger
