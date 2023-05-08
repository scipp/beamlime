# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import logging

from .. import __name__ as beamlime_name
from .handlers import BeamlimeFileHandler
from .loggers import BeamlimeLogger


def _create_log_file(parent_dir: str, prefix: str = "beamlime") -> str:
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


def _create_log_root_dir(dir_path: str) -> None:
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


def initialize_file_handler(config: dict, logger: logging.Logger = None) -> None:
    if logger is None:
        logger = get_logger()

    # Find the location where log files will be collected.
    general_config = config["general"]
    if "log-dir" in general_config and general_config["log-dir"] is not None:
        log_dir = general_config["log-dir"]
    else:
        from ..config.preset_options import DEFAULT_LOG_DIR

        log_dir = DEFAULT_LOG_DIR
        logger.info(
            "Path to the directory for log files is not specified. "
            "Trying to use default logging directory..."
        )

    _create_log_root_dir(log_dir)

    log_path = _create_log_file(parent_dir=log_dir)
    logger.info(f"Start collecting logs into {log_path}")

    file_handler = BeamlimeFileHandler(log_path, header=True)
    logger.addHandler(file_handler)


def _start_new_logger(name: str, logger_class: logging.Logger) -> None:
    """
    Save the original logger class to a temporary variable and restore the logger class
    after getting or instantiating the logger with specific logger class.
    """
    _original_logger = logging.getLoggerClass()
    logging.setLoggerClass(logger_class)
    _ = logging.getLogger(name)
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
        _start_new_logger(name, logger_class)
    return logging.getLogger(name)
