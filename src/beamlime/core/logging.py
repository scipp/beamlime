# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import logging


def _create_log_file(
    parent_dir: str, prefix: str = "beamlime", header: str = ""
) -> str:
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
            parent_dir, f"{prefix}--{cur_timestamp}--{h_time}" f"--{uuid.uuid4()}.log"
        )
    else:
        new_path = time_based

    with open(new_path, "w") as file:
        file.write(header)
        file.write("=" * 56 + "LOG START" + "=" * 56 + "\n")

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


class BeamlimeFileHandler(logging.FileHandler):
    def setFormatter(self, fmt: logging.Formatter | None) -> None:
        return super().setFormatter(fmt)


def initialize_file_handler(config: dict, logger: logging.Logger = None) -> None:
    if logger is None:
        logger = get_logger()

    # Find the location where log files will be collected.
    general_config = config["general"]
    if "log-dir" in general_config:
        log_dir = general_config["log-dir"]
    else:
        from ..config.preset_options import DEFAULT_LOG_DIR

        log_dir = DEFAULT_LOG_DIR
        logger.info(
            "Path to the directory for log files is not specified. "
            "Trying to use default logging directory..."
        )

    _create_log_root_dir(log_dir)
    # TODO: Add application name column like `|APPLICATION: %(app_name)-15s`
    formatter = "{asctime:25} |{levelname:8} |{message}\n"
    header = formatter.format(asctime="TIME", levelname="LEVEL", message="MESSAGE")

    log_path = _create_log_file(parent_dir=log_dir, header=header)
    logger.info(f"Start collecting logs into {log_path}")

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(logging.Formatter(formatter, style="{"))

    logger.addHandler(file_handler)


def get_logger() -> logging.Logger:
    from beamlime import __name__ as beamlime_name

    return logging.getLogger(beamlime_name)
