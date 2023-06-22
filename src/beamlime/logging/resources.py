# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# See comments in ``logging.Logger._src_file``
# for explanation why we didn't use __file__
import logging
import time
from contextlib import contextmanager
from threading import RLock
from typing import Iterator, NewType

from ..empty_binders import empty_log_factory

TimeStamp = NewType("TimeStamp", str)


@empty_log_factory.provider
def create_time_stamp() -> TimeStamp:
    """
    Creates a timestamp with the format, ``{cur_timestamp}--{h_time}``.
    ``htime`` is as an extra information only for the human users,
    and the ``cur_timestamp`` should be used by the applications or modules.
    """
    from datetime import datetime

    cur_timestamp = round(time.time())
    h_time = datetime.fromtimestamp(cur_timestamp).strftime("%Y-%m-%d-%H-%M-%S")
    return TimeStamp(f"{cur_timestamp}--{h_time}")


LogFilePrefix = NewType("LogFilePrefix", str)
DefaultPrefix = LogFilePrefix("beamlime")

LogFileExtension = NewType("LogFileExtension", str)
DefaultFileExtension = LogFileExtension("log")

LogDirectoryPath = NewType("LogDirectoryPath", str)

DirectoryCreated = NewType("DirectoryCreated", bool)


@empty_log_factory.provider
def initialize_log_dir(dir_path: LogDirectoryPath) -> DirectoryCreated:
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

    return True


LogFileName = NewType("LogFileName", str)


@empty_log_factory.provider
def create_log_file_name(
    time_stamp: TimeStamp,
    prefix: LogFilePrefix = DefaultPrefix,
    suffix: LogFileExtension = DefaultFileExtension,
) -> LogFileName:
    """Combine prefix, timestamp and suffix into a new file name."""
    return LogFileName(f"{prefix}--{time_stamp}.{suffix}")


FileHandlerBasePath = NewType("FileHandlerBasePath", str)


@empty_log_factory.provider
def create_log_file_path(
    directory_ready: DirectoryCreated = True,
    *,
    parent_dir: LogDirectoryPath,
    file_name: LogFileName,
) -> FileHandlerBasePath:
    """
    Create a log file path joining ``parent_dir`` and ``file_name``,
    check if the file name already exists and returns the file name.
    It will wait 0.001 second if the file name exists.

    """
    from os.path import join

    return FileHandlerBasePath(join(parent_dir, file_name))


@contextmanager
def hold_logging() -> Iterator[RLock]:
    from . import _lock

    _lock.acquire() if _lock else ...
    try:
        yield _lock
    finally:
        _lock.release() if _lock else ...


def cleanup_file_handlers(logger: logging.Logger):
    """Find file handlers that are connectected to
    non-existing files and remove them from the logger."""
    from os.path import exists

    f_hdlrs = [
        hdlr for hdlr in logger.handlers if isinstance(hdlr, logging.FileHandler)
    ]
    _messy_handlers = [hdlr for hdlr in f_hdlrs if not exists(hdlr.baseFilename)]
    for hdlr in _messy_handlers:
        with hold_logging():
            logger.removeHandler(hdlr)
