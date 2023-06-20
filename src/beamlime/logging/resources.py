# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# See comments in ``logging.Logger._src_file``
# for explanation why we didn't use __file__
import logging
import time
from contextlib import contextmanager
from threading import RLock
from typing import Iterator, NewType

from ..empty_binders import IncompleteLoggingBinder

TimeStamp = NewType("TimeStamp", str)


@IncompleteLoggingBinder.provider
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

LogFileName = NewType("LogFileName", str)


@IncompleteLoggingBinder.provider
def create_log_file_name(
    time_stamp: TimeStamp,
    prefix: LogFilePrefix = DefaultPrefix,
    suffix: LogFileExtension = DefaultFileExtension,
) -> LogFileName:
    """Combine prefix, timestamp and suffix into a new file name."""
    return LogFileName(f"{prefix}--{time_stamp}.{suffix}")


LogDirectoryPath = NewType("LogDirectoryPath", str)
FileHandlerBasePath = NewType("FileHandlerBasePath", str)


@IncompleteLoggingBinder.provider
def create_log_file_path(
    *, parent_dir: LogDirectoryPath, file_name: LogFileName
) -> FileHandlerBasePath:
    """
    Create a log file path joining ``parent_dir`` and ``file_name``,
    check if the file name already exists and returns the file name.
    It will wait 0.001 second if the file name exists.

    Examples
    --------
    To use different prefix or extension, try
    >>> from beamlime.constructors import Container, partial_provider, constant_provider
    >>> with constant_provider(LogDirectoryPath, './'):
    ...   with constant_provider(LogFileName, 'beanline.txt'):
    ...     Container[FileHandlerBasePath]
    './beanline.txt'

    """
    from os.path import exists, join

    if exists((file_path := join(parent_dir, file_name))):
        raise FileExistsError(f"{file_path} already exists.")

    return FileHandlerBasePath(file_path)


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
