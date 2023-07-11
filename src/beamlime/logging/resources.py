# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# See comments in ``logging.Logger._src_file``
# for explanation why we didn't use __file__
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import NewType

from ..empty_providers import log_providers

UTCTimeTag = NewType("UTCTimeTag", str)


@log_providers.provider
def create_utc_time_without_microsecond() -> UTCTimeTag:
    """
    Create a UTC time tag without microsecond.

    Notes
    -----
    ``scipp`` uses UTC time for time labels.
    Microsecond is dropped to keep the log file names readable.
    The time tag is intended to be use only for logging,
    therefore microsecond information is not necessary.
    """
    now = datetime.now(tz=timezone.utc).replace(microsecond=0)
    return UTCTimeTag(now.isoformat())


LogFilePrefix = NewType("LogFilePrefix", str)
DefaultPrefix = LogFilePrefix("beamlime")

LogFileExtension = NewType("LogFileExtension", str)
DefaultFileExtension = LogFileExtension("log")

LogDirectoryPath = NewType("LogDirectoryPath", Path)

DirectoryCreated = NewType("DirectoryCreated", bool)


@log_providers.provider
def initialize_log_dir(dir_path: LogDirectoryPath) -> DirectoryCreated:
    """
    Create the directory including parents if needed and returns True.
    Raises ``FileExistsError`` if ``dir_path`` is an existing file, not a directory.
    """
    dir_path.mkdir(parents=True, exist_ok=True)

    return DirectoryCreated(True)


LogFileName = NewType("LogFileName", Path)


def validate_log_file_prefix(prefix: LogFilePrefix) -> None:
    """Log file prefix should not contain any underscores."""
    if "_" in prefix:
        raise ValueError("Log file prefix should not contain any underscore(`_`).")


@log_providers.provider
def create_log_file_name(
    *,
    prefix: LogFilePrefix = DefaultPrefix,
    time_tag: UTCTimeTag,
    extension: LogFileExtension = DefaultFileExtension,
) -> LogFileName:
    """Combine prefix, time_tag and suffix into a new file name."""
    validate_log_file_prefix(prefix)
    return LogFileName(Path(f"{prefix}_{time_tag}.{extension}"))


FileHandlerBasePath = NewType("FileHandlerBasePath", Path)


@log_providers.provider
def create_log_file_path(
    *,
    directory_ready: DirectoryCreated,
    parent_dir: LogDirectoryPath,
    file_name: LogFileName,
) -> FileHandlerBasePath:
    """
    Create a log file path joining ``parent_dir`` and ``file_name``.

    Notes
    -----
    It does not check if the file already exists or not,
    therefore a file handler using this path might append logs into an existing file.
    This case will be rare since the log file name contains time tag down to seconds,
    and another file is not expected to be created within 1 second.
    """
    if directory_ready:
        return FileHandlerBasePath(parent_dir / file_name)
    else:
        raise ValueError("Directory should be ready first.")


def check_file_handlers(logger: logging.Logger) -> None:
    """
    Raises ``RuntimeError`` if any files attached to file handlers do not exist.

    Notes
    -----
    This function is intended to be used only during the initial setup of the daemon
    or for the integrated testing, which might create and delete a temporary log file.
    """
    from os.path import exists

    f_hdlrs = [
        hdlr for hdlr in logger.handlers if isinstance(hdlr, logging.FileHandler)
    ]
    missing = [
        file_name for hdlr in f_hdlrs if not exists((file_name := hdlr.baseFilename))
    ]
    if missing:
        raise RuntimeError("Files attached to the file handlers are missing.", missing)
