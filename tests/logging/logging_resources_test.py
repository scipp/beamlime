# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from pathlib import Path
from unittest.mock import patch

from beamlime import Factory
from beamlime.logging.resources import (
    FileHandlerBasePath,
    LogDirectoryPath,
    LogFileExtension,
    LogFileName,
    LogFilePrefix,
    UTCTimeTag,
    check_file_handlers,
    create_log_file_name,
    create_log_file_path,
)


@patch("beamlime.logging.resources.datetime")
def test_time_tag(_datetime):
    from datetime import datetime as real_dt
    from datetime import timezone

    from beamlime.logging.resources import create_utc_time_without_microsecond

    real_now = real_dt.now(tz=timezone.utc).replace(microsecond=0)
    _datetime.now.return_value = real_now

    assert create_utc_time_without_microsecond() == UTCTimeTag(real_now.isoformat())


def test_create_log_file_name():
    """Test helper context test."""
    file_prefix = LogFilePrefix("beanline")
    time_tag = UTCTimeTag("rightnow")
    file_extension = LogFileExtension("leaf")
    assert create_log_file_name(
        prefix=file_prefix, time_tag=time_tag, extension=file_extension
    ) == Path("beanline_rightnow.leaf")


def test_create_log_file_invalid_prefix_raises():
    """Test helper context test."""
    import pytest

    file_prefix = LogFilePrefix("bean_line")
    time_tag = UTCTimeTag("rightnow")
    file_extension = LogFileExtension("leaf")
    with pytest.raises(ValueError):
        create_log_file_name(
            prefix=file_prefix, time_tag=time_tag, extension=file_extension
        )


def test_log_file_name_provider(local_logger: bool, default_factory: Factory):
    """Test helper context test."""
    assert local_logger
    with default_factory.local_factory() as factory:
        file_prefix = LogFilePrefix("beanline")
        timestamp = UTCTimeTag("rightnow")
        file_extension = LogFileExtension("leaf")

        with factory.constant_provider(LogFilePrefix, file_prefix):
            with factory.constant_provider(UTCTimeTag, timestamp):
                with factory.constant_provider(LogFileExtension, file_extension):
                    assert factory[LogFileName] == Path("beanline_rightnow.leaf")

        with factory.constant_provider(UTCTimeTag, timestamp):
            assert factory[LogFileName] == Path("beamlime_rightnow.log")


def test_create_log_file_path(tmp_path: Path):
    """Test helper context test."""
    log_dir = LogDirectoryPath(tmp_path / Path("tmp"))
    log_file = LogFileName(Path("tmp.log"))
    expected_path = log_dir / log_file
    assert (
        create_log_file_path(
            directory_ready=True, parent_dir=log_dir, file_name=log_file
        )
        == expected_path
    )


def test_create_log_file_directory_not_ready_raises(tmp_path: Path):
    """Test helper context test."""
    import pytest

    log_dir = LogDirectoryPath(tmp_path / Path("tmp"))
    log_file = LogFileName(Path("tmp.log"))
    with pytest.raises(ValueError):
        create_log_file_path(
            directory_ready=False, parent_dir=log_dir, file_name=log_file
        )


def test_create_log_file_path_provider(
    local_logger: bool, tmp_path: Path, default_factory: Factory
):
    """Test helper context test."""

    assert local_logger
    with default_factory.local_factory() as factory:
        log_dir = LogDirectoryPath(tmp_path / Path("tmp"))
        log_file = LogFileName(Path("tmp.log"))
        expected_path = log_dir / log_file
        with factory.constant_provider(LogDirectoryPath, log_dir):
            with factory.constant_provider(LogFileName, log_file):
                assert factory[FileHandlerBasePath] == expected_path


def test_check_file_handlers(tmp_path: Path):
    """Test helper context test."""
    import os
    from logging import Logger

    import pytest

    from beamlime.logging.handlers import BeamlimeFileHandler

    tmp_file = FileHandlerBasePath(tmp_path / "tmp.log")
    logger = Logger("_")
    hdlr = BeamlimeFileHandler(tmp_file)
    logger.addHandler(hdlr)
    assert hdlr in logger.handlers
    os.remove(tmp_file)
    with pytest.raises(RuntimeError):
        check_file_handlers(logger)
