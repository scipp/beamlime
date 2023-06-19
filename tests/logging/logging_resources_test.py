# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from beamlime.constructors import Container, constant_provider, local_providers

from .contexts import local_loggers


@patch("beamlime.logging.resources.time")
def test_time_stamp(_time):
    import time
    from datetime import datetime

    _time = MagicMock(time)
    cur_time = time.time()
    cur_timestamp = round(cur_time)
    _time.return_value = cur_time
    with local_providers():
        from beamlime.logging.resources import TimeStamp, create_time_stamp

        h_time = datetime.fromtimestamp(cur_timestamp).strftime("%Y-%m-%d-%H-%M-%S")
        assert create_time_stamp() == TimeStamp(f"{cur_timestamp}--{h_time}")


def test_create_log_file_name():
    """Test helper context test."""
    with local_providers():
        from beamlime.logging.resources import (
            LogFileExtension,
            LogFilePrefix,
            TimeStamp,
            create_log_file_name,
        )

        file_prefix = LogFilePrefix("beanline")
        timestamp = TimeStamp("rightnow")
        file_extension = LogFileExtension("leaf")
        assert (
            create_log_file_name(
                time_stamp=timestamp, prefix=file_prefix, suffix=file_extension
            )
            == "beanline--rightnow.leaf"
        )


def test_log_file_name_provider():
    """Test helper context test."""
    with local_providers():
        from beamlime.logging.resources import (
            LogFileExtension,
            LogFileName,
            LogFilePrefix,
            TimeStamp,
        )

        file_prefix = LogFilePrefix("beanline")
        timestamp = TimeStamp("rightnow")
        file_extension = LogFileExtension("leaf")

        with constant_provider(LogFilePrefix, file_prefix):
            with constant_provider(TimeStamp, timestamp):
                with constant_provider(LogFileExtension, file_extension):
                    assert Container[LogFileName] == "beanline--rightnow.leaf"

        with constant_provider(TimeStamp, timestamp):
            assert Container[LogFileName] == "beamlime--rightnow.log"


def test_create_log_file_path():
    """Test helper context test."""
    with local_providers():
        from beamlime.logging.resources import (
            LogDirectoryPath,
            LogFileName,
            create_log_file_path,
        )

        log_dir = LogDirectoryPath("tmp")
        log_file = LogFileName("tmp.log")
        expected_path = Path(log_dir) / Path(log_file)
        assert (
            Path(create_log_file_path(parent_dir=log_dir, file_name=log_file))
            == expected_path
        )


def test_create_log_file_path_provider():
    """Test helper context test."""
    with local_providers():
        from beamlime.logging.resources import (
            FileHandlerBasePath,
            LogDirectoryPath,
            LogFileName,
        )

        log_dir = LogDirectoryPath("tmp")
        log_file = LogFileName("tmp.log")
        expected_path = Path(log_dir) / Path(log_file)
        with constant_provider(LogDirectoryPath, log_dir):
            with constant_provider(LogFileName, log_file):
                assert Path(Container[FileHandlerBasePath]) == expected_path


def test_create_log_file_path_file_exists_raises():
    """Test helper context test."""
    from inspect import getsourcefile

    this_file_path = Path(getsourcefile(test_create_log_file_name))
    existing_dir = this_file_path.parent
    existing_filename = this_file_path.parts[-1]
    with local_providers():
        from beamlime.logging.resources import create_log_file_path

        with pytest.raises(FileExistsError):
            create_log_file_path(parent_dir=existing_dir, file_name=existing_filename)


def test_cleanup_file_handlers(tmp_path: Path):
    """Test helper context test."""
    with local_loggers():
        import os
        from logging import Logger

        from beamlime.logging.handlers import BeamlimeFileHandler
        from beamlime.logging.resources import cleanup_file_handlers

        tmp_file = tmp_path / "tmp.log"
        logger = Logger("_")
        hdlr = BeamlimeFileHandler(tmp_file)
        logger.addHandler(hdlr)
        assert hdlr in logger.handlers
        os.remove(tmp_file)
        cleanup_file_handlers(logger)
        assert hdlr not in logger.handlers