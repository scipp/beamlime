# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import DEBUG, ERROR, INFO, WARNING, Logger, getLevelName
from pathlib import Path

import pytest
from pytest import CaptureFixture, LogCaptureFixture

from beamlime.logging import (
    FileHandlerConfigured,
    LogDirectoryPath,
    LogFileName,
    get_logger,
)
from beamlime.logging.handlers import BeamlimeFileHandler, FileHandler

from .contexts import local_logger_factory


def test_local_loggers():
    """Test helper context test."""
    with local_logger_factory():
        from beamlime.logging import get_logger

        with local_logger_factory():
            logger = get_logger()
            assert get_logger() is logger

        assert not get_logger() is logger


def test_logmixin_protocol():
    with local_logger_factory():
        from beamlime.logging.mixins import BeamlimeLoggingProtocol

        from .dummy_app import LogMixinDummy

        assert isinstance(LogMixinDummy(Logger("_")), BeamlimeLoggingProtocol)


@pytest.mark.parametrize(
    ["level", "log_method", "msg_suffix"],
    [
        (DEBUG, "debug", "debugged"),
        (INFO, "info", "informed"),
        (WARNING, "warning", "warned"),
        (ERROR, "error", "raised error"),
    ],
)
def test_app_logging_stream(
    level: int,
    log_method,
    msg_suffix: str,
    caplog: LogCaptureFixture,
    capsys: CaptureFixture,
):
    with local_logger_factory():
        from beamlime.logging import get_logger

        from .dummy_app import LogMixinDummy

        bm_logger = get_logger(verbose=True)
        bm_logger.setLevel(level)
        app = LogMixinDummy(bm_logger)

        msg = f"Some information needed to be {msg_suffix} with"
        getattr(app, log_method)(msg)

        log_record = caplog.records[-1]
        assert log_record.levelno == level
        assert log_record.levelname == getLevelName(level)
        std_out = capsys.readouterr()[-1]
        for expected_field in (str(app.__class__.__qualname__), msg):
            assert expected_field in log_record.message
            assert expected_field in std_out


def test_file_handler_configuration(tmp_path: Path):
    tmp_log_dir = tmp_path / "tmp"
    tmp_log_filename = "tmp.log"
    tmp_log_path = tmp_log_dir / tmp_log_filename

    with local_logger_factory() as factory:
        logger = get_logger(verbose=False)
        factory.pop(LogDirectoryPath)
        factory.pop(LogFileName)
        factory.register(LogDirectoryPath, lambda: tmp_log_dir)
        factory.register(LogFileName, lambda: tmp_log_filename)

        # Should not have any file handlers set.
        hdlrs = logger.handlers
        assert not any([hdlr for hdlr in hdlrs if isinstance(hdlr, FileHandler)])

        # Set a file handler.
        assert factory[FileHandlerConfigured]
        _f_hdlrs = [hdlr for hdlr in hdlrs if isinstance(hdlr, FileHandler)]
        assert len(_f_hdlrs) == 1

        # Should not add another file handler.
        assert factory[FileHandlerConfigured]
        _f_hdlrs = [hdlr for hdlr in hdlrs if isinstance(hdlr, FileHandler)]
        assert len(_f_hdlrs) == 1

        # Check file path.
        f_hdlr = [hdlr for hdlr in logger.handlers if isinstance(hdlr, FileHandler)][0]
        assert Path(f_hdlr.baseFilename) == tmp_log_path


def test_file_handler_configuration_existing_dir_raises():
    from inspect import getsourcefile

    this_file_path = Path(getsourcefile(test_file_handler_configuration))
    with local_logger_factory() as factory:
        with factory.constant_provider(LogDirectoryPath, this_file_path):
            with pytest.raises(FileExistsError):
                factory[FileHandlerConfigured]


@pytest.mark.parametrize(
    ["level", "log_method", "msg_suffix"],
    [
        (DEBUG, "debug", "debugged"),
        (INFO, "info", "informed"),
        (WARNING, "warning", "warned"),
        (ERROR, "error", "raised error"),
    ],
)
def test_app_logging_file(level: int, log_method, msg_suffix: str, tmp_path: Path):
    tmp_log_path = tmp_path / "tmp.log"

    with local_logger_factory():
        from .dummy_app import LogMixinDummy

        file_handler = BeamlimeFileHandler(tmp_log_path)
        file_handler.initialize()
        logger = Logger("tmp")
        logger.addHandler(file_handler)
        logger.setLevel(level)
        msg = f"Some information needed to be {msg_suffix} with"
        app = LogMixinDummy(logger=logger)
        getattr(app, log_method)(msg)
        file_handler.close()

    log_output = tmp_log_path.read_text()
    for expected_field in (str(app.__class__.__qualname__), getLevelName(level), msg):
        assert expected_field in log_output
