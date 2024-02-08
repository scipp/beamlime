# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import DEBUG, ERROR, INFO, WARNING, Logger, getLevelName
from pathlib import Path

import pytest
from pytest import LogCaptureFixture

from beamlime.logging import (
    FileHandlerConfigured,
    LogDirectoryPath,
    LogFileName,
    get_logger,
)
from beamlime.logging.handlers import BeamlimeFileHandler, FileHandler


def test_local_loggers():
    """Test helper context test.

    ``local_logger_factory`` is used by the fixture ``isolated_logger``.

    """
    from tests.logging.contexts import local_logger

    with local_logger():
        from beamlime.logging import get_logger

        with local_logger():
            logger: Logger = get_logger()
            assert get_logger() is logger

        assert not get_logger() is logger


def test_logmixin_protocol(local_logger: bool):
    from beamlime import LoggingProtocol
    from tests.logging.dummy_app import LogMixinDummy

    assert local_logger
    assert isinstance(LogMixinDummy(Logger("_")), LoggingProtocol)


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
    local_logger: bool,
):
    from beamlime.logging import get_logger
    from tests.logging.dummy_app import LogMixinDummy

    assert local_logger
    bm_logger: Logger = get_logger(verbose=True)
    bm_logger.setLevel(level)
    app = LogMixinDummy(bm_logger)

    msg = f"Some information needed to be {msg_suffix} with"
    getattr(app, log_method)(msg)

    log_record = caplog.records[-1]
    assert log_record.levelno == level
    assert log_record.levelname == getLevelName(level)
    for expected_field in (str(app.__class__.__qualname__), msg):
        assert expected_field in log_record.message


def test_file_handler_configuration(tmp_path: Path, local_logger: bool):
    from beamlime.constructors import ProviderGroup
    from beamlime.ready_factory import log_factory

    assert local_logger
    tmp_log_dir = tmp_path / "tmp"
    tmp_log_filename = "tmp.log"
    tmp_log_path = tmp_log_dir / tmp_log_filename

    tmp_log_providers = ProviderGroup()
    tmp_log_providers[LogDirectoryPath] = lambda: tmp_log_dir
    tmp_log_providers[LogFileName] = lambda: tmp_log_filename

    with log_factory.local_factory(tmp_log_providers) as factory:
        logger: Logger = get_logger(verbose=False)
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


def test_file_handler_configuration_existing_dir_raises(local_logger: bool):
    from inspect import getsourcefile

    from beamlime.ready_factory import log_factory

    assert local_logger
    if src_file := getsourcefile(test_file_handler_configuration):
        this_file_path = Path(src_file)
        with log_factory.local_factory() as factory:
            with factory.constant_provider(LogDirectoryPath, this_file_path):
                with pytest.raises(FileExistsError):
                    factory[FileHandlerConfigured]
    else:
        raise RuntimeError("Could not retrieve the path to this source for testing.")


@pytest.mark.parametrize(
    ["level", "log_method", "msg_suffix"],
    [
        (DEBUG, "debug", "debugged"),
        (INFO, "info", "informed"),
        (WARNING, "warning", "warned"),
        (ERROR, "error", "raised error"),
    ],
)
def test_app_logging_file(
    level: int, log_method, msg_suffix: str, tmp_path: Path, local_logger: bool
):
    from beamlime.logging.formatters import (
        provide_default_headers,
        provide_file_formatter,
    )
    from beamlime.logging.resources import FileHandlerBasePath
    from tests.logging.dummy_app import LogMixinDummy

    assert local_logger

    tmp_log_path = tmp_path / "tmp.log"

    file_handler = BeamlimeFileHandler(FileHandlerBasePath(tmp_log_path))
    file_handler.formatter = provide_file_formatter(provide_default_headers())
    logger = Logger("tmp")
    logger.addHandler(file_handler)
    logger.setLevel(level)
    msg = f"Some information needed to be {msg_suffix} with"
    app = LogMixinDummy(logger=logger)
    getattr(app, log_method)(msg)

    log_output = tmp_log_path.read_text()
    for expected_field in (str(app.__class__.__qualname__), getLevelName(level), msg):
        assert expected_field in log_output
