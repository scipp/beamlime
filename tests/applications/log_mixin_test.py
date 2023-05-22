import logging
from logging import DEBUG, ERROR, INFO, WARNING, getLevelName
from pathlib import Path

import pytest
from pytest import CaptureFixture

from beamlime.logging import get_logger as get_bm_logger
from beamlime.logging.handlers import BeamlimeFileHandler, BeamlimeStreamHandler
from tests.test_helper import DummyApp


def init_logger(logger: logging.Logger, level: int, *handlers: logging.Handler):
    for hdlr in logger.handlers:
        logger.removeHandler(hdlr)
    for hdlr in handlers:
        logger.addHandler(hdlr)
    logger.setLevel(level)


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
    level: int, log_method, msg_suffix: str, capsys: CaptureFixture[str]
):
    app_name = "Dummy Application"
    init_logger(get_bm_logger(), level, BeamlimeStreamHandler())
    app = DummyApp(name=app_name)
    msg = f"Some information needed to be {msg_suffix} with"
    getattr(app, log_method)(msg)
    log_output = capsys.readouterr()[-1]
    for expected_field in (app_name, getLevelName(level), msg):
        assert expected_field in log_output


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
    app_name = "Dummy Application"
    tmp_log_path = tmp_path / "tmp.log"
    init_logger(get_bm_logger(), level, BeamlimeFileHandler(tmp_log_path))
    app = DummyApp(name=app_name)
    msg = f"Some information needed to be {msg_suffix} with"
    getattr(app, log_method)(msg)
    log_output = tmp_log_path.read_text()
    for expected_field in (app_name, getLevelName(level), msg):
        assert expected_field in log_output


@pytest.mark.parametrize("log_method", ("debug", "info", "warning"))
def test_app_logging_disabled_level_std(log_method: str, capsys: CaptureFixture[str]):
    app_name = "Dummy Application"
    logger = get_bm_logger()
    if not logger.handlers:
        logger.addHandler(BeamlimeStreamHandler())
    logger.setLevel(ERROR)
    app = DummyApp(name=app_name)
    getattr(app, log_method)("Message that will not be shown")
    log_output = capsys.readouterr()[-1]
    assert log_output == ""
