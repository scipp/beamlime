from logging import Logger

import pytest

from beamlime.applications.mixins import (
    ApplicationNotPausedException,
    ApplicationNotStartedException,
    ApplicationPausedException,
    ApplicationStartedException,
    FlagControlMixin,
    LogMixin,
)


class DummyApp(LogMixin, FlagControlMixin):
    app_name = "Dummy application for testing flag control."
    _logger = Logger(name="Beamlime Test")


def test_start():
    app = DummyApp()
    assert not app._started and app._paused
    app.start()
    assert app._started and not app._paused


def test_start_twice():
    app = DummyApp()
    app.start()
    assert app._started and not app._paused
    with pytest.raises(ApplicationStartedException):
        app.start()
        assert app._started and not app._paused


def test_start_twice_paused():
    app = DummyApp()
    app.start()
    app.pause()
    with pytest.raises(ApplicationStartedException):
        app.start()
        assert app._started and app._paused


def test_pause():
    app = DummyApp()
    app.start()
    app.pause()
    assert app._started and app._paused


def test_pause_twice():
    app = DummyApp()
    app.start()
    app.pause()
    with pytest.raises(ApplicationPausedException):
        app.pause()
        assert app._started and app._paused


def test_pause_twice_resumed():
    app = DummyApp()
    app.start()
    app.pause()
    app.resume()
    assert app._started and not app._paused
    app.pause()
    assert app._started and app._paused


def test_resume_paused():
    app = DummyApp()
    app.start()
    app.pause()
    app.resume()
    assert app._started and not app._paused


def test_resume_not_paused():
    app = DummyApp()
    app.start()
    with pytest.raises(ApplicationNotPausedException):
        app.resume()
        assert app._started and not app._paused


def test_resume_not_started():
    app = DummyApp()
    with pytest.raises(ApplicationNotStartedException):
        app.resume()
        assert not app._started and not app._paused


def test_resume_paused_twice():
    app = DummyApp()
    app.start()
    app.pause()
    app.resume()
    with pytest.raises(ApplicationNotPausedException):
        app.resume()
        assert app._started and not app._paused


def test_stop_not_started():
    app = DummyApp()
    with pytest.raises(ApplicationNotStartedException):
        app.stop()
        assert not app._started and app._paused


def test_stop_not_paused():
    app = DummyApp()
    app.start()
    assert app._started and not app._paused
    app.stop()
    assert not app._started and app._paused


def test_stop_paused():
    app = DummyApp()
    app.start()
    app.pause()
    assert app._started and app._paused
    app.stop()
    assert not app._started and app._paused


def test_stop_twice():
    app = DummyApp()
    app.start()
    app.stop()
    with pytest.raises(ApplicationNotStartedException):
        app.stop()
        assert not app._started and app._paused
