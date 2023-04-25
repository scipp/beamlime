from beamlime.applications.mixins import FlagControlMixin, LogMixin


class DummyApp(LogMixin, FlagControlMixin):
    ...


def test_start():
    app = DummyApp()
    assert not app._started and app._paused
    app.start()
    assert app._started and not app._paused


def test_start_twice():
    app = DummyApp()
    app.start()
    assert app._started and not app._paused
    # app.start()
    # assert app._started and not app._paused


def test_start_twice_paused():
    app = DummyApp()
    app.start()
    app.pause()
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
    app.resume()
    assert app._started and not app._paused


def test_resume_not_started():
    app = DummyApp()
    app.resume()
    assert app._started and not app._paused


def test_resume_paused_twice():
    app = DummyApp()
    app.start()
    app.pause()
    app.resume()
    app.resume()
    assert app._started and not app._paused


def test_stop_not_started():
    app = DummyApp()
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
    app.stop()
    assert not app._started and app._paused
