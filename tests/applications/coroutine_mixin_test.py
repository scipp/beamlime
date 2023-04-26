from logging import DEBUG, getLevelName

from pytest import CaptureFixture

from beamlime.applications.interfaces import BeamlimeApplicationInterface
from beamlime.logging import get_logger as get_bm_logger
from beamlime.logging.handlers import BeamlimeStreamHandler


def test_daemon_del(capsys: CaptureFixture[str]):
    DYING_MESSAGE = {
        "paused": None,
        "stopped": None,
        "last-word": "Application instance killed.",
    }

    class DummyDaemonApp(BeamlimeApplicationInterface):
        async def _run(self) -> None:
            ...

        def __del__(self):
            self.stop()
            DYING_MESSAGE["started"] = self._started
            DYING_MESSAGE["paused"] = self._paused
            self.debug(DYING_MESSAGE["last-word"])

    logger = get_bm_logger()
    if not logger.handlers:
        logger.addHandler(BeamlimeStreamHandler())
    logger.setLevel(DEBUG)

    app_name = "Dummy Daemon Application"
    app = DummyDaemonApp(app_name)
    app.start()
    del app

    log_output = capsys.readouterr()[-1]
    assert (
        f"{app_name:15} | {getLevelName(DEBUG):8} | "
        f"{DYING_MESSAGE['last-word']}" in log_output
    )
    assert not DYING_MESSAGE["started"]
    assert DYING_MESSAGE["paused"]
