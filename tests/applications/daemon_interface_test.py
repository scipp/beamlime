# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from logging import Logger

import pytest

from beamlime.applications.interfaces import CoroutineInterface

from .controller_test import LocalController


class Daemon(CoroutineInterface):
    def __init__(self) -> None:
        self._command = LocalController()
        self.timeout = 0
        self.logger = Logger("Daemon")

    def run(self):
        ...


@pytest.mark.asyncio
async def test_coroutine_can_start_false():
    app = Daemon()
    app.timeout = 0
    app._command = LocalController()
    assert not app._command.started
    assert not await app.can_start(wait_on_true=False)


@pytest.mark.asyncio
async def test_coroutine_can_start():
    app = Daemon()
    app._command.start()
    assert app._command.started
    assert await app.can_start(wait_on_true=False)


@pytest.mark.asyncio
async def test_coroutine_running():
    app = Daemon()
    app._command.start()
    assert app._command.started and not app._command.paused
    assert await app.running(wait_on_true=False)


@pytest.mark.asyncio
async def test_coroutine_running_pause_false():
    app = Daemon()
    app._command.start()
    app._command.pause()
    assert app._command.started and app._command.paused
    assert not await app.running(wait_on_true=False)


@pytest.mark.asyncio
async def test_coroutine_running_stop_false():
    app = Daemon()
    app._command.start()
    app._command.stop()
    assert not app._command.started and not app._command.paused
    assert not await app.running(wait_on_true=False)
