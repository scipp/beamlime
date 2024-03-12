# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# Tests builtin handlers of ``beamlime.applications.Application``.
from typing import AsyncGenerator, Optional
from unittest.mock import MagicMock

from beamlime.applications.base import (
    Application,
    DaemonInterface,
    MessageBase,
    MessageProtocol,
    MessageRouter,
)


class SimpleDaemon(DaemonInterface):
    class SimpleMessage(MessageBase):
        ...

    def __init__(self) -> None:
        self.test_flag = False

    def test_handler(self) -> None:
        self.test_flag = True

    async def run(self) -> AsyncGenerator[Optional[MessageProtocol], None]:
        yield Application.RegisterHandler(
            kwargs={'event_tp': self.SimpleMessage, 'handler': self.test_handler}
        )  # Register the handler
        yield self.SimpleMessage()  # Try triggering the handler
        yield Application.Stop(args=(self.__class__,))  # Stop the application


def test_dynamic_handler_registration(mock_logger: MagicMock) -> None:
    # Build the application
    message_router = MessageRouter()
    message_router.logger = mock_logger
    daemon = SimpleDaemon()
    app = Application(mock_logger, message_router)

    # Check the clean state
    assert not daemon.test_flag
    app.register_daemon(daemon)
    assert daemon.SimpleMessage not in app.message_router.handlers

    # Run the daemon and check the state
    app.run()
    assert daemon.test_handler in app.message_router.handlers[daemon.SimpleMessage]
    assert daemon.test_flag
