import json
import os
from unittest.mock import MagicMock

import pytest
from scippnexus import Group

from beamlime.applications.daemons import FakeListener, RunStart

pytest_plugins = ('pytest_asyncio',)


def test_fake_listener_can_be_initialized_from_path():
    path = os.path.join(os.path.dirname(__file__), 'ymir.json')
    FakeListener.from_file(path)


@pytest.fixture
def nexus_structure():
    with open(os.path.join(os.path.dirname(__file__), 'ymir.json')) as f:
        return json.load(f)


class MockMessenger(list):
    async def send_message_async(self, m):
        self.append(m)

    def StopRouting(self, *args, **kwargs):
        return 'StopRouting'


async def test_fake_listener_produces_start_event(
    nexus_structure, mock_logger: MagicMock
):
    listener = FakeListener(nexus_structure)

    messenger = MockMessenger()
    listener.logger = mock_logger

    async for message in listener.run():
        await messenger.send_message_async(message)
        break

    assert len(messenger) > 0
    assert isinstance(messenger[0], RunStart)
    assert isinstance(messenger[0].args[0], Group)


async def test_fake_listener_produces_stop_routing(
    nexus_structure, mock_logger: MagicMock
):
    from beamlime.applications.base import Application

    listener = FakeListener(nexus_structure)

    messenger = MockMessenger()
    listener.logger = mock_logger

    async for message in listener.run():
        await messenger.send_message_async(message)

    assert len(messenger) > 1
    assert isinstance(messenger[-1], Application.Stop)
