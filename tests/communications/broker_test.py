# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from asyncio.runners import run as async_run

import pytest

from beamlime.communication.broker import CommunicationBroker


def test_put_get_from_default_channel():
    broker = CommunicationBroker(
        channel_list=([{"name": "queue-type-channel", "type": "SQUEUE"}]),
        subscription_list=[
            {"app-name": "dummy application", "channels": ["queue-type-channel"]}
        ],
    )
    assert async_run(broker.put("sample message", app_name="dummy application"))
    assert async_run(broker.get(app_name="dummy application")) == "sample message"


@pytest.mark.parametrize(
    ["msg", "ch_name"],
    [
        ("sample-message-1", "queue-type-channel-1"),
        ("sample-message-2", "queue-type-channel-2"),
    ],
)
def test_put_get_multiple_channels(msg, ch_name: str):
    broker = CommunicationBroker(
        channel_list=(
            [
                {"name": "queue-type-channel-1", "type": "SQUEUE"},
                {"name": "queue-type-channel-2", "type": "SQUEUE"},
            ]
        ),
        subscription_list=[
            {
                "app-name": "dummy application",
                "channels": ["queue-type-channel-1", "queue-type-channel-2"],
            }
        ],
    )
    assert async_run(broker.put(msg, app_name="dummy application", channel=ch_name))
    assert async_run(broker.get(app_name="dummy application", channel=ch_name)) == msg


def test_put_get_multiple_channels_non_default_raises():
    broker = CommunicationBroker(
        channel_list=(
            [
                {"name": "queue-type-channel-1", "type": "SQUEUE"},
                {"name": "queue-type-channel-2", "type": "SQUEUE"},
            ]
        ),
        subscription_list=[
            {
                "app-name": "dummy application",
                "channels": ["queue-type-channel-1", "queue-type-channel-2"],
            }
        ],
    )
    with pytest.raises(ValueError):
        async_run(broker.put("", app_name="dummy application"))


def test_wrong_channel_name():
    broker = CommunicationBroker(
        channel_list=(
            [
                {"name": "queue-type-channel-1", "type": "SQUEUE"},
                {"name": "queue-type-channel-2", "type": "SQUEUE"},
            ]
        ),
        subscription_list=[
            {"app-name": "dummy application", "channels": ["queue-type-channel-1"]}
        ],
    )
    with pytest.raises(KeyError):
        async_run(
            broker.put("", app_name="dummy application", channel="queue-type-channel-2")
        )


def test_put_full():
    broker = CommunicationBroker(
        channel_list=(
            [
                {
                    "name": "queue-type-channel-1",
                    "type": "SQUEUE",
                    "options": {"maxsize": 1},
                }
            ]
        ),
        subscription_list=[
            {"app-name": "dummy application", "channels": ["queue-type-channel-1"]}
        ],
    )
    assert async_run(broker.put("", app_name="dummy application"))
    assert not async_run(broker.put("", app_name="dummy application"))


def test_get_empty():
    broker = CommunicationBroker(
        channel_list=([{"name": "queue-type-channel-1", "type": "SQUEUE"}]),
        subscription_list=[
            {"app-name": "dummy application", "channels": ["queue-type-channel-1"]}
        ],
    )
    assert async_run(broker.get(app_name="dummy application")) is None


def test_post_read():
    broker = CommunicationBroker(
        channel_list=([{"name": "board-type-channel", "type": "BULLETIN-BOARD"}]),
        subscription_list=[
            {"app-name": "dummy application", "channels": ["board-type-channel"]}
        ],
    )
    assert async_run(broker.post({"control": "start"}, app_name="dummy application"))
    assert async_run(broker.read(app_name="dummy application")) == {"control": "start"}


def test_post_full():
    broker = CommunicationBroker(
        channel_list=(
            [
                {
                    "name": "board-type-channel",
                    "type": "BULLETIN-BOARD",
                    "options": {"maxsize": 1},
                }
            ]
        ),
        subscription_list=[
            {"app-name": "dummy application", "channels": ["board-type-channel"]}
        ],
    )
    assert async_run(broker.post({"control": "start"}, app_name="dummy application"))
    assert not async_run(
        broker.post({"command": "start"}, app_name="dummy application")
    )


def test_read_empty():
    broker = CommunicationBroker(
        channel_list=([{"name": "board-type-channel", "type": "BULLETIN-BOARD"}]),
        subscription_list=[
            {"app-name": "dummy application", "channels": ["board-type-channel"]}
        ],
    )
    assert async_run(broker.read(app_name="dummy application")) is None


def test_description():
    broker = CommunicationBroker(
        channel_list=([{"name": "queue-type-channel", "type": "SQUEUE"}]),
        subscription_list=[
            {"app-name": "dummy application", "channels": ["queue-type-channel"]}
        ],
    )
    assert "queue-type-channel" in (desc := str(broker))
    assert "dummy application" in desc
