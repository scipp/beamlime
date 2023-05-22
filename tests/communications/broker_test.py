# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from asyncio.runners import run as async_run

import pytest

from beamlime.communication.broker import CommunicationBroker

_dummy_app_name = "Dummy Application"


def _subscription(app_name: str = _dummy_app_name, channels: list[str] = None) -> dict:
    return {"app_name": app_name, "channels": channels or []}


_q_ch_1_config = {
    "name": "queue-type-channel-1",
    "constructor": "beamlime.communication.interfaces.SingleProcessQueue",
}
_q_ch_2_config = {
    "name": "queue-type-channel-2",
    "constructor": "beamlime.communication.interfaces.SingleProcessQueue",
}


def test_put_get_from_default_channel():
    broker = CommunicationBroker(
        channel_list=([_q_ch_1_config]),
        subscription_list=[_subscription(channels=[_q_ch_1_config["name"]])],
    )
    assert async_run(broker.put((msg := "sample message"), app_name=_dummy_app_name))
    assert async_run(broker.get(app_name=_dummy_app_name)) == msg


@pytest.mark.parametrize(
    ["msg", "ch_name"],
    [
        ("sample-message-1", _q_ch_1_config["name"]),
        ("sample-message-2", _q_ch_2_config["name"]),
    ],
)
def test_put_get_multiple_channels(msg, ch_name: str):
    broker = CommunicationBroker(
        channel_list=(
            [
                _q_ch_1_config,
                _q_ch_2_config,
            ]
        ),
        subscription_list=[
            _subscription(channels=[_q_ch_1_config["name"], _q_ch_2_config["name"]])
        ],
    )

    assert async_run(broker.put(msg, app_name=_dummy_app_name, channel=ch_name))
    assert async_run(broker.get(app_name=_dummy_app_name, channel=ch_name)) == msg


def test_put_get_multiple_channels_non_default_raises():
    broker = CommunicationBroker(
        channel_list=(
            [
                _q_ch_1_config,
                _q_ch_2_config,
            ]
        ),
        subscription_list=[
            _subscription(channels=[_q_ch_1_config["name"], _q_ch_2_config["name"]])
        ],
    )
    with pytest.raises(ValueError):
        async_run(broker.put("", app_name=_dummy_app_name))


def test_wrong_channel_name():
    broker = CommunicationBroker(
        channel_list=(
            [
                _q_ch_1_config,
                _q_ch_2_config,
            ]
        ),
        subscription_list=[_subscription(channels=[_q_ch_1_config["name"]])],
    )
    with pytest.raises(KeyError):
        async_run(
            broker.put("", app_name=_dummy_app_name, channel="queue-type-channel-2")
        )


def test_put_full():
    broker = CommunicationBroker(
        channel_list=([{**_q_ch_1_config, "specs": {"maxsize": 1}}]),
        subscription_list=[_subscription(channels=[_q_ch_1_config["name"]])],
    )
    assert async_run(broker.put("", app_name=_dummy_app_name))
    assert not async_run(broker.put("", app_name=_dummy_app_name))


def test_get_empty():
    broker = CommunicationBroker(
        channel_list=([_q_ch_1_config]),
        subscription_list=[_subscription(channels=[_q_ch_1_config["name"]])],
    )
    assert async_run(broker.get(app_name=_dummy_app_name)) is None


_b_ch_config = {
    "name": "board-type-channel",
    "constructor": "beamlime.communication.interfaces.BullettinBoard",
}


def test_post_read():
    broker = CommunicationBroker(
        channel_list=([_b_ch_config]),
        subscription_list=[_subscription(channels=[_b_ch_config["name"]])],
    )
    assert async_run(
        broker.post((msg := {"control": "start"}), app_name=_dummy_app_name)
    )
    assert async_run(broker.read(app_name=_dummy_app_name)) == msg


def test_post_full():
    broker = CommunicationBroker(
        channel_list=([{**_b_ch_config, "specs": {"maxsize": 1}}]),
        subscription_list=[_subscription(channels=[_b_ch_config["name"]])],
    )
    assert async_run(broker.post(({"control": "start"}), app_name=_dummy_app_name))
    assert not async_run(broker.post({"command": "start"}, app_name=_dummy_app_name))


def test_read_empty():
    broker = CommunicationBroker(
        channel_list=([_b_ch_config]),
        subscription_list=[_subscription(channels=[_b_ch_config["name"]])],
    )
    assert async_run(broker.read(app_name=_dummy_app_name)) is None


def test_description():
    broker = CommunicationBroker(
        channel_list=([_q_ch_1_config]),
        subscription_list=[_subscription(channels=[_q_ch_1_config["name"]])],
    )
    assert _q_ch_1_config["name"] in (desc := str(broker))
    assert _dummy_app_name in desc
