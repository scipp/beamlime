# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from typing import Any, Literal, Union

from ..config.preset_options import MIN_WAIT_INTERVAL
from .queue_handlers import (
    BullettinBoard,
    KafkaConsumer,
    KafkaProducer,
    MultiProcessQueue,
    SingleProcessQueue,
)

channel_constructors = {
    "SQUEUE": SingleProcessQueue,
    "MQUEUE": MultiProcessQueue,
    "KAFKA-CONSUMER": KafkaConsumer,
    "KAFKA-PRODUCER": KafkaProducer,
    "BULLETIN-BOARD": BullettinBoard,
}


class CommunicationBroker:
    def __init__(self, channel_list: list, subscription_list: list) -> None:
        self.channels = dict()
        for ch in channel_list:
            queue_handler = channel_constructors[ch["type"]]
            self.channels[ch["name"]] = queue_handler(**ch.get("options", {}))

        self.subscriptions = dict()
        for subscription in subscription_list:
            app_name = subscription["app-name"]
            input_channels = subscription.get("input-channels") or ()
            output_channels = subscription.get("output-channels") or ()
            self.subscriptions[app_name] = {
                "in": {ch["name"]: self.channels[ch["name"]] for ch in input_channels},
                "out": {
                    ch["name"]: self.channels[ch["name"]] for ch in output_channels
                },
            }

    def get_default_channel_name(
        self, app_name: str, direction: Literal["in", "out"]
    ) -> str:
        app_subscriptions = self.subscriptions[app_name][direction]
        for ch_name in app_subscriptions:
            return ch_name
        else:
            raise ValueError(
                "Application has multiple input channel, "
                "it should provide channel name to get/put data."
            )

    def get_channel(
        self,
        app_name: str,
        channel: Union[str, int, None],
        /,
        direction: Literal["in", "out"],
    ) -> Union[
        SingleProcessQueue,
        MultiProcessQueue,
        BullettinBoard,
        KafkaConsumer,
        KafkaProducer,
    ]:
        channel_name = channel or self.get_default_channel_name(app_name, direction)
        try:
            return self.subscriptions[app_name][direction][channel_name]
        except KeyError:
            raise KeyError(f"This channel is not subscribed by {app_name}")

    async def get(
        self,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
        **kwargs,
    ) -> Any:
        try:
            _channel = self.get_channel(app_name, channel, direction="in")
            return await _channel.get(
                *args, timeout=timeout, wait_interval=wait_interval, **kwargs
            )
        except TimeoutError:
            return None

    async def put(
        self,
        data,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
        **kwargs,
    ) -> bool:
        try:
            _channel = self.get_channel(app_name, channel, direction="out")
            await _channel.put(
                data, *args, timeout=timeout, wait_interval=wait_interval, **kwargs
            )
            return True
        except TimeoutError:
            return False

    async def consume(
        self,
        *args,
        app_name: str,
        channel: KafkaConsumer,
        timeout: float,
        wait_interval: float,
        **kwargs,
    ) -> Any:
        try:
            _consumer = self.get_channel(app_name, channel, direction="in")
            return await _consumer.consume(
                *args,
                timeout=timeout,
                wait_interval=wait_interval,
                **kwargs,
            )
        except TimeoutError:
            return None

    async def produce(
        self,
        data,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
        **kwargs,
    ) -> bool:
        try:
            _producer = self.get_channel(app_name, channel, direction="out")
            await _producer.produce(
                data,
                *args,
                timeout=timeout,
                wait_interval=wait_interval,
                **kwargs,
            )
            return True
        except TimeoutError:
            return False
