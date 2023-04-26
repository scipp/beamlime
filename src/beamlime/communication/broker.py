# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from multiprocessing.queues import Queue as MQueue
from queue import Empty, Full
from queue import Queue as SQueue
from typing import Any, Literal, Union

from confluent_kafka import KafkaException

from ..config.preset_options import MIN_WAIT_INTERVAL
from ..core.schedulers import async_timeout
from .queue_handlers import KafkaConsumer, KafkaProducer

channel_constructors = {
    "SQUEUE": SQueue,  # TODO: Replace to beamlime queue haneler
    "MQUEUE": MQueue,  # TODO: Replace to beamlime queue handler
    "KAFKA-CONSUMER": KafkaConsumer,
    "KAFKA-PRODUCER": KafkaProducer  # TODO: Add kafka queue handler
    # TODO: make a queue or object that is just keeping the latest result.
}


class CommunicationBroker:
    def __init__(self, channel_list: list, subscription_list: list) -> None:
        self.in_subscriptions = dict()
        self.out_subscriptions = dict()
        self.channels = dict()
        for ch in channel_list:
            ch_name = ch["name"]
            if ch["type"] == "SQUEUE":
                self.channels[ch_name] = SQueue()  # TODO: Update this to handler class
            elif ch["type"].startswith("KAFKA"):
                config = {
                    ch_opt_key: ch_opt
                    for ch_opt_key, ch_opt in ch.items()
                    if ch_opt_key not in ["name", "type"]
                }
                self.channels[ch_name] = channel_constructors[ch["type"]](**config)
        # TODO: Replace () to (ch["options"])
        for subscription in subscription_list:
            app_name = subscription["app-name"]
            input_channels = subscription.get("input-channels") or ()
            self.in_subscriptions[app_name] = {
                ch["name"]: self.channels[ch["name"]] for ch in input_channels
            }
            output_channels = subscription.get("output-channels") or ()
            self.out_subscriptions[app_name] = {
                ch["name"]: self.channels[ch["name"]] for ch in output_channels
            }

    def get_subscription_map(self, direction: Literal["in", "out"]) -> dict:
        if direction == "in":
            return self.in_subscriptions
        elif direction == "out":
            return self.out_subscriptions

    def get_defualt_channel_name(
        self, app_name: str, direction: Literal["in", "out"]
    ) -> str:
        subscription_map = self.get_subscription_map(direction)
        if len(subscription_map[app_name]) == 1:
            for channel_name in subscription_map[app_name]:
                return channel_name
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
    ) -> Union[SQueue, MQueue]:
        subscription_map = self.get_subscription_map(direction)
        channel_name = channel or self.get_defualt_channel_name(app_name, direction)
        try:
            _queue = subscription_map[app_name][channel_name]
            if not isinstance(_queue, SQueue):
                raise ValueError("``get`` method was called from wrong channel!")
        except KeyError:
            raise KeyError(f"This channel is not subscribed by {app_name}")
        return _queue

    def get_in_channel(
        self, app_name: str, channel: Union[str, int, None]
    ) -> Union[SQueue, MQueue]:
        return self.get_channel(app_name, channel, direction="in")

    def get_out_channel(
        self, app_name: str, channel: Union[str, int]
    ) -> Union[SQueue, MQueue]:
        return self.get_channel(app_name, channel, direction="out")

    @async_timeout(Empty)
    async def _get(
        self,
        *args,
        _queue: Union[SQueue, MQueue],
        timeout: int,
        wait_interval: int,
        **kwargs,
    ):
        return _queue.get(*args, timeout=wait_interval, **kwargs)

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
            return await self._get(
                *args,
                _queue=self.get_in_channel(app_name, channel),
                timeout=timeout,
                wait_interval=wait_interval,
                **kwargs,
            )
        except TimeoutError:
            return None

    @async_timeout(Full)
    async def _put(
        self,
        data: Any,
        *args,
        _queue: Union[SQueue, MQueue],
        timeout: int,
        wait_interval: int,
        **kwargs,
    ) -> bool:
        _queue.put(data, *args, timeout=wait_interval, **kwargs)
        return True

    async def put(
        self,
        data,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
        **kwargs,
    ) -> Any:
        try:
            return await self._put(
                data,
                *args,
                _queue=self.get_out_channel(app_name, channel),
                timeout=timeout,
                wait_interval=wait_interval,
                **kwargs,
            )
        except TimeoutError:
            return False

    @async_timeout(Exception)  # TODO: Replace it to kafka specific error
    async def _poll(
        self,
        *args,
        _consumer: KafkaConsumer,
        timeout: int,
        wait_interval: int,
        **kwargs,
    ):
        return _consumer.poll(*args, timeout=wait_interval, **kwargs)

    async def poll(
        self,
        *args,
        app_name: str,
        channel: KafkaConsumer,
        timeout: float,
        wait_interval: float,
        **kwargs,
    ) -> Any:
        try:
            return await self._poll(
                *args,
                _consumer=self.get_in_channel(app_name, channel),
                timeout=timeout,
                wait_interval=wait_interval,
                **kwargs,
            )
        except TimeoutError:
            return None

    @async_timeout(KafkaException)
    async def _produce(
        self,
        data: Any,
        *args,
        _producer: KafkaProducer,
        timeout: int,
        wait_interval: int,
        **kwargs,
    ) -> bool:
        _producer.produce(data, *args, timeout=wait_interval, **kwargs)
        return True

    async def produce(
        self,
        data,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
        **kwargs,
    ) -> Any:
        try:
            return await self._produce(
                data,
                *args,
                _producer=self.get_out_channel(app_name, channel),
                timeout=timeout,
                wait_interval=wait_interval,
                **kwargs,
            )
        except TimeoutError:
            return False
