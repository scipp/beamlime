# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

from functools import partial
from queue import Empty, Full
from typing import Any, Callable, Optional, Type, Union

from ..config.preset_options import MIN_WAIT_INTERVAL
from ..core.schedulers import async_retry
from .interfaces import (
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


async def _read_or_write_wrapper(
    method: Callable,
    timeout: float,
    wait_interval: float,
    /,
    exception: Type,
    return_on_fail: Optional[bool] = None,
) -> Optional[bool]:
    _method = async_retry(
        exception, max_trials=int(timeout / wait_interval), interval=wait_interval
    )(method)
    try:
        return await _method()
    except exception:
        return return_on_fail


_read_wrapper = partial(_read_or_write_wrapper, exception=Empty)
_write_wrapper = partial(_read_or_write_wrapper, exception=Full, return_on_fail=False)


class CommunicationBroker:
    def __init__(self, channel_list: list, subscription_list: list) -> None:
        self.channels = dict()
        for ch in channel_list:
            queue_handler = channel_constructors[ch["type"]]
            self.channels[ch["name"]] = queue_handler(**ch.get("options", {}))

        self.subscriptions = dict()
        for subscription in subscription_list:
            app_name = subscription["app-name"]
            channels = subscription.get("channels") or ()
            self.subscriptions[app_name] = {
                ch["name"]: self.channels[ch["name"]] for ch in channels
            }

    def __str__(self) -> str:
        return (
            "Communication channel instances: \n"
            + "".join(
                f"\t- {ch_name}: {ch_inst.__class__.__name__}\n"
                for ch_name, ch_inst in self.channels.items()
            )
            + "Subscriptions: \n"
            + "".join(
                f"\t- {app_name}: {subs}\n"
                for app_name, subs in self.subscriptions.items()
            )
        )

    def get_default_channel_name(self, app_name: str) -> str:
        app_subscriptions = self.subscriptions[app_name]
        for ch_name in app_subscriptions:
            return ch_name
        else:
            raise ValueError(
                "Application has multiple input channel, "
                "it should provide channel name to get/put data."
            )

    def get_channel(
        self, app_name: str, channel: Union[str, int, None]
    ) -> Union[
        SingleProcessQueue,
        MultiProcessQueue,
        BullettinBoard,
        KafkaConsumer,
        KafkaProducer,
    ]:
        channel_name = channel or self.get_default_channel_name(app_name)
        try:
            return self.subscriptions[app_name][channel_name]
        except KeyError:
            raise KeyError(f"This channel is not subscribed by {app_name}")

    async def read(
        self,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
    ) -> Any:
        _board = self.get_channel(app_name, channel)

        async def _read() -> Any:
            return await _board.read()

        return await _read_wrapper(_read, timeout, wait_interval)

    async def post(
        self,
        data,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
    ) -> bool:
        _board = self.get_channel(app_name, channel)

        async def _post() -> Any:
            await _board.post(data)
            return True

        return await _write_wrapper(_post, timeout, wait_interval)

    async def get(
        self,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
        **kwargs,
    ) -> Any:
        _queue = self.get_channel(app_name, channel)

        async def _get() -> Any:
            return await _queue.get(*args, **kwargs)

        return await _read_wrapper(_get, timeout, wait_interval)

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
        _queue = self.get_channel(app_name, channel)

        async def _put() -> Any:
            await _queue.put(data, *args, **kwargs)
            return True

        return await _write_wrapper(_put, timeout, wait_interval)

    async def consume(
        self,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
        chunk_size: int = 1,
        **kwargs,
    ) -> Any:
        _consumer = self.get_channel(app_name, channel)

        async def _consume() -> Any:
            return await _consumer.consume(*args, chunk_size=chunk_size, **kwargs)

        return await _read_wrapper(_consume, timeout, wait_interval)

    async def produce(
        self,
        data: Any,
        *args,
        app_name: str,
        channel: Union[tuple, str, int],
        timeout: float = 0,
        wait_interval: float = MIN_WAIT_INTERVAL,
        topic: str,
        key: str,
        **kwargs,
    ) -> bool:
        _producer = self.get_channel(app_name, channel)

        async def _produce() -> Any:
            await _producer.produce(topic, *args, key=key, value=data, **kwargs)
            return True

        return await _write_wrapper(_produce, timeout, wait_interval)
