# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from beamlime.v2.core.handler import Handler, HandlerRegistry, MessageHandlerLoop
from beamlime.v2.core.service import Service


class FakeConsumer:
    def get_messages(self):
        return []


class FakeProducer:
    def publish_messages(self, messages: dict[str, str]) -> None:
        pass


def test_create_start_stop_service() -> None:
    config = {}
    consumer = FakeConsumer()
    producer = FakeProducer()
    handler_registry = HandlerRegistry(config=config, handler_cls=Handler)
    loop = MessageHandlerLoop(
        config=config,
        consumer=consumer,
        producer=producer,
        handler_registry=handler_registry,
    )
    service = Service(handler_loop=loop)
    service.start()
    assert loop.is_running
    service.stop()
    assert not loop.is_running
