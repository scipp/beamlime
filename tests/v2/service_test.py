# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from beamlime.v2.core.handler import Handler, HandlerRegistry
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
    service = Service(
        config=config,
        consumer=consumer,
        producer=producer,
        handler_registry=handler_registry,
    )
    service.start()
    assert service.is_running
    service.stop()
    assert not service.is_running
