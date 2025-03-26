# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import TypeVar

from beamlime.core.handler import (
    CommonHandlerFactory,
    Config,
    Handler,
    HandlerFactory,
    HandlerRegistry,
)
from beamlime.core.message import Message, MessageKey
from beamlime.core.processor import StreamProcessor
from beamlime.fakes import FakeMessageSink, FakeMessageSource

T = TypeVar('T')


class ValueToStringHandler(Handler[T, str]):
    def handle(self, messages: list[Message[T]]) -> list[Message[str]]:
        return [
            Message(
                timestamp=message.timestamp, key=message.key, value=str(message.value)
            )
            for message in messages
        ]


def test_consumes_and_produces_messages() -> None:
    config = {}
    source = FakeMessageSource(
        messages=[
            [],
            [Message(timestamp=0, key='topic', value=111)],
            [
                Message(timestamp=0, key='topic', value=222),
                Message(timestamp=0, key='topic', value=333),
            ],
        ]
    )
    sink = FakeMessageSink()
    factory = CommonHandlerFactory(config=config, handler_cls=ValueToStringHandler)
    registry = HandlerRegistry(factory=factory)
    processor = StreamProcessor(source=source, sink=sink, handler_registry=registry)
    processor.process()
    assert len(sink.messages) == 0
    processor.process()
    assert len(sink.messages) == 1
    assert sink.messages[0].value == '111'
    processor.process()
    assert len(sink.messages) == 3
    assert sink.messages[1].value == '222'
    assert sink.messages[2].value == '333'
    processor.process()
    assert len(sink.messages) == 3


class SelectiveHandlerFactory(HandlerFactory[T, str]):
    """Factory that returns a handler only for certain keys."""

    def __init__(
        self,
        *,
        config: Config,
        handler_cls: type[Handler[T, str]],
        allowed_keys: list[str],
    ):
        self._config = config
        self._handler_cls = handler_cls
        self._allowed_keys = allowed_keys

    def make_handler(self, key: MessageKey) -> Handler[T, str] | None:
        if key in self._allowed_keys:
            return self._handler_cls(logger=None, config=self._config)
        return None


def test_registry_counts_non_none_handlers():
    factory = SelectiveHandlerFactory(
        config={}, handler_cls=ValueToStringHandler, allowed_keys=['allowed']
    )
    registry = HandlerRegistry(factory=factory)

    # Get a handler for an allowed key
    handler1 = registry.get('allowed')
    assert handler1 is not None

    # Get handler for a disallowed key
    handler2 = registry.get('disallowed')
    assert handler2 is None

    # Check that len() only counts non-None handlers
    assert len(registry) == 1


def test_processor_skips_keys_without_handlers():
    config = {}
    source = FakeMessageSource(
        messages=[
            [
                Message(timestamp=0, key='allowed', value=111),
                Message(timestamp=0, key='disallowed', value=222),
                Message(timestamp=0, key='allowed', value=333),
            ],
        ]
    )
    sink = FakeMessageSink()
    factory = SelectiveHandlerFactory(
        config=config, handler_cls=ValueToStringHandler, allowed_keys=['allowed']
    )
    registry = HandlerRegistry(factory=factory)
    processor = StreamProcessor(source=source, sink=sink, handler_registry=registry)

    processor.process()

    assert len(sink.messages) == 2
    assert sink.messages[0].value == '111'
    assert sink.messages[1].value == '333'
    # The message with key 'disallowed' should be skipped


def test_processor_with_mixed_handlers():
    # Test with a mix of allowed and disallowed keys over multiple process calls.
    config = {}
    source = FakeMessageSource(
        messages=[
            [Message(timestamp=0, key='allowed', value=111)],
            [Message(timestamp=0, key='disallowed', value=222)],
            [
                Message(timestamp=0, key='allowed', value=333),
                Message(timestamp=0, key='disallowed', value=444),
                Message(timestamp=0, key='allowed', value=555),
            ],
        ]
    )
    sink = FakeMessageSink()
    factory = SelectiveHandlerFactory(
        config=config, handler_cls=ValueToStringHandler, allowed_keys=['allowed']
    )
    registry = HandlerRegistry(factory=factory)
    processor = StreamProcessor(source=source, sink=sink, handler_registry=registry)

    # First batch: one allowed message
    processor.process()
    assert len(sink.messages) == 1
    assert sink.messages[0].value == '111'

    # Second batch: one disallowed message (should be skipped)
    processor.process()
    assert len(sink.messages) == 1  # Still just the first message

    # Third batch: mix of allowed and disallowed
    processor.process()
    assert len(sink.messages) == 3  # Only allowed messages processed
    assert sink.messages[1].value == '333'
    assert sink.messages[2].value == '555'
