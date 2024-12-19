# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from typing import Generic, TypeVar

from .core import Message

T = TypeVar('T')


class FakeMessageSource(Generic[T]):
    """
    A fake message source that returns messages from memory for testing purposes.
    """

    def __init__(self, messages: list[list[Message[T]]]) -> None:
        self._messages = messages
        self._index = 0

    def get_messages(self) -> list[Message[T]]:
        messages = (
            self._messages[self._index] if self._index < len(self._messages) else []
        )
        self._index += 1
        return messages

    def close(self) -> None:
        pass


class FakeMessageSink(Generic[T]):
    """
    A fake message sink that stores messages in memory for testing purposes.
    """

    def __init__(self) -> None:
        self.messages = []

    def publish_messages(self, messages: list[Message[T]]) -> None:
        self.messages.extend(messages)
