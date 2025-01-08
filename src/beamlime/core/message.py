# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

T = TypeVar('T')
Tin = TypeVar('Tin')
Tout = TypeVar('Tout')


@dataclass(frozen=True, slots=True, kw_only=True)
class MessageKey:
    topic: str
    source_name: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Message(Generic[T]):
    timestamp: int
    key: MessageKey
    value: T

    def __lt__(self, other: Message[T]) -> bool:
        return self.timestamp < other.timestamp


class MessageSource(Protocol, Generic[Tin]):
    def get_messages(self) -> list[Message[Tin]]:
        pass

    def close(self) -> None:
        pass


class MessageSink(Protocol, Generic[Tout]):
    def publish_messages(self, messages: list[Message[Tout]]) -> None:
        """
        Publish messages to the producer.

        Args:
            messages: A dictionary of messages to publish, where the key is the
                topic and the value is the message.
        """


def compact_messages(messages: list[Message[T]]) -> list[Message[T]]:
    """
    Compact messages by removing outdates ones, keeping only the latest for each key.
    """
    latest = {}
    for msg in sorted(messages, reverse=True):  # Newest first
        if msg.key not in latest:
            latest[msg.key] = msg
    return sorted(latest.values())
