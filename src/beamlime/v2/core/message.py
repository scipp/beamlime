# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

Tin = TypeVar('Tin')
Tout = TypeVar('Tout')


@dataclass(frozen=True, slots=True, kw_only=True)
class MessageKey:
    topic: str
    source_name: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Message(Generic[Tin]):
    timestamp: int
    key: MessageKey
    value: Tin

    def __lt__(self, other: Message[Tin]) -> bool:
        return self.timestamp < other.timestamp


class MessageSource(Protocol, Generic[Tin]):
    def get_messages(self) -> list[Message[Tin]]:
        pass


class MessageSink(Protocol, Generic[Tout]):
    def publish_messages(self, messages: list[Message[Tout]]) -> None:
        """
        Publish messages to the producer.

        Args:
            messages: A dictionary of messages to publish, where the key is the
                topic and the value is the message.
        """
