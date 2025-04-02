# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Generic, Protocol, TypeVar

T = TypeVar('T')
Tin = TypeVar('Tin')
Tout = TypeVar('Tout')


class StreamKind(str, Enum):
    __slots__ = ()
    UNKNOWN = "unknown"
    MONITOR = "monitor"
    MONITOR_COUNTS = "monitor_counts"
    MONITOR_EVENTS = "monitor_events"
    DETECTOR_EVENTS = "detector_events"
    LOG = "log"
    BEAMLIME_CONFIG = "beamlime_config"
    BEAMLIME_DATA = "beamlime_data"


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamKey:
    kind: StreamKind = StreamKind.UNKNOWN
    source_name: str


CONFIG_MESSAGE_KEY = StreamKey(kind=StreamKind.BEAMLIME_CONFIG, source_name='')


@dataclass(frozen=True, slots=True, kw_only=True)
class Message(Generic[T]):
    timestamp: int
    key: StreamKey
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
            messages: A list of messages to publish.
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
