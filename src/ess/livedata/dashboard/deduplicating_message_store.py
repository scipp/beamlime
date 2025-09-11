# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import threading
from typing import Generic, TypeVar

K = TypeVar('K')
V = TypeVar('V')


class DeduplicatingMessageStore(Generic[K, V]):
    """
    Thread-safe message store that deduplicates by key.

    This is meant to be used with
    :py:class:`~ess.livedata.dashboard.kafka_bridge.KafkaBridge`. The idea is that it
    prevents flooding Kafka with repeated messages for the same key. Note that this is
    not full protection on its own, i.e., the Kafka bridge still needs to ensure it is
    not retrieving messages too frequently. Furthermore, we have enabled throttling in
    Panel.
    """

    def __init__(self):
        self._messages: dict[K, V] = {}
        self._lock = threading.Lock()

    def put(self, key: K, value: V) -> None:
        """Store a message, overwriting any existing message with the same key."""
        with self._lock:
            self._messages[key] = value

    def pop_all(self) -> dict[K, V]:
        """Pop all messages atomically."""
        with self._lock:
            messages = self._messages.copy()
            self._messages.clear()
            return messages
