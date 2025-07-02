# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import threading
from typing import Generic, Protocol, TypeVar

K = TypeVar('K')
V = TypeVar('V')


class MessageTransport(Protocol, Generic[K, V]):
    """Protocol for transporting messages to/from external systems."""

    def send_messages(self, messages: list[tuple[K, V]]) -> None:
        """Send messages to external system."""
        ...

    def receive_messages(self) -> list[tuple[K, V]]:
        """Receive messages from external system."""
        ...


class FakeTransport(MessageTransport[K, V], Generic[K, V]):
    """Fake transport for testing that tracks all interactions."""

    def __init__(self):
        self.sent_messages: list[list[tuple[K, V]]] = []
        self.received_messages: list[list[tuple[K, V]]] = []
        self.send_call_count = 0
        self.receive_call_count = 0
        self.should_fail_send = False
        self.should_fail_receive = False
        self._lock = threading.Lock()

    def send_messages(self, messages: list[tuple[K, V]]) -> None:
        """Send messages to external system."""
        with self._lock:
            self.send_call_count += 1
            if self.should_fail_send:
                raise RuntimeError("Transport send failed")
            self.sent_messages.append(messages.copy())

    def receive_messages(self) -> list[tuple[K, V]]:
        """Receive messages from external system."""
        with self._lock:
            self.receive_call_count += 1
            if self.should_fail_receive:
                raise RuntimeError("Transport receive failed")

            if self.received_messages:
                return self.received_messages.pop(0)
            return []

    def add_incoming_messages(self, messages: dict[K, V]) -> None:
        """Add messages to be returned by receive_messages."""
        with self._lock:
            self.received_messages.append(list(messages.items()))

    def get_stats(self) -> tuple[int, int]:
        """Get call counts thread-safely."""
        with self._lock:
            return self.send_call_count, self.receive_call_count

    def reset(self) -> None:
        """Reset all tracking state."""
        with self._lock:
            self.sent_messages.clear()
            self.received_messages.clear()
            self.send_call_count = 0
            self.receive_call_count = 0
            self.should_fail_send = False
            self.should_fail_receive = False
