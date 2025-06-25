# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import time
from typing import Generic, Protocol, TypeVar

from .deduplicating_message_store import DeduplicatingMessageStore

K = TypeVar('K')
V = TypeVar('V')


class MessageTransport(Protocol, Generic[K, V]):
    """Protocol for transporting messages to/from external systems."""

    def send_messages(self, messages: dict[K, V]) -> None:
        """Send messages to external system."""
        ...

    def receive_messages(self) -> dict[K, V]:
        """Receive messages from external system."""
        ...


class MessageProcessor(Generic[K, V]):
    """
    Handles message processing logic with deduplication and timing control.

    Separated from transport layer to enable testing without external dependencies.
    """

    def __init__(
        self,
        transport: MessageTransport[K, V],
        incoming_poll_interval: float = 1.0,
    ):
        self._transport = transport
        self._incoming_poll_interval = incoming_poll_interval

        # Message queues
        self._outgoing_queue = DeduplicatingMessageStore[K, V]()
        self._incoming_queue = DeduplicatingMessageStore[K, V]()

        # Timing control for incoming messages
        self._last_incoming_check = 0.0

    def publish(self, key: K, value: V) -> None:
        """Queue a message for publishing."""
        self._outgoing_queue.put(key, value)

    def pop_all(self) -> dict[K, V]:
        """Pop all decoded messages from the incoming queue."""
        return self._incoming_queue.pop_all()

    def process_cycle(self) -> bool:
        """
        Process one cycle of outgoing and incoming messages.

        Returns
        -------
        bool
            True if any messages were processed, False otherwise.
        """
        has_outgoing = self._process_outgoing_messages()
        has_incoming = self._process_incoming_messages_timed()
        return has_outgoing or has_incoming

    def _process_outgoing_messages(self) -> bool:
        """Process messages from outgoing queue and send via transport."""
        outgoing = self._outgoing_queue.pop_all()
        if outgoing:
            self._transport.send_messages(outgoing)
            return True
        return False

    def _process_incoming_messages_timed(self) -> bool:
        """Process incoming messages with time-based control."""
        current_time = time.time()

        if current_time - self._last_incoming_check < self._incoming_poll_interval:
            return False

        self._last_incoming_check = current_time
        return self._process_incoming_messages()

    def _process_incoming_messages(self) -> bool:
        """Process incoming messages from transport."""
        incoming = self._transport.receive_messages()
        if incoming:
            for key, value in incoming.items():
                self._incoming_queue.put(key, value)
            return True
        return False
