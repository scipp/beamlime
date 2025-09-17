# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import time
from typing import Generic, TypeVar

from .deduplicating_message_store import DeduplicatingMessageStore
from .message_transport import MessageTransport

K = TypeVar('K')
V = TypeVar('V')


class ThrottlingMessageHandler(Generic[K, V]):
    """
    Handles message processing logic with deduplication and timing control.

    Separated from transport layer to enable testing without external dependencies.
    """

    def __init__(
        self,
        transport: MessageTransport[K, V],
        outgoing_poll_interval: float = 0.1,
        incoming_poll_interval: float = 1.0,
    ):
        self._transport = transport
        self._outgoing_poll_interval = outgoing_poll_interval
        self._incoming_poll_interval = incoming_poll_interval

        # Message queues
        self._outgoing_queue = DeduplicatingMessageStore[K, V]()
        self._incoming_queue = DeduplicatingMessageStore[K, V]()

        # Timing control for both directions
        self._last_outgoing_check = 0.0
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
        has_outgoing = self._process_outgoing_messages_timed()
        has_incoming = self._process_incoming_messages_timed()
        return has_outgoing or has_incoming

    def _process_outgoing_messages_timed(self) -> bool:
        """Process outgoing messages with time-based control."""
        current_time = time.time()

        if current_time - self._last_outgoing_check < self._outgoing_poll_interval:
            return False

        has_messages = self._process_outgoing_messages()
        if has_messages:
            # Update last check time only if messages were processed to avoid
            # unnecessary delays in the next cycle when there are no messages.
            self._last_outgoing_check = current_time

        return has_messages

    def _process_outgoing_messages(self) -> bool:
        """Process messages from outgoing queue and send via transport."""
        outgoing = self._outgoing_queue.pop_all()
        if outgoing:
            self._transport.send_messages(list(outgoing.items()))
            return True
        return False

    def _process_incoming_messages_timed(self) -> bool:
        """Process incoming messages with time-based control."""
        current_time = time.time()

        if current_time - self._last_incoming_check < self._incoming_poll_interval:
            return False

        # Always update last check time since processing incoming involves touching the
        # transport, i.e., polling the external system (Kafka).
        self._last_incoming_check = current_time
        return self._process_incoming_messages()

    def _process_incoming_messages(self) -> bool:
        """Process incoming messages from transport."""
        incoming = self._transport.receive_messages()
        if incoming:
            for key, value in incoming:
                self._incoming_queue.put(key, value)
            return True
        return False
