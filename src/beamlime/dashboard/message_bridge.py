# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import logging
import time
from typing import Any, Generic, Protocol, TypeVar

from ..config.models import ConfigKey
from .message_transport import MessageTransport
from .throttling_message_handler import ThrottlingMessageHandler

K = TypeVar('K')
V = TypeVar('V')
Serialized = TypeVar('Serialized')


class MessageBridge(Protocol, Generic[K, Serialized]):
    """Protocol for publishing and consuming configuration messages."""

    def publish(self, key: K, value: Serialized) -> None:
        """Publish a configuration update message."""

    def pop_all(self) -> dict[K, Serialized]:
        """Pop all available configuration update messages."""


class BackgroundMessageBridge(MessageBridge[ConfigKey, dict[str, Any]]):
    """
    Message bridge that runs in a background thread for non-blocking GUI operations.

    Handles both producing and consuming messages with internal queues for
    communication with the GUI thread. Implements the MessageBridge protocol.
    """

    def __init__(
        self,
        transport: MessageTransport[ConfigKey, dict[str, Any]],
        logger: logging.Logger | None = None,
        outgoing_poll_interval: float = 0.1,
        incoming_poll_interval: float = 1.0,
    ):
        self._logger = logger or logging.getLogger(__name__)

        # Create processor with injected transport
        self._processor = ThrottlingMessageHandler(
            transport=transport,
            outgoing_poll_interval=outgoing_poll_interval,
            incoming_poll_interval=incoming_poll_interval,
        )

        # Thread management
        self._running = False

        self._logger.info("MessageBridge initialized")

    def start(self) -> None:
        """Start the background thread for message operations."""
        self._logger.info("Starting MessageBridge...")
        self._running = True
        self._run_loop()

    def stop(self) -> None:
        """Stop the background thread and cleanup resources."""
        self._running = False
        self._logger.info("MessageBridge stopped.")

    def publish(self, key: ConfigKey, value: dict[str, Any]) -> None:
        """Queue a message for publishing."""
        if not self._running:
            self._logger.warning("Cannot publish - bridge not running")
            return

        self._processor.publish(key, value)

    def pop_all(self) -> dict[ConfigKey, dict[str, Any]]:
        """Pop a decoded message from the incoming queue (non-blocking)."""
        return self._processor.pop_all()

    def _run_loop(self) -> None:
        """Main loop running in background thread."""
        self._logger.info("Starting MessageBridge background thread loop")

        try:
            while self._running:
                # Process one cycle of messages
                has_messages = self._processor.process_cycle()

                # If no messages were processed, sleep briefly to avoid busy waiting
                if not has_messages:
                    time.sleep(0.001)  # 1ms sleep when idle

        except Exception as e:
            self._logger.exception("Error in MessageBridge run loop: %s", e)
        finally:
            self._running = False


class FakeMessageBridge(MessageBridge[K, V], Generic[K, V]):
    """Fake message bridge for testing purposes."""

    def __init__(self):
        self._published_messages: list[tuple[K, V]] = []
        self._incoming_messages: list[tuple[K, V]] = []

    def publish(self, key: K, value: V) -> None:
        """Store published messages for inspection."""
        self._published_messages.append((key, value))

    def pop_all(self) -> dict[K, V]:
        """Pop the next message from the incoming queue."""
        messages = self._incoming_messages.copy()
        self._incoming_messages.clear()
        return dict(messages)

    def add_incoming_message(self, update: tuple[K, V]) -> None:
        """Add a message to the incoming queue for testing."""
        self._incoming_messages.append(update)

    def get_published_messages(self) -> list[tuple[K, V]]:
        """Get all published messages for inspection."""
        return self._published_messages.copy()

    def clear(self) -> None:
        """Clear all stored messages."""
        self._published_messages.clear()
        self._incoming_messages.clear()


class LoopbackMessageBridge(MessageBridge[K, V], Generic[K, V]):
    """Message bridge that loops back published messages to incoming. For testing."""

    def __init__(self):
        self.messages: list[tuple[K, V]] = []

    def publish(self, key: K, value: V) -> None:
        """Store messages and loop them back to incoming."""
        self.messages.append((key, value))

    def pop_all(self) -> dict[K, V]:
        """Pop the next message from the queue."""
        messages = self.messages.copy()
        self.messages.clear()
        return dict(messages)

    def add_incoming_message(self, update: tuple[K, V]) -> None:
        """Add a message to the incoming queue."""
        self.messages.append(update)
