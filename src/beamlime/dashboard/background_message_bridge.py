# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import logging
import time
from typing import Any

from ..config.models import ConfigKey
from .config_service import MessageBridge
from .deduplicating_message_handler import DeduplicatingMessageHandler, MessageTransport


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
        incoming_poll_interval: float = 1.0,
    ):
        self._logger = logger or logging.getLogger(__name__)

        # Create processor with injected transport
        self._processor = DeduplicatingMessageHandler(
            transport=transport,
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
