# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import logging
import time
from typing import Any

from confluent_kafka import Consumer

from ..config.models import ConfigKey
from .config_service import MessageBridge
from .kafka_transport import KafkaTransport
from .message_processor import MessageProcessor


class KafkaBridge(MessageBridge[ConfigKey, dict[str, Any]]):
    """
    Kafka bridge that runs in a background thread for non-blocking GUI operations.

    Handles both producing and consuming messages with internal queues for
    communication with the GUI thread. Implements the MessageBridge protocol.
    """

    def __init__(
        self,
        kafka_config: dict[str, Any],
        consumer: Consumer,
        logger: logging.Logger | None = None,
        incoming_poll_interval: float = 1.0,
        max_batch_size: int = 100,
    ):
        self._logger = logger or logging.getLogger(__name__)

        # Create transport and processor
        transport = KafkaTransport(
            kafka_config=kafka_config,
            consumer=consumer,
            logger=self._logger,
            max_batch_size=max_batch_size,
        )

        self._processor = MessageProcessor(
            transport=transport,
            incoming_poll_interval=incoming_poll_interval,
        )

        # Thread management
        self._running = False

        topic = next(iter(consumer.assignment())).topic
        self._logger.info("KafkaBridge initialized for topic: %s", topic)

    def start(self) -> None:
        """Start the background thread for Kafka operations."""
        self._logger.info("Starting KafkaBridge...")
        self._running = True
        self._run_loop()

    def stop(self) -> None:
        """Stop the background thread and cleanup resources."""
        self._running = False
        self._logger.info("KafkaBridge stopped.")

    def publish(self, key: ConfigKey, value: dict[str, Any]) -> None:
        """Queue a message for publishing to Kafka."""
        if not self._running:
            self._logger.warning("Cannot publish - adapter not running")
            return

        self._processor.publish(key, value)

    def pop_all(self) -> dict[ConfigKey, dict[str, Any]]:
        """Pop a decoded message from the incoming queue (non-blocking)."""
        return self._processor.pop_all()

    def _run_loop(self) -> None:
        """Main loop running in background thread."""
        self._logger.info("Starting KafkaBridge background thread loop")

        try:
            while self._running:
                # Process one cycle of messages
                has_messages = self._processor.process_cycle()

                # If no messages were processed, sleep briefly to avoid busy waiting
                if not has_messages:
                    time.sleep(0.001)  # 1ms sleep when idle

        except Exception as e:
            self._logger.exception("Error in KafkaAdapter run loop: %s", e)
        finally:
            self._running = False
