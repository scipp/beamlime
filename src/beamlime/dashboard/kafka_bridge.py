# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import json
import logging
import threading
import time
from queue import Empty, Queue
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from ..config.models import ConfigKey
from ..handlers.config_handler import ConfigUpdate
from ..kafka.message_adapter import RawConfigItem


class KafkaAdapter:
    """
    Kafka adapter that runs in a background thread for non-blocking GUI operations.

    Handles both producing and consuming messages with internal queues for
    communication with the GUI thread. Implements the MessageBridge protocol.
    """

    def __init__(
        self,
        topic: str,
        kafka_config: dict[str, Any],
        consumer: Consumer,
        logger: logging.Logger | None = None,
        incoming_poll_interval: float = 1.0,
    ):
        self._topic = topic
        self._logger = logger or logging.getLogger(__name__)
        self._producer = Producer(kafka_config)
        self._consumer = consumer
        self._incoming_poll_interval = incoming_poll_interval

        # Message queues
        self._outgoing_queue = Queue()
        self._incoming_queue = Queue()

        # Thread management
        self._thread = None
        self._running = False
        self._lock = threading.Lock()

        # Timing control for incoming messages
        self._last_incoming_check = 0.0

    def start(self) -> None:
        """Start the background thread for Kafka operations."""
        with self._lock:
            if self._running:
                return

            self._running = True
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
            self._logger.info("KafkaAdapter started")

    def stop(self) -> None:
        """Stop the background thread and cleanup resources."""
        with self._lock:
            if not self._running:
                return

            self._running = False

        if self._thread:
            self._thread.join(timeout=5.0)

        self._consumer.close()
        self._producer.flush()
        self._logger.info("KafkaAdapter stopped")

    def publish(self, key: ConfigKey, value: dict[str, Any]) -> None:
        """Queue a message for publishing to Kafka."""
        if not self._running:
            self._logger.warning("Cannot publish - adapter not running")
            return

        self._outgoing_queue.put((key, value))

    def pop_message(self) -> ConfigUpdate | None:
        """Pop a decoded message from the incoming queue (non-blocking)."""
        try:
            return self._incoming_queue.get_nowait()
        except Empty:
            return None

    def _run_loop(self) -> None:
        """Main loop running in background thread."""
        self._consumer.subscribe([self._topic])

        try:
            while self._running:
                # Process outgoing messages immediately when available
                has_outgoing = self._process_outgoing_messages()

                # Process incoming messages only after interval has passed
                has_incoming = self._process_incoming_messages_timed()

                # If no messages were processed, sleep briefly to avoid busy waiting
                if not (has_outgoing or has_incoming):
                    time.sleep(0.001)  # 1ms sleep when idle

        except Exception as e:
            self._logger.exception("Error in KafkaAdapter run loop: %s", e)
        finally:
            self._consumer.close()

    def _process_outgoing_messages(self) -> bool:
        """Process messages from outgoing queue and send to Kafka."""
        messages_processed = False

        try:
            while not self._outgoing_queue.empty():
                key, value = self._outgoing_queue.get_nowait()

                self._producer.produce(
                    self._topic,
                    key=str(key).encode("utf-8"),
                    value=json.dumps(value).encode("utf-8"),
                    callback=self._delivery_callback,
                )
                self._outgoing_queue.task_done()
                messages_processed = True

            # Poll producer for delivery reports (non-blocking)
            self._producer.poll(0)

        except Empty:
            pass
        except Exception as e:
            self._logger.error("Error processing outgoing messages: %s", e)

        return messages_processed

    def _process_incoming_messages_timed(self) -> bool:
        """Process incoming messages from Kafka with time-based control."""
        current_time = time.time()

        # Only check for incoming messages after the interval has passed
        if current_time - self._last_incoming_check < self._incoming_poll_interval:
            return False

        self._last_incoming_check = current_time
        return self._process_incoming_messages()

    def _process_incoming_messages(self) -> bool:
        """Process incoming messages from Kafka."""
        # Use non-blocking poll to avoid delaying outgoing messages
        msg = self._consumer.poll(0.0)
        if msg is None:
            return False

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return False
            else:
                self._logger.error("Consumer error: %s", msg.error())
                return False

        try:
            decoded_update = self._decode_update(msg)
            if decoded_update:
                self._incoming_queue.put(decoded_update)
                return True

        except Exception as e:
            self._logger.error("Failed to process incoming message: %s", e)

        return False

    def _decode_update(self, msg) -> ConfigUpdate | None:
        """Decode a Kafka message into a ConfigUpdate."""
        try:
            return ConfigUpdate.from_raw(
                RawConfigItem(key=msg.key(), value=msg.value())
            )

        except Exception as e:
            self._logger.exception("Failed to decode config message: %s", e)
            return None

    def _delivery_callback(self, err, msg) -> None:
        """Callback for message delivery confirmation."""
        if err:
            self._logger.error("Message delivery failed: %s", err)
        else:
            self._logger.debug(
                "Message delivered to %s [%s]", msg.topic(), msg.partition()
            )
