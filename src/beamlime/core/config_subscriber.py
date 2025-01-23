# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import json
import logging
import threading
import time
from typing import Any

from ..kafka.message_adapter import KafkaMessage
from ..kafka.source import KafkaConsumer


class ConfigSubscriber:
    def __init__(
        self,
        *,
        consumer: KafkaConsumer,
        config: dict[str, Any] | None = None,
        logger: logging.Logger | None = None,
    ):
        # Generate unique group id using service name and random suffix, to ensure all
        # instances of the service get the same messages.
        self._config = config or {}
        self._logger = logger or logging.getLogger(__name__)
        self._consumer = consumer
        self._running = False
        self._thread: threading.Thread | None = None

    def get(self, key: str, default=None):
        return self._config.get(key, default)

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_loop)
        self._thread.start()

    def _process_message(self, message: KafkaMessage | None) -> None:
        if message is None:
            return
        if message.error():
            self._logger.error('Consumer error: %s', message.error())
            return
        key = message.key().decode('utf-8')
        value = json.loads(message.value().decode('utf-8'))
        self._logger.info(
            'Updating config: %s = %s at %s', key, value, message.timestamp()
        )
        self._config[key] = value

    def _run_loop(self):
        try:
            while self._running:
                # We previously relied on `consume` blocking unti the timeout, but this
                # proved problematic in tests where fakes were in use, since it caused
                # idle spinning consuming CPU. We now use a shorter timeout and sleep,
                # even though this may never be a problem with a real Kafka consumer.
                time.sleep(0.01)
                messages = self._consumer.consume(num_messages=100, timeout=0.05)
                for msg in messages:
                    self._process_message(msg)
        except RuntimeError as e:
            self._logger.exception("Error ConfigSubscriber loop failed: %s", e)
            self.stop()
        finally:
            self._logger.info("ConfigSubscriber loop stopped")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join()
        self._consumer.close()
