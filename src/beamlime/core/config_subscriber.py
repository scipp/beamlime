# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import json
import logging
import uuid
from typing import Any

from confluent_kafka import Consumer


class ConfigSubscriber:
    def __init__(
        self,
        *,
        config: dict[str, Any],
        logger: logging.Logger | None = None,
    ):
        # Generate unique group id using service name and random suffix, to ensure all
        # instances of the service get the same messages.
        self._config = config or {}
        self._logger = logger or logging.getLogger(__name__)
        group_id = f"config-subscriber-{uuid.uuid4()}"
        self._consumer = Consumer({**config['kafka'], 'group.id': group_id})
        self._running = False

    def get(self, key: str, default=None):
        return self._config.get(key, default)

    def start(self):
        self._running = True
        self._consumer.subscribe([self._config['topic']])
        try:
            while self._running:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue
                if msg.error():
                    self._logger.error('Consumer error: %s', msg.error())
                    continue

                key = msg.key().decode('utf-8')
                value = json.loads(msg.value().decode('utf-8'))
                self._logger.info(
                    'Updating config: %s = %s at %s', key, value, msg.timestamp()
                )
                self._config[key] = value
        except KeyboardInterrupt:
            pass
        finally:
            self._running = False
            self._consumer.close()

    def stop(self):
        self._running = False