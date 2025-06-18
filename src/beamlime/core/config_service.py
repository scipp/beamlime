# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import json
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from beamlime.config.models import ConfigKey


class ConfigService:
    """
    Service for managing configuration updates via Kafka.

    The service listens for updates on the 'beamlime.control' topic and updates
    the local configuration accordingly. It also provides methods for updating
    the configuration and retrieving the current configuration.

    The topic for this service should be created as compacted:

    .. code-block:: bash
        kafka-topics.sh --create --bootstrap-server localhost:9092 \
        --topic beamlime.control --config cleanup.policy=compact \
        --config min.cleanable.dirty.ratio=0.01 \
        --config segment.ms=100
    """

    def __init__(
        self,
        *,
        kafka_config: dict[str, Any],
        consumer: Consumer,
        topic: str,
        logger: logging.Logger | None = None,
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._producer = Producer(kafka_config)
        self._consumer = consumer
        self._topic = topic
        self._config = {}
        self._running = False
        self._local_updates = set()  # Track locally initiated updates

    def delivery_callback(self, err, msg):
        if err:
            self._logger.error('Message delivery failed: %s', err)

    def update_config(self, key: ConfigKey, value):
        """
        Update configuration value for the specified key.

        Parameters
        ----------
        key:
            ConfigKey specifying the configuration target
        value:
            Value to store for the configuration key
        """
        try:
            str_key = str(key)

            # Mark this as a local update
            update_id = f"{str_key}:{hash(str(value))}"
            self._local_updates.add(update_id)

            self._producer.produce(
                self._topic,
                key=str_key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_callback,
            )
            self._producer.flush()
            self._config[str_key] = value
        except Exception as e:
            if 'update_id' in locals():
                self._local_updates.discard(update_id)
            self._logger.error('Failed to update config: %s', e)

    def get(self, key: ConfigKey, default=None):
        """
        Get configuration value for the specified key.

        Parameters
        ----------
        key:
            ConfigKey to retrieve
        default:
            Value to return if key is not found

        Returns
        -------
        :
            The configuration value or default if not found
        """
        return self._config.get(str(key), default)

    def start(self):
        self._running = True
        self._consumer.subscribe([self._topic])
        try:
            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self._logger.error('Consumer error: %s', msg.error())
                        break

                try:
                    key_str = msg.key().decode('utf-8')
                    value = json.loads(msg.value().decode('utf-8'))

                    # Only update if not from our own producer
                    update_id = f"{key_str}:{hash(str(value))}"
                    if update_id not in self._local_updates:
                        self._config[key_str] = value
                    else:
                        self._local_updates.discard(update_id)
                except Exception as e:
                    self._logger.error('Failed to process message: %s', e)

        except KeyboardInterrupt:
            pass
        finally:
            self._running = False

    def stop(self):
        self._running = False
