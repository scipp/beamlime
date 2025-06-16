# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable
from collections import defaultdict, UserDict
from typing import Any
import json
import logging
import pydantic
from ..config.models import ConfigKey
from ..handlers.config_handler import ConfigUpdate
from ..kafka.message_adapter import RawConfigItem
from functools import wraps


ConfigSchemaRegistry = dict[ConfigKey, type[pydantic.BaseModel]]


class ConfigSchemaManager:
    """Manages configuration schemas and provides validation."""

    def __init__(self, schema_registry: ConfigSchemaRegistry):
        self._registry = schema_registry
        self._logger = logging.getLogger(__name__)

    def has_schema(self, key: ConfigKey) -> bool:
        return key in self._registry

    def validate_update(self, key: ConfigKey, **kwargs) -> dict[str, Any]:
        """Validate and serialize configuration update."""
        model = self._registry.get(key)
        if model is None:
            raise ValueError(f"No schema registered for key {key}")
        return model(**kwargs).model_dump()

    def validate_raw(
        self, key: ConfigKey, raw_data: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Validate raw configuration data."""
        if not self.has_schema(key):
            self._logger.warning('No schema registered for key %s', key)
            return

        model = self._registry[key]
        return model.model_validate(raw_data).model_dump()


class ConfigService:
    def __init__(
        self,
        schema_manager: ConfigSchemaManager,
    ):
        self._schema_manager = schema_manager
        self._subscribers: dict[ConfigKey, list[Callable[..., None]]] = defaultdict(
            list
        )
        self._logger = logging.getLogger(__name__)
        self._config: dict[ConfigKey, dict[str, Any]] = {}

    def update_config(self, key: ConfigKey, **kwargs: Any) -> None:
        # TODO publish to Kafka
        self._config[key] = self._schema_manager.validate_update(key, **kwargs)

    def subscribe(self, key: ConfigKey, callback: Callable[..., None]) -> None:
        """
        Subscribe to configuration updates for a specific key.

        Parameters
        ----------
        key:
            The configuration key to subscribe to.
        callback:
            The callback function to be called when the configuration is updated.
        """
        self._subscribers[key].append(callback)
        if key in self._config:
            # Notify immediately with current config if available
            callback(**self._config[key])

    def get_setter(self, key: ConfigKey) -> Callable[..., None]:
        """
        Returns a callable that updates the configuration for the given key.

        This callable can be used to update the configuration with keyword arguments
        corresponding to the fields of the registered schema for the key.
        """

        @wraps(self.update_config)
        def setter(**kwargs: Any) -> None:
            self.update_config(key, **kwargs)

        return setter

    def _handle_kafka_message(self, key: ConfigKey, raw_data: dict) -> None:
        """Handle incoming Kafka config message with validation."""
        try:
            if (
                validated := self._schema_manager.validate_raw(key, raw_data)
            ) is not None:
                if self._config.get(key) == validated:
                    self._logger.debug('No change in config for key %s, skipping.', key)
                    return
                self._config[key] = validated
                self._notify_subscribers(key, validated)
        except pydantic.ValidationError as e:
            self._logger.error('Invalid config data received for key %s: %s', key, e)

    def _notify_subscribers(self, key: ConfigKey, data: dict) -> None:
        """Notify all subscribers for a given config key."""
        for callback in self._subscribers.get(key, []):
            try:
                callback(**data)
            except Exception as e:  # noqa: PERF203
                self._logger.error(
                    'Error in config subscriber callback for key %s: %s', key, e
                )

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
            self._consumer.close()

    def stop(self):
        self._running = False
