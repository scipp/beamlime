# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import logging
from collections import UserDict, defaultdict
from collections.abc import Callable
from typing import Any, Generic, Protocol, TypeVar

import pydantic

from beamlime.dashboard.message_bridge import MessageBridge

K = TypeVar('K')
V = TypeVar('V')
Serialized = TypeVar('Serialized')


class ConfigSchemaValidator(Protocol, Generic[K, Serialized, V]):
    """Protocol for validating configuration data against schemas."""

    def validate(self, key: K, value: V) -> bool:
        """Check if a schema is registered for the given key."""

    def deserialize(self, key: K, data: Serialized) -> V | None:
        """Validate configuration data."""

    def serialize(self, data: V) -> Serialized:
        """Serialize a pydantic model to a dictionary."""


JSONSerialized = dict[str, Any]


class ConfigSchemaManager(
    UserDict[K, type[pydantic.BaseModel]],
    ConfigSchemaValidator[K, JSONSerialized, pydantic.BaseModel],
):
    """
    Manages configuration schemas.

    Schemas are used to deserialize and validate configuration data, as well as to
    serialize pydantic models to JSON-compatible dictionaries.
    """

    def validate(self, key: K, value: V) -> bool:
        model = self.get(key)
        if model is None:
            return False
        return isinstance(value, model)

    def deserialize(self, key: K, data: JSONSerialized) -> pydantic.BaseModel | None:
        """Validate configuration data."""
        model = self.get(key)
        if model is None:
            return None
        return model.model_validate(data)

    def serialize(self, data: pydantic.BaseModel) -> JSONSerialized:
        """Serialize a pydantic model to a dictionary."""
        return data.model_dump(mode='json')


class ConfigService(Generic[K, Serialized, V]):
    """
    Service for configuration data with schema validation and message publishing.

    This service initializes from Beamlime's Kafka config topic into a local dictionary
    of the latest state. The connection to Kafka is implemented via an implementation of
    :py:class:`MessageBridge`.

    Local producers of new config values, in particular user changes to widget values,
    are set in this service and published to the message bridge, which in turn
    communicates with the Kafka topic.
    """

    def __init__(
        self,
        schema_validator: ConfigSchemaValidator[K, Serialized, V],
        message_bridge: MessageBridge[K, Serialized] | None = None,
    ):
        self.schema_validator = schema_validator
        self._message_bridge = message_bridge
        self._subscribers: dict[K, list[Callable[[V], None]]] = defaultdict(list)
        self._logger = logging.getLogger(__name__)
        self._config: dict[K, V] = {}

    def register_schema(self, key: K, schema: type[pydantic.BaseModel]) -> None:
        """Register a schema for a configuration key."""
        if isinstance(self.schema_validator, ConfigSchemaManager):
            self.schema_validator[key] = schema
        else:
            raise TypeError(
                'Schema validator must be an instance of ConfigSchemaManager for late '
                'schema registration.'
            )

    def subscribe(self, key: K, callback: Callable[[V], None]) -> None:
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
        if (data := self._config.get(key)) is not None:
            self._invoke(key, callback, data)

    def get(self, key: K, default: Any = None) -> V | Any:
        """
        Get the current configuration value for a specific key.

        Parameters
        ----------
        key:
            The configuration key to retrieve.
        default:
            The default value to return if the key is not set.

        Returns
        -------
        The current configuration value or the default if not set.
        """
        return self._config.get(key, default)

    def update_config(self, key: K, value: V) -> None:
        """
        Update the configuration for a specific key.

        This does not trigger callbacks directly. After publishing the update it makes
        it back into the service via :py:meth:`process_incoming_messages`, which will
        then notify all subscribers. This ensures a consistent ordering of updates,
        enforced by Kafka, and avoids potential infinite loops.
        """
        if not isinstance(value, pydantic.BaseModel):
            raise TypeError(
                f'Value for key {key} must be a pydantic model, got {type(value)}'
            )
        if not self.schema_validator.validate(key, value):
            raise ValueError(
                f'No schema registered for key {key} or value does not match schema'
            )
        self._config[key] = value
        if self._message_bridge:
            # Communication with message bridge is using raw JSON.
            self._message_bridge.publish(key, self.schema_validator.serialize(value))

    def process_incoming_messages(self) -> None:
        """Process any available incoming messages from the message bridge."""
        if not self._message_bridge:
            return

        # The message bridge should deal with key-based deduplication. To be sure, we
        # convert the messages to a dict, which will overwrite any duplicates.
        messages = dict(self._message_bridge.pop_all())
        for key, value in messages.items():
            self._handle_config_update(key, value)

    def _handle_config_update(self, key: K, value: Serialized) -> None:
        """Handle a configuration update from the message bridge."""
        try:
            self._logger.debug('Received config update for key %s: %s', key, value)
            validated = self.schema_validator.deserialize(key, value)
            self._logger.debug('Validated config for key %s: %s', key, validated)
            if validated is None:
                return
            self._config[key] = validated
            self._notify_subscribers(key, validated)
        except pydantic.ValidationError as e:
            self._logger.error('Invalid config data received for key %s: %s', key, e)

    def _notify_subscribers(self, key: K, data: V) -> None:
        """Notify all subscribers for a given config key."""
        self._logger.debug(
            'Notifying %d subscribers for key %s', len(self._subscribers[key]), key
        )
        for callback in self._subscribers.get(key, []):
            self._invoke(key, callback, data)

    def _invoke(self, key: K, callback: Callable[[V], None], data: V) -> None:
        try:
            callback(data)
        except Exception as e:
            self._logger.error(
                'Error in config subscriber callback for key %s: %s', key, e
            )
