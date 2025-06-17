# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import logging
from collections import defaultdict
from collections.abc import Callable
from functools import wraps
from typing import Any, Generic, Protocol, TypeVar

import pydantic

from ..handlers.config_handler import ConfigUpdate

K = TypeVar('K')
V = TypeVar('V')


class MessageBridge(Protocol, Generic[K, V]):
    """Protocol for publishing and consuming configuration messages."""

    def publish(self, key: K, value: V) -> None:
        """Publish a configuration update message."""

    def pop_message(self) -> ConfigUpdate | None:
        """Pop the next available configuration update message."""


class ConfigSchemaValidator(Protocol, Generic[K, V]):
    """Protocol for validating configuration data against schemas."""

    def has_schema(self, key: K) -> bool:
        """Check if a schema is registered for the given key."""

    def validate(self, key: K, data: V) -> V:
        """Validate configuration data."""


ConfigSchemaRegistry = dict[K, type[pydantic.BaseModel]]


class ConfigSchemaManager(ConfigSchemaValidator[K, dict[str, Any]]):
    """Manages configuration schemas and provides validation."""

    def __init__(self, schema_registry: ConfigSchemaRegistry):
        self._registry = schema_registry
        self._logger = logging.getLogger(__name__)

    def has_schema(self, key: K) -> bool:
        return key in self._registry

    def validate(self, key: K, data: dict[str, Any]) -> dict[str, Any]:
        """Validate configuration data."""
        model = self._registry.get(key)
        if model is None:
            raise ValueError(f"No schema registered for key {key}")
        return model.model_validate(data).model_dump()


class ConfigService(Generic[K, V]):
    def __init__(
        self,
        schema_validator: ConfigSchemaValidator[K, V],
        message_bridge: MessageBridge[K, V] | None = None,
    ):
        self._schema_validator = schema_validator
        self._message_bridge = message_bridge
        self._subscribers: dict[K, list[Callable[..., None]]] = defaultdict(list)
        self._logger = logging.getLogger(__name__)
        self._config: dict[K, V] = {}

    def subscribe(self, key: K, callback: Callable[..., None]) -> None:
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

    def get_setter(self, key: K) -> Callable[..., None]:
        """
        Returns a callable that updates the configuration for the given key.

        This callable can be used to update the configuration with keyword arguments
        corresponding to the fields of the registered schema for the key.
        """

        @wraps(self.update_config)
        def setter(**kwargs: Any) -> None:
            self.update_config(key, **kwargs)

        return setter

    def update_config(self, key: K, **kwargs: Any) -> None:
        validated_config = self._schema_validator.validate(key, kwargs)
        self._config[key] = validated_config
        if self._message_bridge:
            self._message_bridge.publish(key, validated_config)

    def process_incoming_messages(self) -> None:
        """Process any available incoming messages from the message bridge."""
        if not self._message_bridge:
            return

        while (update := self._message_bridge.pop_message()) is not None:
            self._handle_config_update(update)

    def _handle_config_update(self, update: ConfigUpdate) -> None:
        """Handle a configuration update from the message bridge."""
        try:
            validated = self._schema_validator.validate(update.config_key, update.value)
            if self._config.get(update.config_key) == validated:
                self._logger.debug(
                    'No change in config for key %s, skipping.', update.key
                )
                return
            self._config[update.config_key] = validated
            self._notify_subscribers(update.config_key, validated)
        except pydantic.ValidationError as e:
            self._logger.error(
                'Invalid config data received for key %s: %s', update.key, e
            )

    def _notify_subscribers(self, key: K, data: V) -> None:
        """Notify all subscribers for a given config key."""
        for callback in self._subscribers.get(key, []):
            self._invoke(key, callback, data)

    def _invoke(self, key: K, callback: Callable[..., None], data: V) -> None:
        try:
            # Handle both dict-like and other value types
            if isinstance(data, dict):
                callback(**data)
            else:
                callback(data)
        except Exception as e:
            self._logger.error(
                'Error in config subscriber callback for key %s: %s', key, e
            )


class FakeMessageBridge(MessageBridge[K, V], Generic[K, V]):
    """Fake message bridge for testing purposes."""

    def __init__(self):
        self._published_messages: list[tuple[K, V]] = []
        self._incoming_messages: list[ConfigUpdate] = []

    def publish(self, key: K, value: V) -> None:
        """Store published messages for inspection."""
        self._published_messages.append((key, value))

    def pop_message(self) -> ConfigUpdate | None:
        """Pop the next message from the incoming queue."""
        if self._incoming_messages:
            return self._incoming_messages.pop(0)
        return None

    def add_incoming_message(self, update: ConfigUpdate) -> None:
        """Add a message to the incoming queue for testing."""
        self._incoming_messages.append(update)

    def get_published_messages(self) -> list[tuple[K, V]]:
        """Get all published messages for inspection."""
        return self._published_messages.copy()

    def clear(self) -> None:
        """Clear all stored messages."""
        self._published_messages.clear()
        self._incoming_messages.clear()
