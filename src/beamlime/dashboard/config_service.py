# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import logging
from collections import UserDict, defaultdict
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, Generic, Protocol, TypeVar

import pydantic

K = TypeVar('K')
V = TypeVar('V')
Serialized = TypeVar('Serialized')


class MessageBridge(Protocol, Generic[K, Serialized]):
    """Protocol for publishing and consuming configuration messages."""

    def publish(self, key: K, value: Serialized) -> None:
        """Publish a configuration update message."""

    def pop_message(self) -> tuple[K, Serialized] | None:
        """Pop the next available configuration update message."""


class ConfigSchemaValidator(Protocol, Generic[K, Serialized, V]):
    """Protocol for validating configuration data against schemas."""

    def validate(self, key: K, value: V) -> bool:
        """Check if a schema is registered for the given key."""

    def deserialize(self, key: K, data: Serialized) -> V | None:
        """Validate configuration data."""

    def serialize(self, data: V) -> Serialized:
        """Serialize a pydantic model to a dictionary."""


ConfigSchemaRegistry = dict[K, type[pydantic.BaseModel]]

JSONSerialized = dict[str, Any]


class ConfigSchemaManager(
    UserDict[K, type[pydantic.BaseModel]],
    ConfigSchemaValidator[K, JSONSerialized, pydantic.BaseModel],
):
    """Manages configuration schemas and provides validation."""

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
    def __init__(
        self,
        schema_validator: ConfigSchemaValidator[K, Serialized, V] | None = None,
        message_bridge: MessageBridge[K, Serialized] | None = None,
    ):
        self._schema_validator = schema_validator or ConfigSchemaManager()
        self._message_bridge = message_bridge
        self._subscribers: dict[K, list[Callable[..., None]]] = defaultdict(list)
        self._logger = logging.getLogger(__name__)
        self._config: dict[K, V] = {}
        self._update_disabled = False

    def register_schema(self, key: K, schema: type[pydantic.BaseModel]) -> None:
        """Register a schema for a configuration key."""
        if isinstance(self._schema_validator, ConfigSchemaManager):
            self._schema_validator[key] = schema
        else:
            raise TypeError(
                'Schema validator must be an instance of ConfigSchemaManager for late '
                'schema registration.'
            )

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

    def update_config(self, key: K, value: V) -> None:
        if self._update_disabled:
            return
        if not isinstance(value, pydantic.BaseModel):
            raise TypeError(
                f'Value for key {key} must be a pydantic model, got {type(value)}'
            )
        if not self._schema_validator.validate(key, value):
            raise ValueError(
                f'No schema registered for key {key} or value does not match schema'
            )
        self._config[key] = value
        if self._message_bridge:
            # Communication with message bridge is using raw JSON.
            self._message_bridge.publish(key, self._schema_validator.serialize(value))

    @contextmanager
    def _disable_updates(self):
        """Context manager to temporarily disable update_config."""
        old_state = self._update_disabled
        self._update_disabled = True
        try:
            yield
        finally:
            self._update_disabled = old_state

    def process_incoming_messages(self, num: int = 1000) -> None:
        """Process any available incoming messages from the message bridge."""
        if not self._message_bridge:
            return

        with self._disable_updates():
            # Gather updates but only handle latest message for each key to avoid
            # flooding subscribers with multiple updates or not fully compacted data.
            # We would otherwise see a replay of the history of updates, e.g., in widget
            # states. In practice this only really happens at startup.
            updates = {}
            for _ in range(num):
                if (update := self._message_bridge.pop_message()) is not None:
                    updates[update[0]] = update[1]
                else:
                    break
            for key, value in updates.items():
                self._handle_config_update(key, value)

    def _handle_config_update(self, key: K, value: Serialized) -> None:
        """Handle a configuration update from the message bridge."""
        try:
            validated = self._schema_validator.deserialize(key, value)
            if validated is None:
                return
            self._config[key] = validated
            self._notify_subscribers(key, validated)
        except pydantic.ValidationError as e:
            self._logger.error('Invalid config data received for key %s: %s', key, e)

    def _notify_subscribers(self, key: K, data: V) -> None:
        """Notify all subscribers for a given config key."""
        for callback in self._subscribers.get(key, []):
            self._invoke(key, callback, data)

    def _invoke(self, key: K, callback: Callable[..., None], data: V) -> None:
        try:
            callback(data)
        except Exception as e:
            self._logger.error(
                'Error in config subscriber callback for key %s: %s', key, e
            )


class FakeMessageBridge(MessageBridge[K, V], Generic[K, V]):
    """Fake message bridge for testing purposes."""

    def __init__(self):
        self._published_messages: list[tuple[K, V]] = []
        self._incoming_messages: list[tuple[K, V]] = []

    def publish(self, key: K, value: V) -> None:
        """Store published messages for inspection."""
        self._published_messages.append((key, value))

    def pop_message(self) -> tuple[K, V] | None:
        """Pop the next message from the incoming queue."""
        if self._incoming_messages:
            return self._incoming_messages.pop(0)
        return None

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
    """Message bridge that loops back published messages to incoming."""

    def __init__(self):
        self.messages: list[tuple[K, V]] = []

    def publish(self, key: K, value: V) -> None:
        """Store messages and loop them back to incoming."""
        self.messages.append((key, value))

    def pop_message(self) -> tuple[K, V] | None:
        """Pop the next message from the queue."""
        if self.messages:
            return self.messages.pop(0)
        return None

    def add_incoming_message(self, update: tuple[K, V]) -> None:
        """Add a message to the incoming queue."""
        self.messages.append(update)
