# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..config.models import ConfigKey
from ..core.handler import Config, Handler
from ..core.message import Message, MessageKey
from ..kafka.helpers import beamlime_config_topic
from ..kafka.message_adapter import RawConfigItem


@dataclass
class ConfigUpdate:
    config_key: ConfigKey
    value: Any

    @property
    def source_name(self) -> str | None:
        return self.config_key.source_name

    @property
    def service_name(self) -> str | None:
        return self.config_key.service_name

    @property
    def key(self) -> str:
        return self.config_key.key

    @staticmethod
    def from_raw(item: RawConfigItem) -> ConfigUpdate:
        config_key = ConfigKey.from_string(item.key.decode('utf-8'))
        value = json.loads(item.value.decode('utf-8'))
        return ConfigUpdate(config_key=config_key, value=value)


class ConfigHandler(Handler[bytes, None]):
    """
    Handler for configuration messages.

    This handler processes configuration messages and updates the configuration
    dictionary accordingly. It is used by StreamProcessor to handle configuration
    updates received from a Kafka topic.

    Parameters
    ----------
    logger
        Logger to use
    config
        Configuration object to update
    """

    @staticmethod
    def message_key(instrument: str) -> MessageKey:
        return MessageKey(topic=beamlime_config_topic(instrument), source_name='config')

    def __init__(self, *, logger: logging.Logger | None = None, service_name: str):
        super().__init__(logger=logger, config={})
        self._service_name = service_name
        self._global_store: dict[str, Any] = {}
        self._stores: dict[str, dict[str, Any]] = {}
        self._actions: dict[str, list[Callable[[str, Any], None]]] = {}

    def get_config(self, source_name: str) -> Config:
        """
        Get the configuration store for a specific source name.

        If not configured, a new store is created and returned, based on the current
        global store. This method always returns a store for the given source name,
        i.e., there is not mechanism to check if the source name is valid.

        Parameters
        ----------
        source_name:
            Name of the source to get the configuration for
        """
        return self._stores.setdefault(source_name, dict(self._global_store))

    def register_action(self, *, key: str, action: Callable[[str, Any], None]) -> None:
        """
        Register an action to be called when a specific key is updated.

        Parameters
        ----------
        key:
            Key to watch for changes
        action:
            Function to call when the key is updated. The function will be invoked
            with the source_name and the new value as keyword arguments, e.g.,
            ``action(source_name=source_name, value=value)``.
        """
        self._actions.setdefault(key, []).append(action)

    def handle(self, messages: list[Message[RawConfigItem]]) -> list[Message[None]]:
        """
        Process configuration messages and update the configuration.

        Parameters
        ----------
        messages:
            List of messages containing configuration updates

        Returns
        -------
        :
            Empty list as this handler doesn't produce output messages
        """
        updated: dict[str, ConfigUpdate] = {}
        for message in messages:
            try:
                update = ConfigUpdate.from_raw(message.value)
                if update.service_name not in (None, self._service_name):
                    # Ignore messages not for this service
                    continue
                source_name = update.source_name
                key = update.key
                value = update.value
                self._logger.info(
                    'Updating config for source_name = %s: %s = %s at %s',
                    source_name,
                    key,
                    value,
                    message.timestamp,
                )
                updated[key] = update
                if source_name is None:
                    self._global_store[key] = value
                    for store in self._stores.values():
                        store[key] = value
                else:
                    self.get_config(source_name)[key] = value
            except Exception:
                self._logger.exception('Error processing config message:')
        # Delay action calls until all messages are processed to reduce triggering
        # multiple calls for the same key in case of multiple messages with same key.
        for key, update in updated.items():
            for action in self._actions.get(key, []):
                try:
                    action(source_name=update.source_name, value=update.value)
                except KeyError:  # noqa: PERF203
                    self._logger.exception(
                        'Error processing config action for %s:', key
                    )
        return []
