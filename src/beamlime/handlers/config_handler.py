# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ..core.handler import Config, Handler
from ..core.message import Message, MessageKey
from ..kafka.helpers import beamlime_command_topic


@dataclass
class ConfigUpdate:
    source_name: str | None
    service_name: str | None  # Used for filtering
    key: str
    value: Any

    @staticmethod
    def from_raw(message: dict[str, str | bytes]) -> ConfigUpdate:
        key_parts = message['key'].split('/')
        if len(key_parts) != 3:
            raise ValueError(
                "Invalid key format, expected 'source_name/service_name/key', "
                f"got {message['key']}"
            )
        source_name, service_name, key = key_parts
        if source_name == '*':
            source_name = None
        if service_name == '*':
            service_name = None
        value = json.loads(message['value'].decode('utf-8'))
        return ConfigUpdate(
            source_name=source_name, service_name=service_name, key=key, value=value
        )


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
        return MessageKey(
            topic=beamlime_command_topic(instrument), source_name='config'
        )

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        source_names: Sequence[str],
        service_name: str,
    ):
        super().__init__(logger=logger, config={})
        self._service_name = service_name
        self._stores = {name: {} for name in source_names}
        self._actions: dict[str, list[callable]] = {}

    def get_config(self, source_name: str) -> Config | None:
        """
        Get the configuration store for a specific source name.

        Parameters
        ----------
        source_name:
            Name of the source to get the configuration for
        """
        return self._stores.get(source_name)

    def register_action(self, *, key: str, callback: callable):
        """
        Register an action to be called when a specific key is updated.

        Parameters
        ----------
        key:
            Key to watch for changes
        callback:
            Callback function to call when the key is updated
        """
        self._actions.setdefault(key, []).append(callback)

    def handle(self, messages: list[Message[bytes]]) -> list[Message[None]]:
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
                    for store in self._stores.values():
                        store[key] = value
                else:
                    self._stores[source_name][key] = value
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
