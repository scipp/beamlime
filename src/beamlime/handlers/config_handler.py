# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..config.models import ConfigKey
from ..core.handler import Config, Handler
from ..core.message import CONFIG_STREAM_ID, Message
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

    def __init__(self, *, logger: logging.Logger | None = None, service_name: str):
        super().__init__(logger=logger, config={})
        self._service_name = service_name
        self._global_store: dict[str, Any] = {}
        self._stores: dict[str, dict[str, Any]] = {}
        self._actions: dict[str, list[Callable[[str, Any], None]]] = {}

    @property
    def service_name(self) -> str:
        """Name of the service this handler is associated with."""
        return self._service_name

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

    def register_action(
        self,
        *,
        key: str,
        action: Callable[[str | None, Any], list[tuple[ConfigKey, Any]]],
    ) -> None:
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
            Note: The source_name may be None for global updates.
            The function should return a list of (ConfigKey, value) tuples.
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
        # Stores the most recent update for this key/source combination
        updated: defaultdict[str, dict[str | None, ConfigUpdate]] = defaultdict(dict)
        for message in messages:
            try:
                update = ConfigUpdate.from_raw(message.value)
                if update.service_name not in (None, self._service_name):
                    # Ignore messages not for this service
                    continue
                source_name = update.source_name
                config_key = update.key
                value = update.value
                self._logger.info(
                    'Updating config for source_name = %s: %s = %s at %s',
                    source_name,
                    config_key,
                    value,
                    message.timestamp,
                )
                # Note: The source_name may be None for global updates. ConfigHandler is
                # not aware of all the possible values for source_name (using the keys
                # of self._stores could be incomplete), so the target if the action is
                # responsible for translating source_name=None to the correct list of
                # source names.
                if source_name is None:
                    # source_name=None overrides all previous source-specific updates
                    # for this key.
                    updated[config_key].clear()
                    updated[config_key][source_name] = update
                    self._global_store[config_key] = value
                    for store in self._stores.values():
                        store[config_key] = value
                else:
                    updated[config_key][source_name] = update
                    self.get_config(source_name)[config_key] = value
            except Exception:
                self._logger.exception('Error processing config message:')

        # Delay action calls until all messages are processed to reduce triggering
        # multiple calls for the same key in case of multiple messages with same key.
        results: list[tuple[ConfigKey, Any]] = []
        for config_key, source_updates in updated.items():
            for action in self._actions.get(config_key, []):
                for source_name, update in source_updates.items():
                    try:
                        # Note: Not update.source_name, as it is None for global updates
                        result = action(source_name=source_name, value=update.value)
                        self._logger.info(
                            'Action %s called for source name %s', action, source_name
                        )
                        results.extend(result)
                    except Exception:
                        self._logger.exception(
                            'Error processing config action for %s:', config_key
                        )
        messages: list[Message[Any]] = []
        for config_key, value in results:
            # Fill in the service name if not provided
            final_config_key = ConfigKey(
                source_name=config_key.source_name,
                service_name=config_key.service_name or self._service_name,
                key=config_key.key,
            )
            messages.append(
                Message(
                    stream=CONFIG_STREAM_ID,
                    value=ConfigUpdate(config_key=final_config_key, value=value),
                )
            )
        return messages


class ConfigProcessor:
    """
    Simple config processor that handles workflow_config and start_time messages
    by delegating directly to JobManagerAdapter.
    """

    def __init__(
        self,
        *,
        job_manager_adapter: Any,  # JobManagerAdapter
        logger: logging.Logger | None = None,
    ) -> None:
        self._job_manager_adapter = job_manager_adapter
        self._logger = logger or logging.getLogger(__name__)

    def process_messages(
        self, messages: list[Message[RawConfigItem]]
    ) -> list[Message[Any]]:
        """
        Process config messages and handle workflow_config and start_time updates.

        Parameters
        ----------
        messages:
            List of messages containing configuration updates

        Returns
        -------
        :
            List of response messages
        """
        # Group latest updates by key and source
        latest_updates: defaultdict[str, dict[str | None, ConfigUpdate]] = defaultdict(
            dict
        )

        for message in messages:
            try:
                update = ConfigUpdate.from_raw(message.value)
                source_name = update.source_name
                config_key = update.key
                value = update.value

                self._logger.info(
                    'Processing config message for source_name = %s: %s = %s',
                    source_name,
                    config_key,
                    value,
                )

                if source_name is None:
                    # source_name=None overrides all previous source-specific updates
                    latest_updates[config_key].clear()

                latest_updates[config_key][source_name] = update

            except Exception:
                self._logger.exception('Error processing config message')

        # Process the latest updates
        response_messages: list[Message[Any]] = []

        for config_key, source_updates in latest_updates.items():
            for source_name, update in source_updates.items():
                self._logger.debug(
                    'Processing config key %s for source %s', config_key, source_name
                )
                try:
                    if config_key == 'workflow_config':
                        results = self._job_manager_adapter.set_workflow_with_config(
                            source_name, update.value
                        )
                    elif config_key == 'start_time':
                        results = self._job_manager_adapter.reset_job(
                            source_name, update.value
                        )
                    else:
                        self._logger.debug('Unknown config key: %s', config_key)
                        continue

                    # Convert results to messages
                    for result_config_key, result_value in results:
                        response_messages.append(
                            Message(
                                stream=CONFIG_STREAM_ID,
                                value=ConfigUpdate(
                                    config_key=result_config_key, value=result_value
                                ),
                            )
                        )

                except Exception:
                    self._logger.exception(
                        'Error processing config key %s for source %s',
                        config_key,
                        source_name,
                    )

        return response_messages
